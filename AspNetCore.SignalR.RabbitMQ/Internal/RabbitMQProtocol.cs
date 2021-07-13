using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using MessagePack;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace AspNetCore.SignalR.RabbitMQ.Internal
{
    internal class RabbitMQProtocol
    {
        private readonly IReadOnlyList<IHubProtocol> _protocols;

        public RabbitMQProtocol(IReadOnlyList<IHubProtocol> protocols)
        {
            _protocols = protocols;
        }

        public RabbitMQInvocation ReadInvocation(ReadOnlyMemory<byte> data)
        {
            // See WriteInvocation for the format
            ValidateArraySize(ref data, 2, "Invocation");

            // Read excluded Ids
            IReadOnlyList<string> excludedConnectionIds = null;
            var idCount = MessagePackUtil.ReadArrayHeader(ref data);
            if (idCount > 0)
            {
                var ids = new string[idCount];
                for (var i = 0; i < idCount; i++)
                {
                    ids[i] = MessagePackUtil.ReadString(ref data);
                }

                excludedConnectionIds = ids;
            }

            // Read payload
            var message = ReadSerializedHubMessage(ref data);
            return new RabbitMQInvocation(message, excludedConnectionIds);
        }
        public static SerializedHubMessage ReadSerializedHubMessage(ref ReadOnlyMemory<byte> data)
        {
            var count = MessagePackUtil.ReadMapHeader(ref data);
            var serializations = new SerializedMessage[count];
            for (var i = 0; i < count; i++)
            {
                var protocol = MessagePackUtil.ReadString(ref data);
                var serialized = MessagePackUtil.ReadBytes(ref data);
                serializations[i] = new SerializedMessage(protocol, serialized.Value.ToArray());
            }

            return new SerializedHubMessage(serializations);
        }
        private static void ValidateArraySize(ref ReadOnlyMemory<byte> data, int expectedLength, string messageType)
        {
            var length = MessagePackUtil.ReadArrayHeader(ref data);

            if (length < expectedLength)
            {
                throw new InvalidDataException($"Insufficient items in {messageType} array.");
            }
        }

        public byte[] WriteInvocation(string methodName, object[] args) =>
           WriteInvocation(methodName, args, excludedConnectionIds: null);
        public byte[] WriteInvocation(string methodName, object[] args, IReadOnlyList<string> excludedConnectionIds)
        {
            // Written as a MessagePack 'arr' containing at least these items:
            // * A MessagePack 'arr' of 'str's representing the excluded ids
            // * [The output of WriteSerializedHubMessage, which is an 'arr']
            // Any additional items are discarded.

            var writer = MemoryBufferWriter.Get();
            MessagePackWriter msgWriter = new MessagePackWriter(writer);
            try
            {
                msgWriter.WriteArrayHeader(2);
                if (excludedConnectionIds != null && excludedConnectionIds.Count > 0)
                {
                    msgWriter.WriteArrayHeader(excludedConnectionIds.Count);
                    foreach (var id in excludedConnectionIds)
                    {
                        msgWriter.WriteString(Encoding.UTF8.GetBytes(id));
                    }
                }
                else
                {
                    msgWriter.WriteArrayHeader(0);
                }

                msgWriter.Flush();

                WriteSerializedHubMessage(writer, new SerializedHubMessage(new InvocationMessage(methodName, args)));

                return writer.ToArray();
            }
            finally
            {
                MemoryBufferWriter.Return(writer);
            }
        }

        public byte[] WriteGroupCommand(RabbitMQGroupCommand command)
        {
            var writer = MemoryBufferWriter.Get();
            MessagePackWriter msgWriter = new MessagePackWriter(writer);
            try
            {
                msgWriter.WriteArrayHeader( 5);
                msgWriter.WriteInt32( command.Id);
                msgWriter.WriteString(Encoding.UTF8.GetBytes(command.ServerName));
                msgWriter.WriteUInt8( (byte)command.Action);
                msgWriter.WriteString(Encoding.UTF8.GetBytes(command.GroupName));
                msgWriter.WriteString(Encoding.UTF8.GetBytes(command.ConnectionId));

                msgWriter.Flush();

                return writer.ToArray();
            }
            finally
            {
                MemoryBufferWriter.Return(writer);
            }
        }
        public RabbitMQGroupCommand ReadGroupCommand(ReadOnlyMemory<byte> data)
        {
            // See WriteGroupCommand for format.
            ValidateArraySize(ref data, 5, "GroupCommand");

            var id = MessagePackUtil.ReadInt32(ref data);
            var serverName = MessagePackUtil.ReadString(ref data);
            var action = (GroupAction)MessagePackUtil.ReadByte(ref data);
            var groupName = MessagePackUtil.ReadString(ref data);
            var connectionId = MessagePackUtil.ReadString(ref data);

            return new RabbitMQGroupCommand(id, action, serverName, groupName, connectionId);
        }
        public byte[] WriteAck(int messageId)
        {
            // Written as a MessagePack 'arr' containing at least these items:
            // * An 'int': The Id of the command being acknowledged.
            // Any additional items are discarded.

            var writer = MemoryBufferWriter.Get();
            MessagePackWriter msgWriter = new MessagePackWriter(writer);
            try
            {
                msgWriter.WriteArrayHeader(1);
                msgWriter.WriteInt32(messageId);

                msgWriter.Flush();
                return writer.ToArray();
            }
            finally
            {
                MemoryBufferWriter.Return(writer);
            }
        }
        public int ReadAck(ReadOnlyMemory<byte> data)
        {
            // See WriteAck for format
            ValidateArraySize(ref data, 1, "Ack");
            return MessagePackUtil.ReadInt32(ref data);
        }

        public byte[] WriteList(IReadOnlyList<string> list)
        {
            var writer = MemoryBufferWriter.Get();
            MessagePackWriter msgWriter = new MessagePackWriter(writer);
            try
            {
                msgWriter.WriteArrayHeader(list.Count);
                foreach (var item in list)
                {
                    msgWriter.WriteString(Encoding.UTF8.GetBytes(item));
                }

                msgWriter.Flush();
                return writer.ToArray();
            }
            finally
            {
                MemoryBufferWriter.Return(writer);
            }
        }
        public IReadOnlyList<string> ReadList(ReadOnlyMemory<byte> data)
        {
            IReadOnlyList<string> list = null;
            var itemCount = MessagePackUtil.ReadArrayHeader(ref data);
            if (itemCount > 0)
            {
                var items = new string[itemCount];
                for (var i = 0; i < itemCount; i++)
                {
                    items[i] = MessagePackUtil.ReadString(ref data);
                }
                list = items;
            }
            return list;
        }
        private void WriteSerializedHubMessage(MemoryBufferWriter writer, SerializedHubMessage message)
        {
            // Written as a MessagePack 'map' where the keys are the name of the protocol (as a MessagePack 'str')
            // and the values are the serialized blob (as a MessagePack 'bin').

            MessagePackWriter msgWriter = new MessagePackWriter(writer);
            msgWriter.WriteMapHeader(_protocols.Count);

            foreach (var protocol in _protocols)
            {
                msgWriter.WriteString(Encoding.UTF8.GetBytes(protocol.Name));

                var serialized = message.GetSerializedMessage(protocol);
                var isArray = MemoryMarshal.TryGetArray(serialized, out var array);
                Debug.Assert(isArray);
                msgWriter.Write(array.Array);
            }

            msgWriter.Flush();
        }
    }
}
