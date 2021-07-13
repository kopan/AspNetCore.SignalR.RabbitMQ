using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using MessagePack;

namespace AspNetCore.SignalR.RabbitMQ.Internal
{
    internal static class MessagePackUtil
    {
        public static int ReadArrayHeader(ref ReadOnlyMemory<byte> data)
        {
            var arr = GetArray(data);
            MessagePackReader reader = new MessagePackReader(arr);
            var val = reader.ReadArrayHeader();
            var readSize = (int)reader.Consumed;
            //var val = MessagePackBinary.ReadArrayHeader(arr.Array, arr.Offset, out var readSize);
            data = data.Slice(readSize);
            return val;
        }

        public static int ReadMapHeader(ref ReadOnlyMemory<byte> data)
        {
            var arr = GetArray(data);
            MessagePackReader reader = new MessagePackReader(arr);
            var val = reader.ReadMapHeader();
            var readSize = (int)reader.Consumed;
            //var val = MessagePackBinary.ReadMapHeader(arr.Array, arr.Offset, out var readSize);
            data = data.Slice(readSize);
            return val;
        }

        public static string ReadString(ref ReadOnlyMemory<byte> data)
        {
            var arr = GetArray(data);
            MessagePackReader reader = new MessagePackReader(arr);
            var val = reader.ReadString();
            var readSize = (int)reader.Consumed;
            //var val = MessagePackBinary.ReadString(arr.Array, arr.Offset, out var readSize);
            data = data.Slice(readSize);
            return val;
        }

        public static ReadOnlySequence<byte>? ReadBytes(ref ReadOnlyMemory<byte> data)
        {
            var arr = GetArray(data);
            MessagePackReader reader = new MessagePackReader(arr);
            var val = reader.ReadBytes();
            var readSize = (int)reader.Consumed;
            //var val = MessagePackBinary.ReadBytes(arr.Array, arr.Offset, out var readSize);
            data = data.Slice(readSize);
            return val;
        }

        public static int ReadInt32(ref ReadOnlyMemory<byte> data)
        {
            var arr = GetArray(data);
            MessagePackReader reader = new MessagePackReader(arr);
            var val = reader.ReadInt32();
            var readSize = (int)reader.Consumed;
            //var val = MessagePackBinary.ReadInt32(arr.Array, arr.Offset, out var readSize);
            data = data.Slice(readSize);
            return val;
        }

        public static byte ReadByte(ref ReadOnlyMemory<byte> data)
        {
            var arr = GetArray(data);
            MessagePackReader reader = new MessagePackReader(arr);
            var val = reader.ReadByte();
            var readSize = (int)reader.Consumed;
            //var val = MessagePackBinary.ReadByte(arr.Array, arr.Offset, out var readSize);
            data = data.Slice(readSize);
            return val;
        }

        private static ArraySegment<byte> GetArray(ReadOnlyMemory<byte> data)
        {
            var isArray = MemoryMarshal.TryGetArray(data, out var array);
            Debug.Assert(isArray);
            return array;
        }
    }
}
