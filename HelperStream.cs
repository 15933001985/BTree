using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace BTree
{
    public class HelperStream
    {
        private Stream StreamObj;

        public HelperStream()
        {
            this.StreamObj = new MemoryStream();
        }
        public HelperStream(byte[] buffer)
        {
            this.StreamObj = new MemoryStream(buffer);
        }
        public HelperStream(FileStream streamObj)
        {
            this.StreamObj = streamObj;
        }

        public static HelperStream New()
        {
            HelperStream obj = new HelperStream();

            return obj;
        }

        public static HelperStream New(byte[] buffer)
        {
            HelperStream obj = new HelperStream(buffer);

            return obj;
        }

        public static HelperStream New(FileStream streamObj)
        {
            HelperStream obj = new HelperStream(streamObj);

            return obj;
        }

        public long Position
        {
            get
            {
                return this.StreamObj.Position;
            }
            set
            {
                this.StreamObj.Position = value;
            }
        }

        public long Length
        {
            get
            {
                return this.StreamObj.Length;
            }
        }

        public byte[] ToArray()
        {
            if(this.StreamObj is MemoryStream)
            {
                return ((MemoryStream)this.StreamObj).ToArray();
            }

            return null;
        }

        public byte ReadByte()
        {
            return (byte)this.StreamObj.ReadByte();
        }

        public byte[] Read(int length)
        {
            byte[] buffer = new byte[length];

            this.StreamObj.Read(buffer, 0, buffer.Length);

            return buffer;
        }

        public int ReadInt()
        {
            byte[] addrPtrBytes = this.Read(4);

            int i = BitConverter.ToInt32(addrPtrBytes, 0);

            return i;
        }

        public uint ReadUInt()
        {
            byte[] addrPtrBytes = this.Read(4);

            uint i = BitConverter.ToUInt32(addrPtrBytes, 0);

            return i;
        }

        public long ReadLong()
        {
            byte[] addrPtrBytes = this.Read(8);

            long l = BitConverter.ToInt64(addrPtrBytes, 0);

            return l;
        }

        public DateTime ReadDateTime()
        {
            byte[] addrPtrBytes = this.Read(8);

            long ticks = BitConverter.ToInt64(addrPtrBytes, 0);

            return new DateTime(ticks);
        }

        public string ReadString(int length)
        {
            byte[] buffer = this.Read(length);

            return Encoding.UTF8.GetString(buffer);
        }

        public string Read_Len_1_String()
        {
            int length = this.StreamObj.ReadByte();

            byte[] buffer = this.Read(length);

            return Encoding.UTF8.GetString(buffer);
        }

        public string Read_Len_2_String()
        {
            byte[] bytes = this.Read(2);

            ushort length = BitConverter.ToUInt16(bytes, 0);

            byte[] buffer = this.Read(length);

            return Encoding.UTF8.GetString(buffer);
        }

        public string Read_Len_4_String()
        {
            byte[] bytes = this.Read(4);

            int length = BitConverter.ToInt32(bytes, 0);

            byte[] buffer = this.Read(length);

            return Encoding.UTF8.GetString(buffer);
        }

        public byte[] Read_Len_2_Buffer()
        {
            byte[] bytes = this.Read(2);

            ushort length = BitConverter.ToUInt16(bytes, 0);

            byte[] buffer = this.Read(length);

            return buffer;
        }

        public byte[] Read_Len_4_Buffer()
        {
            byte[] bytes = this.Read(4);

            int length = BitConverter.ToInt32(bytes, 0);

            byte[] buffer = this.Read(length);

            return buffer;
        }

        public void WriteByte(byte b)
        {
            this.StreamObj.WriteByte(b);
        }

        public void Write(byte[] buffer)
        {
            this.StreamObj.Write(buffer, 0, buffer.Length);
        }

        public void Write(byte v)
        {
            this.WriteByte(v);
        }

        public void Write(short v)
        {
            byte[] bytes = BitConverter.GetBytes(v);

            this.Write(bytes);
        }

        public void Write(int i)
        {
            byte[] bytes = BitConverter.GetBytes(i);

            this.Write(bytes);
        }

        public void Write(uint i)
        {
            byte[] bytes = BitConverter.GetBytes(i);

            this.Write(bytes);
        }

        public void Write(long l)
        {
            byte[] bytes = BitConverter.GetBytes(l);

            this.Write(bytes);
        }

        public void Write(double d)
        {
            byte[] bytes = BitConverter.GetBytes(d);

            this.Write(bytes);
        }

        public void Write(DateTime time)
        {
            byte[] bytes = BitConverter.GetBytes(time.Ticks);

            this.Write(bytes);
        }

        public void Write_Len_1_String(string s)
        {
            byte[] bytesString = Encoding.UTF8.GetBytes(s);

            this.StreamObj.WriteByte((byte)bytesString.Length);

            this.Write(bytesString);
        }

        public void Write_Len_2_String(string s)
        {
            byte[] bytesString = Encoding.UTF8.GetBytes(s);

            byte[] bytesLength = BitConverter.GetBytes((ushort)bytesString.Length);

            this.Write(bytesLength);

            this.Write(bytesString);
        }

        public void Write_Len_4_String(string s)
        {
            byte[] bytesString = Encoding.UTF8.GetBytes(s);

            byte[] bytesLength = BitConverter.GetBytes(bytesString.Length);

            this.Write(bytesLength);

            this.Write(bytesString);
        }

        public void Write_Len_2_Buffer(byte[] buffer)
        {
            byte[] bytesLength = BitConverter.GetBytes((ushort)buffer.Length);

            this.Write(bytesLength);

            this.Write(buffer);
        }

        public void Write_Len_4_Buffer(byte[] buffer)
        {
            byte[] bytesLength = BitConverter.GetBytes(buffer.Length);

            this.Write(bytesLength);

            this.Write(buffer);
        }
    }
}
