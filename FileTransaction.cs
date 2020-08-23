using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace BTree
{
    public enum EnumFileTempDataBufferType
    {
        Write = 0, //写文件
        Append = 1,//追加文件
        CutOff = 2 //截断文件
    }
    public class EntityFileTempDataBuffer
    {
        public EnumFileTempDataBufferType Type;
        public long AddrPtr;
        public byte[] Buffer;
    }
    public class EntityFileTempData
    {
        private byte[] fileNameBuffer;
        public byte[] FileNameBuffer
        {
            get
            {
                return this.fileNameBuffer;
            }
        }
        private string fileName;
        /// <summary>
        /// 数据文件名称
        /// </summary>
        public string FileName
        {
            get
            {
                return this.fileName;
            }
            set
            {
                this.fileName = value;
                this.fileNameBuffer = Encoding.UTF8.GetBytes(value);
            }
        }

        /// <summary>
        /// 数据地址
        /// 数据
        /// </summary>
        public List<EntityFileTempDataBuffer> BufferList = new List<EntityFileTempDataBuffer>();

        public int GetBufferLength()
        {
            int length = 0;

            foreach (EntityFileTempDataBuffer bufferObj in this.BufferList)
            {
                if (bufferObj.Type == EnumFileTempDataBufferType.CutOff)
                {
                    continue;
                }

                length += bufferObj.Buffer.Length;
            }

            if (length == 0)
            {
                length = 1;
            }

            return length;
        }

        public void AddBuffer(long addrPtr, byte[] buffer)
        {
            this.AddBuffer(addrPtr, buffer, EnumFileTempDataBufferType.Write);
        }

        public void AddBuffer(long addrPtr, byte[] buffer, EnumFileTempDataBufferType type)
        {
            EntityFileTempDataBuffer obj = new EntityFileTempDataBuffer();

            obj.Type = type;
            obj.AddrPtr = addrPtr;
            obj.Buffer = buffer;

            this.BufferList.Add(obj);
        }
    }
    public class FileTransaction : IDisposable
    {
        /// <summary>
        /// 数据文件和临时文件所在的目录
        /// 目前只支持同目录下的文件保护
        /// </summary>
        public string Dir;

        /// <summary>
        /// 临时文件名称
        /// </summary>
        private string TempFileName;

        /// <summary>
        /// 锁文件名称
        /// </summary>
        private string LockerFileName;

        #region

        public FileStream LogicLockerFile = null;

        public FileTransaction(string dir, string tranName, bool openLock = true)
        {
            this.Dir = dir;
            this.TempFileName = tranName + ".temp";
            this.LockerFileName = tranName + ".locker";

            if (openLock == true)
            {
                string lockerFilePath = this.Dir + "\\" + this.LockerFileName;

                this.LogicLockerFile = new FileStream(lockerFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
            }

            #region 检测是否有未完成的操作

            string tempFilePath = this.Dir + "\\" + this.TempFileName;

            using (FileStream fsTemp = new FileStream(tempFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, 1, FileOptions.WriteThrough))
            {
                this.RestoreAssert(fsTemp);
            }

            #endregion
        }

        public void Close()
        {
            if (this.LogicLockerFile != null)
            {
                this.LogicLockerFile.Close();
                this.LogicLockerFile.Dispose();
            }
        }
        public void Dispose()
        {
            this.Close();
        }

        #endregion

        public List<EntityFileTempData> DataList = new List<EntityFileTempData>();
     
        #region 添加一个操作

        private EntityFileTempData FindTempDataObj(string fileName)
        {
            foreach (EntityFileTempData dataObj in this.DataList)
            {
                if (dataObj.FileName == fileName)
                {
                    return dataObj;
                }
            }

            return null;
        }

        public void Add(string fileName, long addrPtr, byte[] buffer)
        {
            this.Add(fileName, addrPtr, buffer, EnumFileTempDataBufferType.Write);
        }

        /// <summary>
        /// 添加一个文件写操作
        /// </summary>
        /// <param name="fileName">文件名称</param>
        /// <param name="addrPtr">文件中的地址</param>
        /// <param name="buffer">要写入的数据, null 表示要截取文件到 addrPtr</param>
        public void Add(string fileName, long addrPtr, byte[] buffer, EnumFileTempDataBufferType type)
        {
            EntityFileTempData obj = this.FindTempDataObj(fileName);

            if (obj == null)
            {
                EntityFileTempDataBuffer bufferObj = new EntityFileTempDataBuffer();

                bufferObj.Type = type;
                bufferObj.AddrPtr = addrPtr;
                bufferObj.Buffer = buffer;


                obj = new EntityFileTempData();

                obj.FileName = fileName;
                obj.BufferList.Add(bufferObj);

                this.DataList.Add(obj);
            }
            else
            {
                EntityFileTempDataBuffer bufferObj = new EntityFileTempDataBuffer();

                bufferObj.Type = type;
                bufferObj.AddrPtr = addrPtr;
                bufferObj.Buffer = buffer;

                obj.BufferList.Add(bufferObj);
            }
        }

        #endregion

        #region 序列化 实例化

        public byte[] Serialize(FileStream fs)
        {
            HelperStream ms = HelperStream.New(fs);

            //开始1位是文件完整标识位
            //0表示文件未完成
            ms.WriteByte(0);

            foreach (EntityFileTempData dataObj in this.DataList)
            {
                ms.WriteByte((byte)dataObj.FileNameBuffer.Length);
                ms.Write(dataObj.FileNameBuffer);

                //写入次数
                ms.Write(dataObj.BufferList.Count);

                foreach (EntityFileTempDataBuffer bufferObj in dataObj.BufferList)
                {
                    //地址
                    ms.Write(bufferObj.AddrPtr);
                    //操作类型
                    ms.WriteByte((byte)bufferObj.Type);

                    if (bufferObj.Buffer != null)
                    {
                        //数据长度
                        ms.Write(bufferObj.Buffer.Length);
                        //数据
                        ms.Write(bufferObj.Buffer);
                    }
                    else
                    {
                        int length = 0;
                        //数据长度
                        ms.Write(length);
                    }
                }
            }

            return ms.ToArray();
        }

        public int GetSerializeLength()
        {
            //开始1位是文件完整标识位
            //0表示文件未完成
            int length = 1;

            foreach (EntityFileTempData dataObj in this.DataList)
            {
                length += 1;
                length += dataObj.FileNameBuffer.Length;

                //写入次数
                length += 1;

                foreach (EntityFileTempDataBuffer bufferObj in dataObj.BufferList)
                {
                    //地址
                    length += 8;
                    //操作类型
                    length += 1;

                    if (bufferObj.Buffer != null)
                    {
                        //数据长度
                        length += 4;
                        //数据
                        length += bufferObj.Buffer.Length;
                    }
                    else
                    {
                        //数据长度
                        length += 4;
                    }
                }
            }

            return length;
        }

        public void Instance(byte[] tempBuffer)
        {
            HelperStream ms = HelperStream.New(tempBuffer);

            //文件完整标识1个字节, 跳过去
            ms.Position = 1;

            while (ms.Position != ms.Length)
            {
                EntityFileTempData dataObj = new EntityFileTempData();

                byte fileNameLength = (byte)ms.ReadByte();

                dataObj.FileName = ms.ReadString(fileNameLength);

                //写入次数
                int count = ms.ReadInt();

                for (int i = 0; i < count; i++)
                {
                    long addrPtr = ms.ReadLong();

                    EnumFileTempDataBufferType type = (EnumFileTempDataBufferType)ms.ReadByte();

                    int bufferLength = ms.ReadInt();

                    byte[] buffer = ms.Read(bufferLength);

                    dataObj.AddBuffer(addrPtr, buffer, type);
                }

                this.DataList.Add(dataObj);
            }
        }

        #endregion

        #region 尝试恢复

        /// <summary>
        /// 检测是否需要恢复文件
        /// </summary>
        /// <param name="fs">数据文件</param>
        /// <param name="fsTemp">临时文件</param>
        /// <returns>是否对数据文件进行了恢复</returns>
        public void RestoreAssert(FileStream fsTemp)
        {
            if (fsTemp.Length == 0)
            {//绝大部分情况
                return;
            }

            byte[] bufferTemp = new byte[fsTemp.Length];
            fsTemp.Read(bufferTemp, 0, bufferTemp.Length);

            if (bufferTemp[0] != 13)
            {//临时文件还没有写完整,也就是数据文件还没有写
                fsTemp.SetLength(0);
                fsTemp.Flush();

                return;
            }

            this.Instance(bufferTemp);

            this.WriteDataFile(true);

            fsTemp.Seek(0, SeekOrigin.Begin);
            fsTemp.WriteByte(0);//0 表示文件未完整
            fsTemp.Flush();

            fsTemp.SetLength(0);
            fsTemp.Flush();
        }

        #endregion

        /// <summary>
        /// 除了Temp临时缓存文件外，其它的文件都是数据文件
        /// 索引文件在这个函数里面也认为是数据文件
        /// </summary>
        /// <param name="fileTranObj"></param>
        private void WriteDataFile(bool restor)
        {
            foreach (EntityFileTempData dataObj in this.DataList)
            {
                string dataFilePath = this.Dir + "\\" + dataObj.FileName;

                int dataLength = dataObj.GetBufferLength();

                using (FileStream fsData = new FileStream(dataFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, dataLength, FileOptions.WriteThrough))
                {
                    foreach (EntityFileTempDataBuffer bufferObj in dataObj.BufferList)
                    {
                        //kv.Value 只能是文件的追加操作
                        if (bufferObj.Type == EnumFileTempDataBufferType.Write)
                        {
                            fsData.Seek(bufferObj.AddrPtr, SeekOrigin.Begin);

                            fsData.Write(bufferObj.Buffer, 0, bufferObj.Buffer.Length);
                        }
                        else if (bufferObj.Type == EnumFileTempDataBufferType.CutOff)
                        {
                            //kv.Key 就是文件长度
                            fsData.SetLength(bufferObj.AddrPtr);
                        }
                    }

                    fsData.Flush();
                }
            }
        }

        /// <summary>
        /// 通用的以保护模式写文件
        /// 保证文件在意外情况毁坏后，可以恢复
        /// </summary>
        /// <param name="dir">文件所在目录</param>
        /// <param name="fileName">文件名称</param>
        /// <param name="addrPtrAndBufferDic">Key:文件中的地址 -1 表示增加数据，Value 是数据缓存</param>
        public void Commit()
        {
            if (this.DataList.Count == 0)
            {
                return;
            }

            string tempFilePath = this.Dir + "\\" + this.TempFileName;

            int tempFileLength = this.GetSerializeLength();

            using (FileStream fsTemp = new FileStream(tempFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, tempFileLength, FileOptions.WriteThrough))
            {
                this.Serialize(fsTemp);
                fsTemp.Flush();

                fsTemp.Seek(0, SeekOrigin.Begin);
                fsTemp.WriteByte(13);//13 表示文件已经写完整
                fsTemp.Flush();


                this.WriteDataFile(false);


                fsTemp.Seek(0, SeekOrigin.Begin);
                fsTemp.WriteByte(0);//0 表示文件这完整
                fsTemp.Flush();

                //临时文件清零,表示数据文件已经写入成功
                fsTemp.SetLength(0);
                fsTemp.Flush();
            }

            this.DataList.Clear();
        }
    }
}
