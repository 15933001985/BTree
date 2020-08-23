using System;
using System.IO;

using System.Collections.Generic;

namespace BTree
{
	public class BTree<K, V>
	{
        #region 内部类

        public class BTreePair
		{
			public K Key;
			public V Value;
			public BTreeNode SubNode;
		}

		public class BTreeNode
		{
			public bool IsLeaf;			
			public byte Numrec = 0;
			/// <summary>
			/// 不写入文件
			/// 只在读取时赋值
			/// </summary>
			public long SelfAddrPtr = NULL_ADDR;
			public long ParentAddrPtr = NULL_ADDR;
			public BTreeNode LeftPtr;
			public BTreeNode RightPtr;
			public BTreePair[] Pairs = new BTreePair[PAIR_CONST_COUNT];
		}

		private enum BTreeResult
        {
			KEY_NO_FIND = -1,
			MERGE_OK = -2,
			MERGE_NO = -3
		}

		public class BTreeGlobal : IDisposable
		{
			#region 树文件

			public string TreeDir;
			public string TreeFileName;
			public string TreeFilePath;
			public FileStream TreeStreamObj = null;
			public long TreeEndAddrPtr;

			public Dictionary<long, byte[]> NodeCacheDic = new Dictionary<long, byte[]>();

			#endregion

			#region 根结点地址管理文件

			public string RootFileName;
			public FileStream RootStreamObj = null;
			public long RootEndAddrPtr;

			public List<long> RootAddrCacheList = new List<long>();

			#endregion

			#region 叶结点地址管理文件

			public string LeafFileName;
			public FileStream LeafStreamObj = null;
			public long LeafEndAddrPtr;

			public List<long> LeafAddrCacheList = new List<long>();

			#endregion			

			public BTreeGlobal(string filePath)
			{
				this.TreeFilePath = filePath;

				int index = filePath.LastIndexOf('\\');
				this.TreeDir = filePath.Substring(0, index);

				this.TreeFileName = filePath.Substring(index + 1);
				this.TreeStreamObj = new FileStream(this.TreeFilePath, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
				this.TreeEndAddrPtr = this.TreeStreamObj.Length;

				this.RootFileName = this.TreeFileName.Split('.')[0] + ".root";
				this.RootStreamObj = new FileStream(this.TreeDir + "\\" + this.RootFileName, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
				this.RootEndAddrPtr = this.RootStreamObj.Length;

				this.LeafFileName = this.TreeFileName.Split('.')[0] + ".leaf";
				this.LeafStreamObj = new FileStream(this.TreeDir + "\\" + this.LeafFileName, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
				this.LeafEndAddrPtr = this.LeafStreamObj.Length;
			}

			public void Close()
			{
				if (this.TreeStreamObj != null)
				{
					this.TreeStreamObj.Close();
					this.TreeStreamObj.Dispose();
				}

				if (this.RootStreamObj != null)
				{
					this.RootStreamObj.Close();
					this.RootStreamObj.Dispose();
				}

				if (this.LeafStreamObj != null)
				{
					this.LeafStreamObj.Close();
					this.LeafStreamObj.Dispose();
				}
			}

			public void Dispose()
			{
				this.Close();
			}

            #region 需要初始化

            #region 配置

            public int KEY_BYTES_LENGTH = 8;
			public int VALUE_BYTES_LENGTH = 8;

			public K NULL_KEY;
			public V NULL_VALUE;

			/// <summary>
			/// 是否支持从叶级删除
			/// </summary>
			public bool IS_REMOVE_FROM_LEAF = true;

			#endregion

			public delegate byte[] GetKeyBuffer_Delegate(K key);
			public GetKeyBuffer_Delegate GetKeyBufferFun;

			public delegate byte[] GetValueBuffer_Delegate(V value);
			public GetValueBuffer_Delegate GetValueBufferFun;


			public delegate K ToKey_Delegate(byte[] keyBuffer, int pos);
			public ToKey_Delegate ToKeyFun;

			public delegate V ToValue_Delegate(byte[] valueBuffer, int pos);
			public ToValue_Delegate ToValueFun;

			public delegate int KeyCompare_Delegate(K keyA, K keyB);
			public KeyCompare_Delegate KeyCompareFun;

			public delegate int ValueCompare_Delegate(V valueA, V valueB);
			public ValueCompare_Delegate ValueCompareFun;

			#endregion

			public void AddToFileTransaction(FileTransaction fileTranObj)
			{
				foreach (KeyValuePair<long, byte[]> kv in this.NodeCacheDic)
				{
					fileTranObj.Add(this.TreeFileName, kv.Key, kv.Value);
				}

				foreach (long addrPtr in this.RootAddrCacheList)
				{
					byte[] buffer = BitConverter.GetBytes(addrPtr);

					fileTranObj.Add(this.RootFileName, this.RootEndAddrPtr, buffer);

					this.RootEndAddrPtr += buffer.Length;
				}

				fileTranObj.Add(this.RootFileName, this.RootEndAddrPtr, null, EnumFileTempDataBufferType.CutOff);

				foreach (long addrPtr in this.LeafAddrCacheList)
				{
					byte[] buffer = BitConverter.GetBytes(addrPtr);

					fileTranObj.Add(this.LeafFileName, this.LeafEndAddrPtr, buffer);

					this.LeafEndAddrPtr += buffer.Length;
				}

				fileTranObj.Add(this.LeafFileName, this.LeafEndAddrPtr, null, EnumFileTempDataBufferType.CutOff);
			}
		}

        #endregion

        #region 回调

        #region 循环回调

        public delegate void Loop_Delegate(K key, V value);

        #endregion

        #region 当所属结点改变时回调

        public delegate void Node_Change_Delegate(BTreeNode nodeObj, BTreePair pairObj);
		public Node_Change_Delegate LeafNodeChangeFun = null;

		#endregion

		#endregion

		public const byte PAIR_CONST_COUNT = 7;
		public bool INTCHARMM_IS_ODD = ((PAIR_CONST_COUNT & 1) == 1);

		private const long NULL_ADDR = long.MinValue;

		private int NODE_HEAD_LENGTH_LEAF = 26;
		private int NODE_HEAD_LENGTH_ROOT = 10;

		private const int PAIR_LENGTH_LEAF = 16;
		private const int PAIR_LENGTH_ROOT = 16;

		private int NODE_LENGTH_LEAF;
		private int NODE_LENGTH_ROOT;

		private BTreeGlobal GlobalObj;
		public BTreeNode RootNode;

		public BTree(BTreeGlobal globalObj) : this(globalObj, NULL_ADDR)
		{
			
		}

		public BTree(BTreeGlobal globalObj, long rootPtr)
        {
			this.GlobalObj = globalObj;
			
			if(this.GlobalObj.IS_REMOVE_FROM_LEAF == false)
            {
				this.NODE_HEAD_LENGTH_LEAF -= 8;
				this.NODE_HEAD_LENGTH_ROOT -= 8;
			}

			this.NODE_LENGTH_LEAF = NODE_HEAD_LENGTH_LEAF + (PAIR_LENGTH_LEAF * PAIR_CONST_COUNT);
			this.NODE_LENGTH_ROOT = NODE_HEAD_LENGTH_ROOT + (PAIR_LENGTH_ROOT * PAIR_CONST_COUNT);

			if (rootPtr < 0)
			{
				this.RootNode = this.NewNodeLeaf();

				this.FileWriteNode(this.RootNode);
			}
            else
            {
				this.RootNode = this.FileInstanceNode(rootPtr);
			}
		}

        #region 文件操作

        #region 文件指针

        private long TreeGetEndAddrPtr(bool isLeaf)
        {
			long endPtr = this.GlobalObj.TreeEndAddrPtr;

			if (isLeaf == true)
			{
				this.GlobalObj.TreeEndAddrPtr += NODE_LENGTH_LEAF;
			}
            else
            {
				this.GlobalObj.TreeEndAddrPtr += NODE_LENGTH_ROOT;
			}

			return endPtr;
        }

		private void FileReleaseNode(BTreeNode nodeObj)
		{
			this.ReleaseNodeCount += 1;			

			if (this.GlobalObj.NodeCacheDic.ContainsKey(nodeObj.SelfAddrPtr) == true)
			{
				this.GlobalObj.NodeCacheDic.Remove(nodeObj.SelfAddrPtr);
			}

            if (nodeObj.IsLeaf == true)
            {
				this.LeafReleaseEndAddrPtr(nodeObj.SelfAddrPtr);
            }
            else
            {
				this.RootReleaseEndAddrPtr(nodeObj.SelfAddrPtr);
			}

			//如果只剩一个根结点了，这句代码就起作用了
			nodeObj.SelfAddrPtr = NULL_ADDR;
		}

		private long RootGetEndAddrPtr()
		{
			if (this.GlobalObj.RootAddrCacheList.Count > 0)
			{
				long rootAddr = this.GlobalObj.RootAddrCacheList[this.GlobalObj.RootAddrCacheList.Count - 1];

				this.GlobalObj.RootAddrCacheList.RemoveAt(this.GlobalObj.RootAddrCacheList.Count - 1);

				return rootAddr;
			}

			if (this.GlobalObj.RootEndAddrPtr >= 8)
			{
				byte[] rootAddrBuffer = new byte[8];

				this.GlobalObj.RootStreamObj.Seek(this.GlobalObj.RootEndAddrPtr - 8, SeekOrigin.Begin);

				this.GlobalObj.RootStreamObj.Read(rootAddrBuffer, 0, rootAddrBuffer.Length);

				long addrPtr =  BitConverter.ToInt64(rootAddrBuffer, 0);

				this.GlobalObj.RootEndAddrPtr -= 8;

				return addrPtr;
			}

			return this.TreeGetEndAddrPtr(false);
		}

		private void RootReleaseEndAddrPtr(long addrPtr)
		{
			this.GlobalObj.RootAddrCacheList.Add(addrPtr);
		}

		private long LeafGetEndAddrPtr()
		{
			if (this.GlobalObj.LeafAddrCacheList.Count > 0)
			{
				long leafAddr = this.GlobalObj.LeafAddrCacheList[this.GlobalObj.LeafAddrCacheList.Count - 1];

				this.GlobalObj.LeafAddrCacheList.RemoveAt(this.GlobalObj.LeafAddrCacheList.Count - 1);

				return leafAddr;
			}

			if (this.GlobalObj.LeafEndAddrPtr >= 8)
			{
				byte[] leafAddrBuffer = new byte[8];

				this.GlobalObj.LeafStreamObj.Seek(this.GlobalObj.LeafEndAddrPtr - 8, SeekOrigin.Begin);

				this.GlobalObj.LeafStreamObj.Read(leafAddrBuffer, 0, leafAddrBuffer.Length);

				long addrPtr = BitConverter.ToInt64(leafAddrBuffer, 0);

				this.GlobalObj.LeafEndAddrPtr -= 8;

				return addrPtr;
			}

			return this.TreeGetEndAddrPtr(true);
		}

		private void LeafReleaseEndAddrPtr(long addrPtr)
		{
			this.GlobalObj.LeafAddrCacheList.Add(addrPtr);
		}

		#endregion

		#region Read Write Base

		private byte[] FileReadFromDisk(long nodeAddrPtr, int offset, int length)
        {
			byte[] buffer = new byte[offset + length];

			this.GlobalObj.TreeStreamObj.Seek(nodeAddrPtr, SeekOrigin.Begin);

			this.GlobalObj.TreeStreamObj.Read(buffer, 0, buffer.Length);

			return buffer;
		}

		private void FileWriteBuffer(long nodeAddrPtr, int offset, byte[] buffer)
		{
			if (this.GlobalObj.NodeCacheDic.ContainsKey(nodeAddrPtr) == false)
			{
				this.GlobalObj.NodeCacheDic.Add(nodeAddrPtr, this.FileReadFromDisk(nodeAddrPtr, offset, buffer.Length));
			}

			byte[] bufferA = this.GlobalObj.NodeCacheDic[nodeAddrPtr];

			if (bufferA.Length < offset + buffer.Length)
			{
				byte[] bufferB = this.FileReadFromDisk(nodeAddrPtr, offset, buffer.Length);

				Array.Copy(bufferA, bufferB, bufferA.Length);

				this.GlobalObj.NodeCacheDic[nodeAddrPtr] = bufferB;

				bufferA = bufferB;
			}

			Array.Copy(buffer, 0, bufferA, offset, buffer.Length);
		}

		private byte[] FileReadBuffer(long nodeAddrPtr, int offset, int length)
		{
			if (this.GlobalObj.NodeCacheDic.ContainsKey(nodeAddrPtr) == false)
            {
				this.GlobalObj.NodeCacheDic.Add(nodeAddrPtr, this.FileReadFromDisk(nodeAddrPtr, offset, length));
			}

			byte[] bufferA = this.GlobalObj.NodeCacheDic[nodeAddrPtr];

			if(bufferA.Length < offset + length)
            {
				byte[] bufferB = this.FileReadFromDisk(nodeAddrPtr, offset, length);

				Array.Copy(bufferA, bufferB, bufferA.Length);

				this.GlobalObj.NodeCacheDic[nodeAddrPtr] = bufferB;

				bufferA = bufferB;
			}

			if(offset != 0)
            {
				byte[] bytes = new byte[length];

				Array.Copy(bufferA, offset, bytes, 0, length);

				return bytes;
            }

			return bufferA;
		}

		#endregion

		#region Instance

		private BTreeNode FileInstanceNode(long nodeAddrPtr)
		{
			if (nodeAddrPtr == NULL_ADDR)
			{
				return null;
			}

			byte isLeaf = this.FileReadBuffer(nodeAddrPtr, 0, 1)[0];

			byte[] buffer;

			if (isLeaf == 1)
            {
				buffer = this.FileReadBuffer(nodeAddrPtr, 0, NODE_LENGTH_LEAF);
			}
            else
            {
				buffer = this.FileReadBuffer(nodeAddrPtr, 0, NODE_LENGTH_ROOT);
			}

			BTreeNode nodeObj = new BTreeNode();

			nodeObj.IsLeaf = (buffer[0] == 1 ? true : false);
			nodeObj.Numrec = buffer[1];

			int pos = 2;

			if (this.GlobalObj.IS_REMOVE_FROM_LEAF == true)
			{
				nodeObj.ParentAddrPtr = BitConverter.ToInt64(buffer, pos);

				pos += 8;
			}

			if (nodeObj.IsLeaf == true)
			{
				nodeObj.LeftPtr = new BTreeNode();
				nodeObj.LeftPtr.SelfAddrPtr = BitConverter.ToInt64(buffer, pos);

				pos += 8;

				nodeObj.RightPtr = new BTreeNode();
				nodeObj.RightPtr.SelfAddrPtr = BitConverter.ToInt64(buffer, pos);

				pos += 8;
			}

			for (int i = 0; i < nodeObj.Numrec; i++)
			{
				nodeObj.Pairs[i] = new BTreePair();

				nodeObj.Pairs[i].Key = this.GlobalObj.ToKeyFun(buffer, pos);

				pos += this.GlobalObj.KEY_BYTES_LENGTH;

				if (nodeObj.IsLeaf == true)
				{
					nodeObj.Pairs[i].Value = this.GlobalObj.ToValueFun(buffer, pos);
				}
				else
				{
					nodeObj.Pairs[i].SubNode = new BTreeNode();
					nodeObj.Pairs[i].SubNode.SelfAddrPtr = BitConverter.ToInt64(buffer, pos);
				}

				pos += this.GlobalObj.VALUE_BYTES_LENGTH;
			}

			nodeObj.SelfAddrPtr = nodeAddrPtr;

			return nodeObj;
		}

		private byte FileInstanceNumrec(long nodeAddrPtr)
		{
			byte[] buffer = this.FileReadBuffer(nodeAddrPtr, 1, 1);

			return buffer[0];
		}

		private bool FileInstanceIsLeaf(long nodeAddrPtr)
		{
			byte[] buffer = this.FileReadBuffer(nodeAddrPtr, 0, 1);

			return buffer[0] == 1 ? true : false;
		}

		private K FileInstancePairKey(bool isLeaf, long nodeAddrPtr, byte pairIndex)
		{
			if (isLeaf == true)
			{
				long pos = nodeAddrPtr + NODE_HEAD_LENGTH_LEAF + (PAIR_LENGTH_LEAF * pairIndex);

				byte[] buffer = this.FileReadBuffer(pos, 0, this.GlobalObj.KEY_BYTES_LENGTH);

				return this.GlobalObj.ToKeyFun(buffer, 0);
			}
			else
			{
				long pos = nodeAddrPtr + NODE_HEAD_LENGTH_ROOT + (PAIR_LENGTH_ROOT * pairIndex);

				byte[] buffer = this.FileReadBuffer(pos, 0, this.GlobalObj.KEY_BYTES_LENGTH);

				return this.GlobalObj.ToKeyFun(buffer, 0);
			}
		}

		private BTreeNode FileInstancePairSubNode(long nodeAddrPtr, byte pairIndex)
		{
			long pos = nodeAddrPtr + NODE_HEAD_LENGTH_ROOT + (PAIR_LENGTH_ROOT * pairIndex) + 8;

			byte[] buffer = this.FileReadBuffer(pos, 0, 8);

			BTreeNode subNodeObj = new BTreeNode();
			subNodeObj.SelfAddrPtr = BitConverter.ToInt64(buffer, 0);

			return subNodeObj;
		}

		#endregion

		#region Serialize

		private byte[] FileSerializeNode(BTreeNode nodeObj)
		{
			byte[] buffer;

			MemoryStream ms = new MemoryStream();

			ms.WriteByte((byte)(nodeObj.IsLeaf == true ? 1 : 0));
			ms.WriteByte(nodeObj.Numrec);

			if (this.GlobalObj.IS_REMOVE_FROM_LEAF == true)
			{
				ms.Write(BitConverter.GetBytes(nodeObj.ParentAddrPtr), 0, 8);
			}

			if (nodeObj.IsLeaf == true)
			{
				if (nodeObj.LeftPtr != null)
				{
					ms.Write(BitConverter.GetBytes(nodeObj.LeftPtr.SelfAddrPtr), 0, 8);
				}
				else
				{
					ms.Write(BitConverter.GetBytes(NULL_ADDR), 0, 8);
				}

				if (nodeObj.RightPtr != null)
				{
					ms.Write(BitConverter.GetBytes(nodeObj.RightPtr.SelfAddrPtr), 0, 8);
				}
				else
				{
					ms.Write(BitConverter.GetBytes(NULL_ADDR), 0, 8);
				}
			}

			int i = 0;

			for (i = 0; i < nodeObj.Numrec; i++)
			{
				buffer = this.GlobalObj.GetKeyBufferFun(nodeObj.Pairs[i].Key);

				ms.Write(buffer, 0, buffer.Length);

				if (nodeObj.IsLeaf == true)
				{
					buffer = this.GlobalObj.GetValueBufferFun(nodeObj.Pairs[i].Value);

					ms.Write(buffer, 0, buffer.Length);
				}
				else
				{
					ms.Write(BitConverter.GetBytes(nodeObj.Pairs[i].SubNode.SelfAddrPtr), 0, 8);
				}
			}

			while (i < PAIR_CONST_COUNT)
			{
				//Key
				ms.Write(BitConverter.GetBytes(NULL_ADDR), 0, 8);

				//Value Or SubNode
				ms.Write(BitConverter.GetBytes(NULL_ADDR), 0, 8);

				i += 1;
			}

			return ms.ToArray();
		}

		private void FileWriteNode(BTreeNode nodeObj)
        {
			byte[] buffer = this.FileSerializeNode(nodeObj);

			this.FileWriteBuffer(nodeObj.SelfAddrPtr, 0, buffer);
		}

		private void FileWriteParentAddrPtr(long nodeAddrPtr, long parentAddrPtr)
		{
			if (nodeAddrPtr == NULL_ADDR)
			{
				return;
			}

			byte[] parentAddrBuffer = BitConverter.GetBytes(parentAddrPtr);

			this.FileWriteBuffer(nodeAddrPtr, 2, parentAddrBuffer);
		}

		private void FileWriteNodeLeftAddr(long nodeAddrPtr, long leftAddrPtr)
		{
			if(nodeAddrPtr == NULL_ADDR)
            {
				return;
            }

			byte[] leftBuffer = BitConverter.GetBytes(leftAddrPtr);

			this.FileWriteBuffer(nodeAddrPtr, this.GlobalObj.IS_REMOVE_FROM_LEAF ? 10 : 2, leftBuffer);
		}

		private void FileWriteNodeRightAddr(long nodeAddrPtr, long rightAddrPtr)
		{
			if (nodeAddrPtr == NULL_ADDR)
			{
				return;
			}

			byte[] rightBuffer = BitConverter.GetBytes(rightAddrPtr);

			this.FileWriteBuffer(nodeAddrPtr, this.GlobalObj.IS_REMOVE_FROM_LEAF ? 18 : 10, rightBuffer);
		}

		private void FileWritePair(BTreeNode nodeObj, byte pairIndex)
		{
			if (nodeObj.IsLeaf == true)
			{
				int offset = NODE_HEAD_LENGTH_LEAF + (PAIR_LENGTH_LEAF * pairIndex);

				byte[] bufferKey = this.GlobalObj.GetKeyBufferFun(nodeObj.Pairs[pairIndex].Key);
				byte[] bufferValue = this.GlobalObj.GetValueBufferFun(nodeObj.Pairs[pairIndex].Value);

				this.FileWriteBuffer(nodeObj.SelfAddrPtr, offset, bufferKey);
				this.FileWriteBuffer(nodeObj.SelfAddrPtr, offset + bufferKey.Length, bufferValue);
			}
            else
            {
				int offset = NODE_HEAD_LENGTH_ROOT + (PAIR_LENGTH_ROOT * pairIndex);

				byte[] bufferKey = this.GlobalObj.GetKeyBufferFun(nodeObj.Pairs[pairIndex].Key);
				byte[] bufferSubNodeAddrPtr;

				if (nodeObj.Pairs[pairIndex].SubNode != null)
				{
					bufferSubNodeAddrPtr = BitConverter.GetBytes(nodeObj.Pairs[pairIndex].SubNode.SelfAddrPtr);
				}
				else
				{
					bufferSubNodeAddrPtr = BitConverter.GetBytes(NULL_ADDR);
				}

				this.FileWriteBuffer(nodeObj.SelfAddrPtr, offset, bufferKey);
				this.FileWriteBuffer(nodeObj.SelfAddrPtr, offset + bufferKey.Length, bufferSubNodeAddrPtr);
			}
		}

		private void FileWritePairKey(BTreeNode nodeObj, byte pairIndex)
		{
			int offset;

			if (nodeObj.IsLeaf == true)
			{
				offset = NODE_HEAD_LENGTH_LEAF + (PAIR_LENGTH_LEAF * pairIndex);				
			}
            else
            {
				offset = NODE_HEAD_LENGTH_ROOT + (PAIR_LENGTH_ROOT * pairIndex);
			}

			byte[] bufferKey = this.GlobalObj.GetKeyBufferFun(nodeObj.Pairs[pairIndex].Key);

			this.FileWriteBuffer(nodeObj.SelfAddrPtr, offset, bufferKey);
		}

        #endregion

        #endregion

        #region 基函数

        #region 新建结点

        public int NewNodeCount = 0;
		public int ReleaseNodeCount = 0;

        private BTreeNode NewNodeLeaf()
		{
			BTreeNode nodeObj = new BTreeNode();

			nodeObj.IsLeaf = true;
			nodeObj.LeftPtr = new BTreeNode() { SelfAddrPtr = NULL_ADDR };
			nodeObj.RightPtr = new BTreeNode() { SelfAddrPtr = NULL_ADDR };
			nodeObj.SelfAddrPtr = this.LeafGetEndAddrPtr();

			for (int i = 0; i < PAIR_CONST_COUNT; i++)
			{
				nodeObj.Pairs[i] = new BTreePair();

				nodeObj.Pairs[i].Key = this.GlobalObj.NULL_KEY;
				nodeObj.Pairs[i].Value = this.GlobalObj.NULL_VALUE;
			}

			this.NewNodeCount += 1;

			return nodeObj;
		}

		private BTreeNode NewNodeRoot()
		{
			BTreeNode nodeObj = new BTreeNode();

			nodeObj.IsLeaf = false;
			nodeObj.SelfAddrPtr = this.RootGetEndAddrPtr();

			for (int i = 0; i < PAIR_CONST_COUNT; i++)
			{
				nodeObj.Pairs[i] = new BTreePair();

				nodeObj.Pairs[i].Key = this.GlobalObj.NULL_KEY;
				nodeObj.Pairs[i].SubNode = null;
			}

			this.NewNodeCount += 1;

			return nodeObj;
		}

		#endregion

		private bool IsFull(int numrec)
		{
			if (numrec >= PAIR_CONST_COUNT)
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		private int Binaryle_Begin(BTreeNode root, K key)
		{
			int i = 0;

			while (i < root.Numrec)
			{
				if (root.Pairs[i] == null)
				{
					break;
				}
				else if (this.GlobalObj.KeyCompareFun(root.Pairs[i].Key, key) == 0)
				{
					break;
				}
				else if (this.GlobalObj.KeyCompareFun(root.Pairs[i].Key, key) > 0)
				{
					i -= 1;

					break;
				}
				else
				{
					i++;
				}
			}

			return i;
		}

		private int Binaryle_End(BTreeNode root, K key)
		{
			int i = 0;

			while (i < root.Numrec)
			{
				if (root.Pairs[i] == null || this.GlobalObj.KeyCompareFun(root.Pairs[i].Key, key) > 0)
				{
					break;
				}
				else
				{
					i++;
				}
			}

			return i;
		}

		#endregion

		#region 添加

		private void PutArray(BTreeNode nodeObj, int pos, BTreeNode newNodeObj, BTreePair pairObj)
		{
			for (int i = nodeObj.Numrec - 1; i >= pos; i--)
			{
				int j = i + 1;

				if(j >= PAIR_CONST_COUNT)
                {
					continue;
                }

				if(nodeObj.Pairs[j] == null)
                {
					nodeObj.Pairs[j] = new BTreePair();
				}

				nodeObj.Pairs[j].Key = nodeObj.Pairs[i].Key;
				nodeObj.Pairs[j].Value = nodeObj.Pairs[i].Value;
				nodeObj.Pairs[j].SubNode = nodeObj.Pairs[i].SubNode;
			}
		
			if(nodeObj.Pairs[pos] == null)
            {
				nodeObj.Pairs[pos] = new BTreePair();
			}

			nodeObj.Pairs[pos].Key = pairObj.Key;
			nodeObj.Pairs[pos].Value = pairObj.Value;
			nodeObj.Pairs[pos].SubNode = newNodeObj;

			nodeObj.Numrec++;

			if (this.LeafNodeChangeFun != null && nodeObj.IsLeaf == true)
			{
				this.LeafNodeChangeFun(nodeObj, pairObj);
			}
		}

		private BTreeNode SplitNode(BTreeNode nodeObj, byte pos, BTreeNode newSubNodeObj, BTreePair pairObj)
		{
			BTreeNode newNodeObj;

            if (nodeObj.IsLeaf == true)
            {
				newNodeObj = this.NewNodeLeaf();
			}
            else
            {
				newNodeObj = this.NewNodeRoot();
			}

			if (nodeObj.IsLeaf == true)
			{
				this.FileWriteNodeLeftAddr(nodeObj.RightPtr.SelfAddrPtr, newNodeObj.SelfAddrPtr);

				newNodeObj.RightPtr = nodeObj.RightPtr;
				nodeObj.RightPtr.LeftPtr = newNodeObj;

				nodeObj.RightPtr = newNodeObj;
				newNodeObj.LeftPtr = nodeObj;				
			}

			byte m = PAIR_CONST_COUNT / 2;

			if (this.INTCHARMM_IS_ODD == true)
			{
				m += 1;

				newNodeObj.Numrec = m;
				nodeObj.Numrec = m;
			}
			else
            {
				newNodeObj.Numrec = m;
				nodeObj.Numrec = (byte)(m + 1);
			}

			newNodeObj.IsLeaf = nodeObj.IsLeaf;



			if(pos < nodeObj.Numrec)
            {
				int j = 0;

				//分裂到新结点的部分
				for (int i = nodeObj.Numrec - 1; j < newNodeObj.Numrec; i++)
				{
					newNodeObj.Pairs[j].Key = nodeObj.Pairs[i].Key;
					newNodeObj.Pairs[j].Value = nodeObj.Pairs[i].Value;
					newNodeObj.Pairs[j].SubNode = nodeObj.Pairs[i].SubNode;

                    if (this.LeafNodeChangeFun != null && nodeObj.IsLeaf == true)
                    {
                        this.LeafNodeChangeFun(newNodeObj, newNodeObj.Pairs[j]);
                    }

					j += 1;
				}

				//大于 pos 的部分
				for (int i = nodeObj.Numrec - 1; i > pos; i--)
                {
                    j = i - 1;

                    nodeObj.Pairs[i].Key = nodeObj.Pairs[j].Key;
                    nodeObj.Pairs[i].Value = nodeObj.Pairs[j].Value;
                    nodeObj.Pairs[i].SubNode = nodeObj.Pairs[j].SubNode;
                }

				nodeObj.Pairs[pos] = pairObj;
				nodeObj.Pairs[pos].SubNode = newSubNodeObj;

				if (this.LeafNodeChangeFun != null && nodeObj.IsLeaf == true)
				{
					this.LeafNodeChangeFun(nodeObj, pairObj);
				}
			}
            else
            {
				int j = 0;

				//小于 pos 的部分
				for (int i = nodeObj.Numrec; i < pos; i++)
				{
					newNodeObj.Pairs[j].Key = nodeObj.Pairs[i].Key;
					newNodeObj.Pairs[j].Value = nodeObj.Pairs[i].Value;
					newNodeObj.Pairs[j].SubNode = nodeObj.Pairs[i].SubNode;

					if (this.LeafNodeChangeFun != null && nodeObj.IsLeaf == true)
					{
						this.LeafNodeChangeFun(newNodeObj, newNodeObj.Pairs[j]);
					}

					j += 1;
				}

				newNodeObj.Pairs[j] = pairObj;
				newNodeObj.Pairs[j].SubNode = newSubNodeObj;

				if (this.LeafNodeChangeFun != null && nodeObj.IsLeaf == true)
				{
					this.LeafNodeChangeFun(newNodeObj, newNodeObj.Pairs[j]);
				}

				j += 1;

				//大于 pos 的部分
				for (int i = pos; j < newNodeObj.Numrec; i++)
				{
					newNodeObj.Pairs[j].Key = nodeObj.Pairs[i].Key;
					newNodeObj.Pairs[j].Value = nodeObj.Pairs[i].Value;
					newNodeObj.Pairs[j].SubNode = nodeObj.Pairs[i].SubNode;

					if (this.LeafNodeChangeFun != null && nodeObj.IsLeaf == true)
					{
						this.LeafNodeChangeFun(newNodeObj, newNodeObj.Pairs[j]);
					}

					j += 1;
				}
			}

			
			//byte i = 0;

			//BTreePair[] pairArray = new BTreePair[PAIR_CONST_COUNT + 1];

			//for(i = 0; i < pos; i++)
			//{
			//	pairArray[i] = nodeObj.Pairs[i];
			//}

			//pairArray[pos] = new BTreePair();
			//pairArray[pos].Key = pairObj.Key;
			//pairArray[pos].Value = pairObj.Value;
			//pairArray[pos].SubNode = newSubNodeObj;

			//if (pos < nodeObj.Numrec && this.LeafNodeChangeFun != null && nodeObj.IsLeaf == true)
			//{
			//	this.LeafNodeChangeFun(nodeObj, pairObj);
			//}

			//if (pos < nodeObj.Pairs.Length)
   //         {
			//	for (i = pos; i < nodeObj.Pairs.Length; i++)
			//	{
			//		int j = i + 1;

			//		pairArray[j] = nodeObj.Pairs[i];
			//	}
			//}

			//nodeObj.Pairs = pairArray;

			//for (i = 0; i < newNodeObj.Numrec; i++)
			//{
			//	byte rootIndex = (byte)(nodeObj.Numrec + i);

			//	newNodeObj.Pairs[i].Key = nodeObj.Pairs[rootIndex].Key;
			//	newNodeObj.Pairs[i].Value = nodeObj.Pairs[rootIndex].Value;
			//	newNodeObj.Pairs[i].SubNode = nodeObj.Pairs[rootIndex].SubNode;

			//	if (this.LeafNodeChangeFun != null && nodeObj.IsLeaf == true)
			//	{
			//		this.LeafNodeChangeFun(newNodeObj, newNodeObj.Pairs[i]);
			//	}
			//}

			return newNodeObj;
		}

		/// <summary>
		/// 添加数据时的文件操作在这个函数
		/// </summary>
		/// <param name="root">根结点</param>
		/// <param name="pair">添加的数据</param>
		private BTreeNode InsertHelp(BTreeNode rootObj, BTreePair pairObj)
		{
			int currec;
			BTreeNode subLeftNode = null;
			BTreeNode subRightNode = null;

			#region 父级处理

			if (rootObj.IsLeaf == false)
			{
				currec = this.Binaryle_End(rootObj, pairObj.Key);

				int pos = currec;

				if(pos > 0)
                {
					pos -= 1;
                }

				if (rootObj.Pairs[pos].SubNode != null)
				{
					subLeftNode = rootObj.Pairs[pos].SubNode = this.FileInstanceNode(rootObj.Pairs[pos].SubNode.SelfAddrPtr);

					subRightNode = this.InsertHelp(subLeftNode, pairObj);
				}

				if (this.GlobalObj.KeyCompareFun(rootObj.Pairs[pos].Key, subLeftNode.Pairs[0].Key) != 0)
				{
					rootObj.Pairs[pos].Key = subLeftNode.Pairs[0].Key;

					this.FileWritePairKey(rootObj, (byte)pos);
				}

				if (subRightNode == null)
                {//没有分裂新结点

					return null;
                }

                subLeftNode.ParentAddrPtr = rootObj.SelfAddrPtr;
                subRightNode.ParentAddrPtr = rootObj.SelfAddrPtr;

                this.FileWriteNode(subLeftNode);
                this.FileWriteNode(subRightNode);

				currec = this.Binaryle_End(rootObj, subRightNode.Pairs[0].Key);				

				if (this.IsFull(rootObj.Numrec) == false)
                {//本父结点还有可用的 Pair					

					this.PutArray(rootObj, currec, subRightNode, subRightNode.Pairs[0]);

                    this.FileWriteNode(rootObj);

                    return null;
                }

				BTreeNode rootRightNode = this.SplitNode(rootObj, (byte)currec, subRightNode, subRightNode.Pairs[0]);

				if (this.GlobalObj.IS_REMOVE_FROM_LEAF == true)
				{
					for (int i = 0; i < rootRightNode.Numrec; i++)
					{
						this.FileWriteParentAddrPtr(rootRightNode.Pairs[i].SubNode.SelfAddrPtr, rootRightNode.SelfAddrPtr);

					}
				}

				return rootRightNode;
			}

			#endregion

			#region 叶级处理

			currec = this.Binaryle_End(rootObj, pairObj.Key);

			if (this.IsFull(rootObj.Numrec) == false)
			{
				this.PutArray(rootObj, currec, null, pairObj);

                this.FileWriteNode(rootObj);

				return null;
			}


			subRightNode = this.SplitNode(rootObj, (byte)currec, null, pairObj);

			return subRightNode;

            #endregion
        }

        private void InsertPair(BTreePair pair)
		{
			BTreeNode subRightNode = InsertHelp(this.RootNode, pair);

			if (subRightNode == null)
			{
				return;
			}

			BTreeNode newRoot = this.NewNodeRoot();

			this.RootNode.ParentAddrPtr = newRoot.SelfAddrPtr;
			subRightNode.ParentAddrPtr = newRoot.SelfAddrPtr;

			newRoot.IsLeaf = false;
			newRoot.Numrec = 2;

			newRoot.Pairs[0].Key = this.RootNode.Pairs[0].Key;
			newRoot.Pairs[0].Value = this.RootNode.Pairs[0].Value;
			newRoot.Pairs[0].SubNode = this.RootNode;

			newRoot.Pairs[1].Key = subRightNode.Pairs[0].Key;
			newRoot.Pairs[1].Value = subRightNode.Pairs[0].Value;
			newRoot.Pairs[1].SubNode = subRightNode;

			this.FileWriteNode(this.RootNode);
			this.FileWriteNode(subRightNode);
			this.FileWriteNode(newRoot);

			this.RootNode = newRoot;

			return;
		}

        #endregion

        #region 查找

        private BTreeNode FindHelp(BTreeNode rootObj, ref int pos, K key)
		{
			pos = this.Binaryle_Begin(rootObj, key);

			if (rootObj.IsLeaf == false && pos < rootObj.Numrec)
            {
				BTreeNode nodeObj = this.FileInstanceNode(rootObj.Pairs[pos].SubNode.SelfAddrPtr);

				return this.FindHelp(nodeObj, ref pos, key);
			}				
			
			if(pos >= rootObj.Numrec || this.GlobalObj.KeyCompareFun(rootObj.Pairs[pos].Key, key) != 0)
            {
				return null;
            }

			return rootObj;
		}
		//查找目标结点
		private BTreeNode FindNode(K key)
		{
			int pos = -1;

			BTreeNode nodeObj = this.FindHelp(this.RootNode, ref pos, key);

			if (nodeObj != null)
			{
				return nodeObj;
			}

			return null;
		}
		//查找目标键值
		public BTreePair Find(K key)
		{
			int pos = -1;

			BTreeNode nodeObj = this.FindHelp(this.RootNode, ref pos, key);

			if (nodeObj != null)
			{
				return nodeObj.Pairs[pos];
			}

			return null;
		}

        #endregion

        #region 删除

        #region 弃用

        /// <summary>
        /// 合并链表结点
        /// 为提升效率,取消了这个功能
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        private void MergeNodes(BTreeNode left, BTreeNode right)
		{
			for (int i = left.Numrec; i < left.Numrec + right.Numrec; i++)
			{
				left.Pairs[i].Key = right.Pairs[i - left.Numrec].Key;
				left.Pairs[i].Value = right.Pairs[i - left.Numrec].Value;
				left.Pairs[i].SubNode = right.Pairs[i - left.Numrec].SubNode;
			}
			left.Numrec += right.Numrec;
			//right->numrec = 0;
			left.RightPtr = right.RightPtr;
			if (right.RightPtr != null)
				right.RightPtr.LeftPtr = left;

			//释放结点空间
			//free(right);
		}

		/// <summary>
		/// 平衡数据
		/// 为了提升效率,取消了这个功能
		/// </summary>
		/// <param name="left"></param>
		/// <param name="right"></param>
		private void ShuffleNodes(BTreeNode left, BTreeNode right)
		{
			int i, j;
			if (left.Numrec > right.Numrec)
			{
				j = (left.Numrec + right.Numrec) / 2;
				for (i = j; i > 0; i--)
				{
					right.Pairs[i].Key = right.Pairs[i - 1].Key;
					right.Pairs[i].Value = right.Pairs[i - 1].Value;
					right.Pairs[i].SubNode = right.Pairs[i - 1].SubNode;
				}
				right.Pairs[0].Key = left.Pairs[left.Numrec - 1].Key;
				right.Pairs[0].Value = left.Pairs[left.Numrec - 1].Value;
				right.Pairs[0].SubNode = left.Pairs[left.Numrec - 1].SubNode;
				left.Numrec--;
				right.Numrec++;
			}
			else
			{
				left.Pairs[left.Numrec].Key = right.Pairs[0].Key;
				left.Pairs[left.Numrec].Value = right.Pairs[0].Value;
				left.Pairs[left.Numrec].SubNode = right.Pairs[0].SubNode;
				for (i = 0; i < right.Numrec - 1; i++)
				{
					right.Pairs[i].Key = right.Pairs[i + 1].Key;
					right.Pairs[i].Value = right.Pairs[i + 1].Value;
					right.Pairs[i].SubNode = right.Pairs[i + 1].SubNode;
				}
				left.Numrec++;
				right.Numrec--;
			}
		}

        #endregion

        #region 从根结点查找 并删除

        //删除数据
        private void RemoveRepeat(BTreeNode rootObj, int pos, K key)
		{
			int beginIndex = pos;

			byte reduceCount = 0;

			while (beginIndex >= 0 && this.GlobalObj.KeyCompareFun(rootObj.Pairs[beginIndex].Key, key) == 0)
			{
				beginIndex -= 1;

				reduceCount += 1;
			}

			beginIndex += 1;

			int j = beginIndex;

			for (int i = pos + 1; i < rootObj.Numrec; i++)
			{
				rootObj.Pairs[j].Key = rootObj.Pairs[i].Key;
				rootObj.Pairs[j].Value = rootObj.Pairs[i].Value;
				rootObj.Pairs[j].SubNode = rootObj.Pairs[i].SubNode;

				j += 1;
			}

			rootObj.Numrec -= reduceCount;
		}

		private void RemoveSingle(BTreeNode rootObj, int pos)
		{
			for (int i = pos + 1; i < rootObj.Numrec; i++)
			{
				byte j = (byte)(i - 1);

				rootObj.Pairs[j].Key = rootObj.Pairs[i].Key;
				rootObj.Pairs[j].Value = rootObj.Pairs[i].Value;
				rootObj.Pairs[j].SubNode = rootObj.Pairs[i].SubNode;
			}

			rootObj.Numrec -= 1;
		}

		private BTreeResult RemoveBase(BTreeNode nodeObj)
        {
			if (nodeObj.Numrec <= 0)
			{
				if (nodeObj.IsLeaf == true)
				{
					this.FileWriteNodeRightAddr(nodeObj.LeftPtr.SelfAddrPtr, nodeObj.RightPtr.SelfAddrPtr);

					this.FileWriteNodeLeftAddr(nodeObj.RightPtr.SelfAddrPtr, nodeObj.LeftPtr.SelfAddrPtr);
				}

				this.FileReleaseNode(nodeObj);

				return BTreeResult.MERGE_OK;				
			}

			this.FileWriteNode(nodeObj);

			return BTreeResult.MERGE_NO;
		}

		private BTreeResult RemoveHelp(BTreeNode rootObj, K key)
		{
			int currec = this.Binaryle_End(rootObj, key);

			if (currec > 0)
			{
				currec -= 1;
			}

			#region 父结点处理

			if (rootObj.IsLeaf == false)
            {
				BTreeNode subObj = this.FileInstanceNode(rootObj.Pairs[currec].SubNode.SelfAddrPtr);

				BTreeResult rs = this.RemoveHelp(subObj, key);

				if (rs == BTreeResult.MERGE_OK)
				{
					this.RemoveSingle(rootObj, currec);

					if (this.RootNode.SelfAddrPtr == rootObj.SelfAddrPtr && rootObj.Numrec == 1)
					{
						this.FileReleaseNode(this.RootNode);

						this.RootNode = this.FileInstanceNode(rootObj.Pairs[0].SubNode.SelfAddrPtr);

						return rs;
					}

					rs = this.RemoveBase(rootObj);
				}
				else if (this.GlobalObj.KeyCompareFun(rootObj.Pairs[currec].Key, subObj.Pairs[0].Key) != 0)
				{
					rootObj.Pairs[currec].Key = subObj.Pairs[0].Key;
					rootObj.Pairs[currec].SubNode = subObj;

					this.FileWritePair(rootObj, (byte)currec);
				}

				return rs;
			}

			#endregion

			#region 叶结点处理

			if (this.GlobalObj.KeyCompareFun(rootObj.Pairs[currec].Key,  key) != 0)
			{
				return BTreeResult.KEY_NO_FIND;
			}

			this.RemoveRepeat(rootObj, currec, key);

			return this.RemoveBase(rootObj);

            #endregion
        }

		#endregion

		#region 根据叶结点指针,从叶结点直接删除数据

		private void LeafRemoveHelp(BTreeResult rs, BTreeNode subNodeObj, K subKey0)
		{
			BTreeNode parentNodeObj = this.FileInstanceNode(subNodeObj.ParentAddrPtr);

			if (parentNodeObj == null)
            {
				return;
            }

			int currec = this.Binaryle_End(parentNodeObj, subKey0);

			if (currec > 0)
			{
				currec -= 1;
			}

			if (rs == BTreeResult.MERGE_OK)
			{
				subKey0 = parentNodeObj.Pairs[0].Key;

				this.RemoveSingle(parentNodeObj, currec);

				if (this.RootNode.SelfAddrPtr == parentNodeObj.SelfAddrPtr && parentNodeObj.Numrec == 1)
				{
					this.FileReleaseNode(this.RootNode);

					this.RootNode = this.FileInstanceNode(parentNodeObj.Pairs[0].SubNode.SelfAddrPtr);

					return;
				}

				rs = this.RemoveBase(parentNodeObj);

				this.LeafRemoveHelp(rs, parentNodeObj, subKey0);
			}
			else if (this.GlobalObj.KeyCompareFun(parentNodeObj.Pairs[currec].Key, subNodeObj.Pairs[0].Key) != 0)
			{
				parentNodeObj.Pairs[currec].Key = subNodeObj.Pairs[0].Key;
				parentNodeObj.Pairs[currec].SubNode = subNodeObj;

				this.FileWritePair(parentNodeObj, (byte)currec);
			}
		}

		public void LeafRemove(long nodeAddrPtr, V value)
        {
			if(this.GlobalObj.IS_REMOVE_FROM_LEAF == false)
            {
				return;
            }

			BTreeNode nodeObj = this.FileInstanceNode(nodeAddrPtr);

			int currec = -1;

			for (int i = 0; i < nodeObj.Numrec; i++)
            {
				if(this.GlobalObj.ValueCompareFun(nodeObj.Pairs[i].Value, value) == 0)
                {
					currec = i;

					break;
                }
            }

            if (currec == -1)
            {
				return;
            }

			K subKey0 = nodeObj.Pairs[0].Key;

			this.RemoveSingle(nodeObj, currec);

			if (this.RootNode.SelfAddrPtr == nodeObj.SelfAddrPtr && nodeObj.Numrec == 0)
			{
				this.FileReleaseNode(this.RootNode);

				return;
			}

			BTreeResult rs = this.RemoveBase(nodeObj);			

			this.LeafRemoveHelp(rs, nodeObj, subKey0);
		}

		#endregion

		#endregion

		#region 公开函数

		#region 添加

		public void Add(K key, V v)
		{
			BTreePair pair = new BTreePair();
			pair.Key = key;
			pair.Value = v;

			this.InsertPair(pair);
		}
		//public void AddMaxKey(V v)
		//{
		//	BTreePair pair = new BTreePair();
		//	K max = this.FindMaxKey();
		//	pair.Key = max + 1;
		//	//pair.value = (char*) malloc((strlen(v)+1) * sizeof(char));
		//	pair.Value = v;
		//	this.InsertPair(pair);
		//}

        #endregion

        #region 查找

        public K FindMinKey()
		{
			BTreeNode nodeObj = this.RootNode;

			while (nodeObj.IsLeaf != true)
			{
				nodeObj = nodeObj.Pairs[0].SubNode;
				nodeObj.IsLeaf = this.FileInstanceIsLeaf(nodeObj.SelfAddrPtr);
				nodeObj.Pairs[0] = new BTreePair();
				nodeObj.Pairs[0].SubNode = this.FileInstancePairSubNode(nodeObj.SelfAddrPtr, 0);
			}

			K key = this.FileInstancePairKey(nodeObj.IsLeaf, nodeObj.SelfAddrPtr, 0);

			return key;
		}

		public K FindMaxKey()
		{
			BTreeNode nodeObj = this.RootNode;

			while (nodeObj.IsLeaf != true)
			{
				nodeObj = nodeObj.Pairs[nodeObj.Numrec - 1].SubNode;
				nodeObj.Numrec = this.FileInstanceNumrec(nodeObj.SelfAddrPtr);
				nodeObj.IsLeaf = this.FileInstanceIsLeaf(nodeObj.SelfAddrPtr);
				nodeObj.Pairs[nodeObj.Numrec - 1] = new BTreePair();
				nodeObj.Pairs[nodeObj.Numrec - 1].SubNode = this.FileInstancePairSubNode(nodeObj.SelfAddrPtr, (byte)(nodeObj.Numrec - 1));
			}

			K key = this.FileInstancePairKey(nodeObj.IsLeaf, nodeObj.SelfAddrPtr, (byte)(nodeObj.Numrec - 1));

			return key;
		}

		public BTreeNode FindMinNode()
		{
			BTreeNode nodeObj = this.RootNode;

			while (nodeObj.IsLeaf != true)
			{
				nodeObj = this.FileInstanceNode(nodeObj.Pairs[0].SubNode.SelfAddrPtr);
			}

			return nodeObj;
		}
		public BTreeNode FindMaxNode()
		{
			BTreeNode nodeObj = this.RootNode;

			while (nodeObj.IsLeaf != true)
			{
				nodeObj = this.FileInstanceNode(nodeObj.Pairs[nodeObj.Numrec - 1].SubNode.SelfAddrPtr);
			}

			return nodeObj;
		}
		public void Loop_Min_To_Max(Loop_Delegate fun)
		{
			BTreeNode nodeObj = this.FindMinNode();

			int j = 0;

			while (nodeObj != null)
			{
				for (int i = j; i < nodeObj.Numrec; i++)
				{
					fun(nodeObj.Pairs[i].Key, nodeObj.Pairs[i].Value);
				}

				nodeObj = this.FileInstanceNode(nodeObj.RightPtr.SelfAddrPtr);

				j = 0;
			}
		}
		public void Loop_Max_To_Min(Loop_Delegate fun)
		{
			BTreeNode nodeObj = this.FindMaxNode();

			int j = 0;

			while (nodeObj != null)
			{
				for (int i = nodeObj.Numrec - 1; i >= j; i--)
				{
					fun(nodeObj.Pairs[i].Key, nodeObj.Pairs[i].Value);
				}

				nodeObj = this.FileInstanceNode(nodeObj.LeftPtr.SelfAddrPtr);

				j = 0;
			}
		}

		public void Loop_To_Max(K key, Loop_Delegate fun)
		{
			BTreeNode nodeObj = this.FindNode(key);

			int j = 0;

			while (nodeObj != null)
			{
				for (int i = j; i < nodeObj.Numrec; i++)
				{
					if (this.GlobalObj.KeyCompareFun(nodeObj.Pairs[i].Key, key) < 0)
					{
						continue;
					}

					fun(nodeObj.Pairs[i].Key, nodeObj.Pairs[i].Value);
				}

				nodeObj = this.FileInstanceNode(nodeObj.RightPtr.SelfAddrPtr);

				j = 0;
			}
		}
		public void Loop_To_Min(K key, Loop_Delegate fun)
		{
			BTreeNode nodeObj = this.FindNode(key);

			int j = 0;

			while (nodeObj != null)
			{
				for (int i = nodeObj.Numrec - 1; i >= j; i--)
				{
					if(this.GlobalObj.KeyCompareFun(nodeObj.Pairs[i].Key, key) > 0)
                    {
						continue;
                    }

					fun(nodeObj.Pairs[i].Key, nodeObj.Pairs[i].Value);
				}

				nodeObj = this.FileInstanceNode(nodeObj.LeftPtr.SelfAddrPtr);

				j = 0;
			}
		}

		#endregion

		#region 删除

		public void Remove(K key)
		{
			if(this.RootNode.SelfAddrPtr == NULL_ADDR || this.RootNode.Numrec == 0)
            {
				return;
            }

			BTreeResult rs = RemoveHelp(this.RootNode, key);

            while (rs != BTreeResult.KEY_NO_FIND && this.RootNode.SelfAddrPtr != NULL_ADDR)
            {
                rs = RemoveHelp(this.RootNode, key);
            }
        }

		//private void DistroyIntCharTree(BTreeNode root)
		//{
		//	int i;
		//	if (root->isLeaf == 0)
		//	{
		//		for (i = 0; i < root->numrec; i++)
		//		{
		//			if (root->pairs[i].pointer != NULL)
		//				DistroyIntCharTree(root->pairs[i].pointer, freev);
		//		}
		//	}
		//	if (freev)
		//	{
		//		if (root->isLeaf == 1)
		//			for (i = 0; i < root->numrec; i++)
		//				free(root->pairs[i].value);
		//	}
		//	free(root);
		//}

		#endregion

		#endregion
	}
}