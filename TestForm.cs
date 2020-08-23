using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using System.Text;

namespace BTree
{
    public partial class TestForm : Form
    {
        private string TreeDir;
        private const string TreeFileName = "tree.bin";
        private const string TransactionName = "tree";
        private const string RootAddrName = "root.bin";

        private void InitGlobal(BTree<long, long>.BTreeGlobal globalObj, bool removeFromLeaf)
        {
            globalObj.IS_REMOVE_FROM_LEAF = removeFromLeaf;

            globalObj.KEY_BYTES_LENGTH = 8;
            globalObj.VALUE_BYTES_LENGTH = 8;

            globalObj.NULL_KEY = long.MinValue;
            globalObj.NULL_VALUE = long.MinValue;

            globalObj.GetKeyBufferFun = delegate (long key)
            {
                return BitConverter.GetBytes(key);
            };

            globalObj.GetValueBufferFun = delegate (long value)
            {
                return BitConverter.GetBytes(value);
            };

            globalObj.ToKeyFun = delegate (byte[] keyBuffer, int pos)
            {
                return BitConverter.ToInt64(keyBuffer, pos);
            };

            globalObj.ToValueFun = delegate (byte[] valueBuffer, int pos)
            {
                return BitConverter.ToInt64(valueBuffer, pos);
            };

            globalObj.KeyCompareFun = delegate (long keyA, long keyB)
            {
                if (keyA > keyB)
                {
                    return 1;
                }

                if (keyA < keyB)
                {
                    return -1;
                }

                return 0;
            };

            globalObj.ValueCompareFun = delegate (long valueA, long valueB)
            {
                if (valueA > valueB)
                {
                    return 1;
                }

                if (valueA < valueB)
                {
                    return -1;
                }

                return 0;
            };
        }

        private long LoadRootAddr()
        {
            if (File.Exists(TreeDir + "\\" + RootAddrName) == false)
            {
                return -1;
            }

            byte[] buffer = File.ReadAllBytes(TreeDir + "\\" + RootAddrName);

            return BitConverter.ToInt64(buffer, 0);
        }

        public TestForm()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            this.TreeDir = Application.StartupPath + "\\BTree";

            if(Directory.Exists(this.TreeDir) == false)
            {
                Directory.CreateDirectory(this.TreeDir);
            }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            this.button1.Enabled = false;

            using (FileTransaction fileTranObj = new FileTransaction(TreeDir, TransactionName))
            {
                using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
                {
                    this.InitGlobal(globalObj, false);

                    BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());
                    {
                        DateTime timeA = DateTime.Now;

                        for (long i = 100000; i >= 0; i--)
                        {
                            treeObj.Add(i, i);
                        }

                        globalObj.AddToFileTransaction(fileTranObj);

                        if (treeObj.RootNode.SelfAddrPtr != this.LoadRootAddr())
                        {
                            fileTranObj.Add(RootAddrName, 0, BitConverter.GetBytes(treeObj.RootNode.SelfAddrPtr));
                        }

                        fileTranObj.Commit();

                        DateTime timeB = DateTime.Now;

                        MessageBox.Show("增加结点数: " + treeObj.NewNodeCount + " 用时: " + (timeB - timeA).TotalMilliseconds);
                    }
                }
            }

            this.button1.Enabled = true;
        }

        private void button5_Click(object sender, EventArgs e)
        {
            this.button5.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                StringBuilder builder = new StringBuilder();

                DateTime timeA = DateTime.Now;

                treeObj.Loop_Min_To_Max(delegate (long k, long v)
                {
                    builder.Append(k.ToString() + "\t" + v + "\r\n");
                });

                DateTime timeB = DateTime.Now;

                MessageBox.Show("用时: " + (timeB - timeA).TotalMilliseconds);

                this.textBox1.Text = builder.ToString();
            }

            this.button5.Enabled = true;
        }

        private void button2_Click(object sender, EventArgs e)
        {
            this.button2.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                StringBuilder builder = new StringBuilder();

                treeObj.Loop_Max_To_Min(delegate (long k, long v)
                {
                    builder.Append(k.ToString() + "\t" + v + "\r\n");
                });

                this.textBox1.Text = builder.ToString();
            }


            this.button2.Enabled = true;
        }

        private void button7_Click(object sender, EventArgs e)
        {
            this.button7.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                StringBuilder builder = new StringBuilder();

                treeObj.Loop_To_Min(Convert.ToInt64(this.textBox2.Text.Trim()), delegate (long k, long v)
                {
                    builder.Append(k.ToString() + "\t" + v + "\r\n");
                });

                this.textBox1.Text = builder.ToString();
            }

            this.button7.Enabled = true;
        }

        private void button6_Click(object sender, EventArgs e)
        {
            this.button6.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                StringBuilder builder = new StringBuilder();

                treeObj.Loop_To_Max(Convert.ToInt64(this.textBox2.Text.Trim()), delegate (long k, long v)
                {
                    builder.Append(k.ToString() + "\t" + v + "\r\n");
                });

                this.textBox1.Text = builder.ToString();
            }

            this.button6.Enabled = true;
        }

        private void button3_Click(object sender, EventArgs e)
        {
            this.button3.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                StringBuilder builder = new StringBuilder();

                long minKey = treeObj.FindMinKey();

                this.textBox1.Text = minKey.ToString();
            }

            this.button3.Enabled = true;
        }

        private void button4_Click(object sender, EventArgs e)
        {
            this.button4.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                StringBuilder builder = new StringBuilder();

                long maxKey = treeObj.FindMaxKey();

                this.textBox1.Text = maxKey.ToString();
            }

            this.button4.Enabled = true;
        }

        private void button8_Click(object sender, EventArgs e)
        {
            this.button8.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                treeObj.Remove(Convert.ToInt64(this.textBox3.Text.Trim()));

                using (FileTransaction fileTranObj = new FileTransaction(TreeDir, TransactionName))
                {
                    globalObj.AddToFileTransaction(fileTranObj);

                    if (treeObj.RootNode.SelfAddrPtr != this.LoadRootAddr())
                    {
                        fileTranObj.Add(RootAddrName, 0, BitConverter.GetBytes(treeObj.RootNode.SelfAddrPtr));
                    }

                    fileTranObj.Commit();
                }
            }

            this.button8.Enabled = true;
        }

        private void button13_Click(object sender, EventArgs e)
        {
            this.button13.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj,false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                List<long> list = new List<long>();

                DateTime timeA = DateTime.Now;

                treeObj.Loop_Min_To_Max(delegate (long k, long v)
                {
                    list.Add(k);
                });

                foreach (long k in list)
                {
                    treeObj.Remove(k);
                }

                using (FileTransaction fileTranObj = new FileTransaction(TreeDir, TransactionName))
                {
                    globalObj.AddToFileTransaction(fileTranObj);

                    if (treeObj.RootNode.SelfAddrPtr != this.LoadRootAddr())
                    {
                        fileTranObj.Add(RootAddrName, 0, BitConverter.GetBytes(treeObj.RootNode.SelfAddrPtr));
                    }

                    fileTranObj.Commit();
                }

                DateTime timeB = DateTime.Now;

                MessageBox.Show("删除结点数: " + treeObj.ReleaseNodeCount + " 用时: " + (timeB - timeA).TotalMilliseconds);
            }

            this.button13.Enabled = true;
        }

        private void button11_Click(object sender, EventArgs e)
        {
            this.button11.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                Random randObj = new Random((int)DateTime.Now.Ticks);

                DateTime timeA = DateTime.Now;

                int maxNum = 100000;

                for (int i = 0; i < maxNum; i++)
                {
                    int a = randObj.Next(0, (int)maxNum);

                    treeObj.Add(a, a);
                }

                using (FileTransaction fileTranObj = new FileTransaction(TreeDir, TransactionName))
                {
                    globalObj.AddToFileTransaction(fileTranObj);

                    if (treeObj.RootNode.SelfAddrPtr != this.LoadRootAddr())
                    {
                        fileTranObj.Add(RootAddrName, 0, BitConverter.GetBytes(treeObj.RootNode.SelfAddrPtr));
                    }

                    fileTranObj.Commit();
                }

                DateTime timeB = DateTime.Now;

                MessageBox.Show("增加结点数: " + treeObj.NewNodeCount + " 用时: " + (timeB - timeA).TotalMilliseconds);
            }

            this.button11.Enabled = true;
        }

        private void button12_Click(object sender, EventArgs e)
        {
            this.button12.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                Random randObj = new Random((int)DateTime.Now.Ticks);

                DateTime timeA = DateTime.Now;

                int maxNum = 100000;

                for (int i = 0; i < maxNum; i++)
                {
                    int a = randObj.Next(0, (int)maxNum);

                    treeObj.Remove(a);
                }

                using (FileTransaction fileTranObj = new FileTransaction(TreeDir, TransactionName))
                {
                    globalObj.AddToFileTransaction(fileTranObj);

                    if (treeObj.RootNode.SelfAddrPtr != this.LoadRootAddr())
                    {
                        fileTranObj.Add(RootAddrName, 0, BitConverter.GetBytes(treeObj.RootNode.SelfAddrPtr));
                    }

                    fileTranObj.Commit();
                }

                DateTime timeB = DateTime.Now;

                MessageBox.Show("减少结点数: " + treeObj.ReleaseNodeCount + " 用时: " + (timeB - timeA).TotalMilliseconds);
            }

            this.button12.Enabled = true;
        }

        private Dictionary<long, long> NodeAddrDic = new Dictionary<long, long>();

        private void button9_Click(object sender, EventArgs e)
        {
            this.button9.Enabled = false;

            using (FileTransaction fileTranObj = new FileTransaction(TreeDir, TransactionName))
            {
                using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
                {
                    this.InitGlobal(globalObj, true);

                    BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());
                    {
                        long minNum = 0;
                        long maxNum = 10000;

                        DateTime timeA = DateTime.Now;

                        treeObj.LeafNodeChangeFun = delegate (BTree<long, long>.BTreeNode nodeObj, BTree<long, long>.BTreePair pairObj)
                        {
                            this.NodeAddrDic[pairObj.Value] = nodeObj.SelfAddrPtr;
                        };

                        for (long i = maxNum; i >= minNum; i--)
                        {
                            treeObj.Add(i, i);
                        }

                        globalObj.AddToFileTransaction(fileTranObj);

                        if (treeObj.RootNode.SelfAddrPtr != this.LoadRootAddr())
                        {
                            fileTranObj.Add(RootAddrName, 0, BitConverter.GetBytes(treeObj.RootNode.SelfAddrPtr));
                        }

                        fileTranObj.Commit();

                        DateTime timeB = DateTime.Now;

                        MessageBox.Show("增: " + treeObj.NewNodeCount + "  减: " + treeObj.ReleaseNodeCount + " 用时: " + (timeB - timeA).TotalMilliseconds);
                    }
                }
            }

            this.button9.Enabled = true;
        }

        private void button14_Click(object sender, EventArgs e)
        {
            if (this.NodeAddrDic.Count == 0)
            {
                MessageBox.Show("请先添加数据!");

                return;
            }

            this.button14.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, true);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                DateTime timeA = DateTime.Now;

                foreach (KeyValuePair<long, long> kv in this.NodeAddrDic)
                {
                    treeObj.LeafRemove(kv.Value, kv.Key);
                }

                this.NodeAddrDic.Clear();

                using (FileTransaction fileTranObj = new FileTransaction(TreeDir, TransactionName))
                {
                    globalObj.AddToFileTransaction(fileTranObj);

                    if (treeObj.RootNode.SelfAddrPtr != this.LoadRootAddr())
                    {
                        fileTranObj.Add(RootAddrName, 0, BitConverter.GetBytes(treeObj.RootNode.SelfAddrPtr));
                    }

                    fileTranObj.Commit();
                }

                DateTime timeB = DateTime.Now;

                MessageBox.Show("减少结点数: " + treeObj.ReleaseNodeCount + " 用时: " + (timeB - timeA).TotalMilliseconds);
            }

            this.button14.Enabled = true;
        }

        private void button10_Click(object sender, EventArgs e)
        {
            this.button10.Enabled = false;

            string[] files = Directory.GetFiles(TreeDir);

            foreach (string file in files)
            {
                File.Delete(file);
            }

            this.NodeAddrDic.Clear();

            this.button10.Enabled = true;
        }

        private void button15_Click(object sender, EventArgs e)
        {
            this.button15.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, true);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                StringBuilder builder = new StringBuilder();

                DateTime timeA = DateTime.Now;

                treeObj.Loop_Min_To_Max(delegate (long k, long v)
                {
                    builder.Append(k.ToString() + "\t" + v + "\r\n");
                });

                DateTime timeB = DateTime.Now;

                MessageBox.Show("用时: " + (timeB - timeA).TotalMilliseconds);

                this.textBox1.Text = builder.ToString();
            }

            this.button15.Enabled = true;
        }

        private void button16_Click(object sender, EventArgs e)
        {
            this.button16.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, true);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                StringBuilder builder = new StringBuilder();

                treeObj.Loop_Max_To_Min(delegate (long k, long v)
                {
                    builder.Append(k.ToString() + "\t" + v + "\r\n");
                });

                this.textBox1.Text = builder.ToString();
            }


            this.button16.Enabled = true;
        }

        private void button17_Click(object sender, EventArgs e)
        {
            this.button17.Enabled = false;

            using (BTree<long, long>.BTreeGlobal globalObj = new BTree<long, long>.BTreeGlobal(TreeDir + "\\" + TreeFileName))
            {
                this.InitGlobal(globalObj, false);

                BTree<long, long> treeObj = new BTree<long, long>(globalObj, this.LoadRootAddr());

                List<long> list = new List<long>();

                DateTime timeA = DateTime.Now;

                treeObj.Loop_Min_To_Max(delegate (long k, long v)
                {
                    list.Add(k);
                });

                foreach (long k in list)
                {
                    treeObj.Remove(k);
                }

                using (FileTransaction fileTranObj = new FileTransaction(TreeDir, TransactionName))
                {
                    globalObj.AddToFileTransaction(fileTranObj);

                    if (treeObj.RootNode.SelfAddrPtr != this.LoadRootAddr())
                    {
                        fileTranObj.Add(RootAddrName, 0, BitConverter.GetBytes(treeObj.RootNode.SelfAddrPtr));
                    }

                    fileTranObj.Commit();
                }

                DateTime timeB = DateTime.Now;

                MessageBox.Show("删除结点数: " + treeObj.ReleaseNodeCount + " 用时: " + (timeB - timeA).TotalMilliseconds);
            }

            this.button17.Enabled = true;
        }
    }
}
