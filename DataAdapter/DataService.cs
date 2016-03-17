using System;
using System.Collections;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.ServiceProcess;
using System.Runtime.InteropServices;
using System.Windows.Forms;
using System.Xml;
using System.Xml.Xsl;
using System.Text;
using System.IO;
using System.Collections.Generic;
using System.Messaging;
using System.Xml.Schema;

namespace DataAdapter
{
    [System.ServiceProcess.ServiceProcessDescription("DPA Data Service")]
    public class DataService :  System.ServiceProcess.ServiceBase
    {
        private System.ComponentModel.IContainer components;
        private System.Threading.Thread serviceth;
        System.Data.DataSet settings = new DataSet();
        private int timer=30;
        private bool tcpStoped;
        string ConvertWaitPath = "";
        string ConvertBakPath = "";
        string ConvertErrorPath = "";
        string SendPath = "";
        string SendBakPath = "";
        string SendErrorPath = "";
        string SendMQ = "";
        string ReceiptPath = "";
        string ReceiptMQ = "";
        string XSDFile = "";
        string XSLTFile = "";
        string ConnectionType = "";
        string ConnectionString = "";

        FileInfo fileInfo;
        string fileName = "";
        bool isSuccess = false;

        public DataService()
        {
            InitializeComponent();
        }

        #region static Function For Progrom
        static void Main1(string[] args)
        {
            if (args.Length > 0)
            {
                foreach (string arg in args)
                {
                    switch (arg.ToLower())
                    {
                        case "service":
                            System.ServiceProcess.ServiceBase[] ServicesToRun;
                            ServicesToRun = new System.ServiceProcess.ServiceBase[] { new DataService() };
                            System.ServiceProcess.ServiceBase.Run(ServicesToRun);
                            break;
                        case "install":
                            Install();
                            break;
                        case "uninstall":
                            UnInstall();
                            break;
                        case "start":
                            System.ServiceProcess.ServiceController ctlstart = new ServiceController("DPADataService");
                            System.Console.WriteLine("Start Service Please Wait...");
                            ctlstart.Start();
                            Console.WriteLine("Service Started");
                            break;
                        case "stop":
                            System.ServiceProcess.ServiceController ctlstop = new ServiceController("DPADataService");

                            if (ctlstop.Status != System.ServiceProcess.ServiceControllerStatus.Stopped)
                            {
                                System.Console.WriteLine("Stop Service Please Wait...");
                                ctlstop.Stop();
                                Console.WriteLine("Service Stoped");
                            }
                            break;
                        case "help":
                        default:
                            Console.WriteLine("Usage:DataService option");
                            Console.WriteLine("option:");
                            Console.WriteLine("\tInstall\t\tInstall Service");
                            Console.WriteLine("\tUnInstall\t\tUnInstall Service");
                            Console.WriteLine("\tService\t\tRun Service");
                            Console.WriteLine("If block the option it will be run windows form as controler and moniter");
                            break;
                    }
                }
            }

        }
        static void Install()
        {
            try
            {
                Console.WriteLine("Get Install Util...");
                System.IO.Stream buffer = System.Reflection.Assembly.GetExecutingAssembly().GetManifestResourceStream("NordaSoft.InstallUtil.exe");
                System.IO.FileStream fs = System.IO.File.Open(System.Windows.Forms.Application.StartupPath + "\\InstallUtil.exe", System.IO.FileMode.Create, System.IO.FileAccess.Write);
                System.IO.BinaryReader br = new System.IO.BinaryReader(buffer);
                fs.Write(br.ReadBytes((int)buffer.Length), 0, (int)buffer.Length);
                fs.Close();
                br.Close();
                buffer.Close();
                Console.WriteLine("Install Service...");
                System.Diagnostics.ProcessStartInfo psi = new ProcessStartInfo("InstallUtil.exe", "-i \"" + System.Windows.Forms.Application.ExecutablePath + "\"");
                psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
                System.Diagnostics.Process p = System.Diagnostics.Process.Start(psi);
                p.WaitForExit();
                p.Close();
                Console.WriteLine("Install Finished");
                System.IO.File.Delete(System.Windows.Forms.Application.StartupPath + "\\InstallUtil.exe");
                System.ServiceProcess.ServiceController ctl = new ServiceController("DPADataService");
                System.Console.WriteLine("Start Service Please Wait...");
                ctl.Start();
                Console.WriteLine("Service Started");
            }
            catch (System.Exception err)
            {
                System.Console.WriteLine(err.Message);
            }
        }
        static void UnInstall()
        {
            try
            {
                System.ServiceProcess.ServiceController ctl = new ServiceController("DPADataService");

                if (ctl.Status != System.ServiceProcess.ServiceControllerStatus.Stopped)
                {
                    System.Console.WriteLine("Stop Service Please Wait...");
                    ctl.Stop();
                    Console.WriteLine("Service Stoped");
                }
                Console.WriteLine("Get UnInstall Util...");
                System.IO.Stream buffer = System.Reflection.Assembly.GetExecutingAssembly().GetManifestResourceStream("NordaSoft.InstallUtil.exe");
                System.IO.FileStream fs = System.IO.File.Open(System.Windows.Forms.Application.StartupPath + "\\InstallUtil.exe", System.IO.FileMode.Create, System.IO.FileAccess.Write);
                System.IO.BinaryReader br = new System.IO.BinaryReader(buffer);
                fs.Write(br.ReadBytes((int)buffer.Length), 0, (int)buffer.Length);
                fs.Close();
                br.Close();
                buffer.Close();
                Console.WriteLine("UnInstall Service...");
                System.Diagnostics.ProcessStartInfo psi = new ProcessStartInfo("InstallUtil.exe", "-u \"" + System.Windows.Forms.Application.ExecutablePath + "\"");
                psi.WindowStyle = System.Diagnostics.ProcessWindowStyle.Hidden;
                System.Diagnostics.Process p = System.Diagnostics.Process.Start(psi);
                p.WaitForExit();
                p.Close();
                System.IO.File.Delete(System.Windows.Forms.Application.StartupPath + "\\InstallUtil.exe");
                Console.WriteLine("Service UnInstalled");
            }
            catch (System.Exception err)
            {
                System.Console.WriteLine(err.Message);
            }
        }
        #endregion

        #region 系统构造
        private void InitializeComponent()
        {
            this.AutoLog = false;
            this.CanShutdown = true;
            this.ServiceName = "DPADataService";
        }
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (components != null)
                {
                    components.Dispose();
                }
            }
            base.Dispose(disposing);
        }
        protected override void OnStart(string[] args)
        {
            // TODO: Add code here to start your service.
            tcpStoped = false;
            this.WriteLog("Service Start");
            serviceth = new System.Threading.Thread(new System.Threading.ThreadStart(Service));
            serviceth.Start();
            System.Threading.Thread.Sleep(5000);
            if (!serviceth.IsAlive)
            {
                System.ServiceProcess.ServiceController sc = new ServiceController("DPADataService");
                sc.Stop();
                sc.Dispose();
            }
        }
        protected override void OnStop()
        {
            // TODO: Add code here to perform any tear-down necessary to stop your service.
            tcpStoped = true;
            this.WriteLog("Wite For Service Stop");
            while (serviceth.IsAlive)
            {
                System.Threading.Thread.Sleep(1000);
            }
            this.WriteLog("Service Stoped");
        }
        #endregion

        public void WriteLog(string message)
        {
            try
            {
                System.IO.StreamWriter sw = new System.IO.StreamWriter(System.Windows.Forms.Application.StartupPath + "\\Log\\DSEvent" + DateTime.Now.ToString("yyyyMMdd") + ".log", true);
                sw.Write(DateTime.Now.ToString() + "\t" + message + "\r\n");
                sw.Close();
            }
            catch
            { }
        }
        public void MoveFile(string fileName, string bkpath)
        {
            try
            {
                if (string.IsNullOrEmpty(bkpath))
                {
                    File.Delete(fileName);
                    return;
                }
                string fname = bkpath + "\\" + Path.GetFileName(fileName);
                if (Path.GetExtension(fname) == "")
                {
                    fname = fname + ".xml";
                }
                if (!Directory.Exists(bkpath))
                {
                    Directory.CreateDirectory(bkpath);
                }
                if (File.Exists(fname))
                {
                    File.Delete(fname);
                }

                File.Move(fileName, fname);
            }
            catch
            {
                File.Delete(fileName);
            }
        }
        private IDataAccess GetDataAccess()
        {
            try
            {
                IDataAccess idac = null;
                if (string.IsNullOrEmpty(ConnectionType) == false && string.IsNullOrEmpty(ConnectionString) == false)
                {
                    switch (ConnectionType.ToUpper())
                    {
                        case "SQLSERVER":
                            idac = new SQLDataAccess(ConnectionString);
                            //WriteLog("Connect To SQL");
                            break;
                        case "ORACLE":
                            idac = new OraDataAccess(ConnectionString);
                            WriteLog("Connect To ORA");
                            break;
                        case "OLEDB":
                            idac = new OleDataAccess(ConnectionString);
                            WriteLog("Connect To OLE");
                            break;
                    }

                }
                return idac;
            }
            catch (Exception ex)
            {
                WriteLog("数据库配置有误：" + ex.Message);
                return null;
            }
        }
        private bool SendToMSMQ(string mqPath, string sendFilePath, string label)
        {
            bool flag = true;
            try
            {
                MessageQueue sendQueue = new MessageQueue(mqPath);
                //MessageQueue sendQueue = new MessageQueue(@"FormatName:DIRECT=TCP:192.168.8.47\private$\receipt");
                sendQueue.Formatter = new System.Messaging.XmlMessageFormatter(new Type[] { typeof(XmlDocument) });

                XmlDocument docReply = new XmlDocument();
                docReply.Load(sendFilePath);

                System.Messaging.Message msg = new System.Messaging.Message();
                msg.Body = docReply;
                msg.Label = label;
                sendQueue.Send(msg, MessageQueueTransactionType.Single);
            }
            catch (Exception ex)
            {
                this.WriteLog("发送MQ异常:" + ex.Message + ex.StackTrace);
                flag = false;
            }
            return flag;
        }

        //服务主函数
        private void Service()
        {
            #region 获取设置
            //ESettings.xml,加密后的配置文件
            //Settings.xml，原始配置文件，此文件在生成了ESettings.xml后可以删除
            try
            {
                #region 获取设置 settings
                System.IO.MemoryStream settingStream = new System.IO.MemoryStream();
                System.IO.FileStream efs;
                System.IO.FileStream nfs;
                byte[] key = System.Text.Encoding.Unicode.GetBytes("加密秘钥");//'加密秘钥'可修改
                byte[] iv = System.Text.Encoding.Unicode.GetBytes("初始矢量");//‘初始矢量’可修改
                System.Security.Cryptography.DES des = System.Security.Cryptography.DES.Create();
                if (System.IO.File.Exists(System.Windows.Forms.Application.StartupPath + "\\ESettings.xml"))
                {
                    efs = new System.IO.FileStream(System.Windows.Forms.Application.StartupPath + "\\ESettings.xml", System.IO.FileMode.Open);
                    System.Security.Cryptography.CryptoStream cs = new System.Security.Cryptography.CryptoStream(efs, des.CreateDecryptor(key, iv), System.Security.Cryptography.CryptoStreamMode.Read);
                    byte[] buff = new byte[1024];
                    int l = 0;
                    for (; ; )
                    {
                        l = cs.Read(buff, 0, 1024);
                        settingStream.Write(buff, 0, l);
                        if (l < 1024)
                            break;
                    }
                    settingStream.Position = 0;
                    cs.Close();
                    efs.Close();
                }
                else
                {
                    nfs = new System.IO.FileStream(System.Windows.Forms.Application.StartupPath + "\\Settings.xml", System.IO.FileMode.Open);
                    efs = new System.IO.FileStream(System.Windows.Forms.Application.StartupPath + "\\ESettings.xml", System.IO.FileMode.Create);
                    System.Security.Cryptography.CryptoStream cs = new System.Security.Cryptography.CryptoStream(efs, des.CreateEncryptor(key, iv), System.Security.Cryptography.CryptoStreamMode.Write);
                    byte[] buff = new byte[1024];
                    int l = 0;
                    for (; ; )
                    {
                        l = nfs.Read(buff, 0, 1024);
                        cs.Write(buff, 0, l);
                        settingStream.Write(buff, 0, l);
                        if (l < 1024)
                            break;
                    }
                    cs.FlushFinalBlock();
                    cs.Close();
                    nfs.Close();
                    efs.Close();
                    settingStream.Position = 0;
                }
                System.IO.StreamReader sr = new System.IO.StreamReader(settingStream, System.Text.Encoding.UTF8);
                settings.ReadXml(sr);
                settingStream.Close();
                #endregion
                if (settings == null || settings.Tables.Count == 0 || settings.Tables["App"] == null || settings.Tables["App"].Rows.Count == 0)
                {
                    this.WriteLog("无配置文件或配置信息有误");
                    tcpStoped = true;
                    return;
                }
                #region 获取配置值
                #region 必填路径
                if (settings.Tables["App"].Columns.Contains("SendPath") == false || settings.Tables["App"].Rows[0]["SendPath"].ToString().Trim() == "")
                {
                    this.WriteLog("未配置报文发送路径");
                    tcpStoped = true;
                    return;
                }
                if (settings.Tables["App"].Columns.Contains("SendBakPath") == false || settings.Tables["App"].Rows[0]["SendBakPath"].ToString().Trim() == "")
                {
                    this.WriteLog("未配置报文发送备份路径");
                    tcpStoped = true;
                    return;
                }
                if (settings.Tables["App"].Columns.Contains("SendMQ") == false || settings.Tables["App"].Rows[0]["SendMQ"].ToString().Trim() == "")
                {
                    this.WriteLog("未配置报文发送MQ");
                    tcpStoped = true;
                    return;
                }
                if (settings.Tables["App"].Columns.Contains("ReceiptPath") == false || settings.Tables["App"].Rows[0]["ReceiptPath"].ToString().Trim() == "")
                {
                    this.WriteLog("未配置回执路径");
                    tcpStoped = true;
                    return;
                }
                #endregion

                SendPath = settings.Tables["App"].Rows[0]["SendPath"].ToString().Trim();
                SendBakPath = settings.Tables["App"].Rows[0]["SendBakPath"].ToString().Trim() + "\\" + DateTime.Now.ToString("yyyyMM");
                if (!Directory.Exists(SendBakPath))
                {
                    Directory.CreateDirectory(SendBakPath);
                }
                SendErrorPath = settings.Tables["App"].Rows[0]["SendBakPath"].ToString().Trim() + "\\Error";
                if (!Directory.Exists(SendErrorPath))
                {
                    Directory.CreateDirectory(SendErrorPath);
                }
                SendMQ = settings.Tables["App"].Rows[0]["SendMQ"].ToString().Trim();
                ReceiptPath = settings.Tables["App"].Rows[0]["ReceiptPath"].ToString().Trim();

                if (settings.Tables["App"].Columns.Contains("Timer") == true && settings.Tables["App"].Rows[0]["Timer"].ToString().Trim() != "" &&
                    int.TryParse(settings.Tables["App"].Rows[0]["Timer"].ToString().Trim(), out timer) == false)
                {
                    timer = 30;
                }

                if (settings.Tables["App"].Columns.Contains("ConvertWaitPath") == true && settings.Tables["App"].Rows[0]["ConvertWaitPath"].ToString().Trim() != "")
                {
                    ConvertWaitPath = settings.Tables["App"].Rows[0]["ConvertWaitPath"].ToString().Trim();
                }
                if (settings.Tables["App"].Columns.Contains("ConvertBakPath") == true && settings.Tables["App"].Rows[0]["ConvertBakPath"].ToString().Trim() != "")
                {
                    ConvertBakPath = settings.Tables["App"].Rows[0]["ConvertBakPath"].ToString().Trim() + "\\" + DateTime.Now.ToString("yyyyMM");
                    if (!Directory.Exists(ConvertBakPath))
                    {
                        Directory.CreateDirectory(ConvertBakPath);
                    }
                    ConvertErrorPath = settings.Tables["App"].Rows[0]["ConvertBakPath"].ToString().Trim() + "\\Error";
                    if (!Directory.Exists(ConvertErrorPath))
                    {
                        Directory.CreateDirectory(ConvertErrorPath);
                    }
                }
                if (settings.Tables["App"].Columns.Contains("ReceiptMQ") == true && settings.Tables["App"].Rows[0]["ReceiptMQ"].ToString().Trim() != "")
                {
                    ReceiptMQ = settings.Tables["App"].Rows[0]["ReceiptMQ"].ToString().Trim();
                }
                if (settings.Tables["App"].Columns.Contains("XSDFile") == true && settings.Tables["App"].Rows[0]["XSDFile"].ToString().Trim() != "")
                {
                    XSDFile = settings.Tables["App"].Rows[0]["XSDFile"].ToString().Trim();
                }
                if (settings.Tables["App"].Columns.Contains("XSLTFile") == true && settings.Tables["App"].Rows[0]["XSLTFile"].ToString().Trim() != "")
                {
                    XSLTFile = settings.Tables["App"].Rows[0]["XSLTFile"].ToString().Trim();
                }

                //数据库可以不配置，不配置时不记录发送历史
                if (settings.Tables["App"].Columns.Contains("ConnectionType") == true && settings.Tables["App"].Rows[0]["ConnectionType"].ToString().Trim() != "")
                {
                    ConnectionType = settings.Tables["App"].Rows[0]["ConnectionType"].ToString().Trim();
                }
                //数据库可以不配置，不配置时不记录发送历史
                if (settings.Tables["App"].Columns.Contains("ConnectionString") == true && settings.Tables["App"].Rows[0]["ConnectionString"].ToString().Trim() != "")
                {
                    ConnectionString = settings.Tables["App"].Rows[0]["ConnectionString"].ToString().Trim();
                }
                
                #endregion
            }
            catch (System.Exception error)
            {
                this.WriteLog(error.Message);
                tcpStoped = true;
            }
            #endregion
            IDataAccess DAC = GetDataAccess();
            do
            {
                #region 1.校验 2.转换
                if (ConvertWaitPath.Trim() != "" && ((XSDFile != "" && File.Exists(XSDFile)) || (XSLTFile != "" && File.Exists(XSLTFile))))
                {
                    foreach (string fileFullName in Directory.GetFiles(ConvertWaitPath.Trim()))
                    {
                        try
                        {
                            fileInfo = new FileInfo(fileFullName);
                            if ((DateTime.Now - fileInfo.LastWriteTime).TotalSeconds <= 10)//最后修改时间10秒内的文件不处理，防止文件未写完
                            {
                                continue;
                            }
                            fileName = fileInfo.Name;
                            if (fileInfo.Length <= 0)
                            {
                                //此处可以生成校验错误的回执文件，暂无
                                MoveFile(fileFullName, ConvertErrorPath);
                                this.WriteLog("报文校验异常：报文无内容.报文文件：" + fileName);
                                continue;
                            }
                            if (XSDFile != "" && File.Exists(XSDFile))
                            {
                                try
                                {
                                    //方法一
                                    string message = XmlValidationByXsd(fileFullName, XSDFile, null);
                                    if (message != "")
                                    {
                                        //此处可以生成校验错误的回执文件，暂无
                                        MoveFile(fileFullName, ConvertErrorPath);
                                        this.WriteLog("报文校验异常:" + message);
                                    }

                                    ////方法二
                                    //if (!IsValidXml(fileFullName, XSDFile))
                                    //{
                                    //    //此处可以生成校验错误的回执文件，暂无
                                    //    MoveFile(fileFullName, ConvertErrorPath);
                                    //    this.WriteLog("报文校验异常:" + Errors);
                                    //}
                                    ////方法三
                                    //if (!IsValidXmlByReader(fileFullName))
                                    //{
                                    //    //此处可以生成校验错误的回执文件，暂无
                                    //    MoveFile(fileFullName, ConvertErrorPath);
                                    //    this.WriteLog("报文校验异常:" + Errors);
                                    //}
                                    //if (!IsValidXmlByDataset(fileFullName))
                                    //{
                                    //    //此处可以生成校验错误的回执文件，暂无
                                    //    MoveFile(fileFullName, ConvertErrorPath);
                                    //    this.WriteLog("报文校验异常:" + Errors);
                                    //}
                                }
                                catch (Exception ex2)
                                {
                                    MoveFile(fileFullName, ConvertErrorPath);
                                    this.WriteLog("报文校验异常:" + ex2.Message + ex2.StackTrace);
                                }
                            }
                            if (XSLTFile != "" && File.Exists(XSLTFile))
                            {
                                try
                                {
                                    System.Xml.Xsl.XslCompiledTransform transform = new XslCompiledTransform();
                                    transform.Load(XSLTFile);
                                    transform.Transform(fileFullName, SendPath + "\\" + fileName);

                                    MoveFile(fileFullName, ConvertBakPath);
                                }
                                catch (Exception ex2)
                                {
                                    //此处可以生成校验错误的回执文件，暂无
                                    MoveFile(fileFullName, ConvertErrorPath);
                                    this.WriteLog("报文转换异常：" + ex2.Message + ex2.StackTrace);
                                    continue;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            //此处可以生成校验错误的回执文件，暂无
                            MoveFile(fileFullName, ConvertErrorPath);
                            this.WriteLog("报文校验或转换异常：" + ex.Message + ".报文文件：" + fileName);
                            continue;
                        }
                    }
                }
                #endregion
                #region 3.MQ发送
                if (SendPath.Trim() != "")
                {
                    foreach (string fileFullName in Directory.GetFiles(SendPath.Trim()))
                    {
                        try
                        {
                            fileInfo = new FileInfo(fileFullName);
                            if ((DateTime.Now - fileInfo.LastWriteTime).TotalSeconds <= 10)
                            {
                                continue;
                            }
                            fileName = fileInfo.Name;

                            if (fileInfo.Length <= 0)
                            {
                                //下面1行代码，将文件备份，但出错时已转移到错误文件夹，所以此处可注释不备份
                                //File.Copy(fileFullName, SendBakPath + "\\" + fileName);
                                MoveFile(fileFullName, SendErrorPath);
                                this.WriteLog("发送失败：报文无内容.报文名：" + fileFullName);
                                continue;
                            }

                            isSuccess = SendToMSMQ(SendMQ, fileFullName, Path.GetFileNameWithoutExtension(fileName));
                            if (!isSuccess)
                            {
                                //下面1行代码，将文件备份，但出错时已转移到错误文件夹，所以此处可注释不备份
                                //File.Copy(fileFullName, SendBakPath + "\\" + fileName);
                                MoveFile(fileFullName, SendErrorPath);
                                continue;
                            }

                            MoveFile(fileFullName, SendBakPath);
                            //DAC.ExecuteSql("INSERT INTO SendLog(CreateDate,CreateUserName,BackupFullName,FileName,Status) VALUES(GETDATE(),'报文发送','" + SendBakPath + "\\" + Path.GetFileName(fileName) + "','" + Path.GetFileName(fileName) + "','发送成功')");
                            if (DAC.ErrMessages != "")
                            {
                                WriteLog("文件" + fileName + "发送成功,但写入日志失败！");
                            }
                            else
                            {
                                WriteLog("文件" + fileName + "发送成功！");
                            }
                        }
                        catch (Exception ex)
                        {
                            MoveFile(fileFullName, SendErrorPath);
                            this.WriteLog("MQ发送异常:" + ex.Message + ex.StackTrace);
                        }
                    }
                }
                #endregion
                #region 4.MQ接收  回执
                if (ReceiptPath != "" && ReceiptMQ != "")
                {
                    try
                    {
                        MessageQueue receiveQueue = new MessageQueue(ReceiptMQ);
                        receiveQueue.Formatter = new XmlMessageFormatter(new Type[] { typeof(XmlDocument) });
                        MessageEnumerator myEnumerator = receiveQueue.GetMessageEnumerator2();
                        if (myEnumerator.MoveNext())
                        {
                            System.Messaging.Message msg = myEnumerator.Current;
                            try
                            {
                                if (msg != null && msg.Body != null)
                                {
                                    XmlDocument docReply = (XmlDocument)msg.Body;
                                    docReply.Save(ReceiptPath + "\\" + msg.Label.Trim() + ".xml");
                                    WriteLog("文件" + msg.Label.Trim() + ".xml" + "接收成功！");
                                }
                            }
                            catch (Exception ex)
                            {
                                this.WriteLog("获取MQ回执异常:" + ex.Message + ex.StackTrace);
                            }
                            myEnumerator.RemoveCurrent();
                        }
                    }
                    catch (Exception ex2)
                    {
                        this.WriteLog("获取MQ回执异常:" + ex2.Message + ex2.StackTrace);
                    }
                }
                #endregion

                for (int t = 0; t < timer; t++)
                {
                    System.Threading.Thread.Sleep(1000);
                    if (tcpStoped)
                    {
                        WriteLog("Service Stop By User" + tcpStoped.ToString());
                        break;
                    }
                }
            } while (!tcpStoped);
        }

        #region XSD校验方式一 XML报文, 不处理校验错误的具体错误文字
        /// <summary>  
        /// 通过xsd验证xml格式是否正确，正确返回空字符串，错误返回提示  
        /// </summary>  
        /// <param name="xmlFile">xml文件</param>  
        /// <param name="xsdFile">xsd文件</param>  
        /// <param name="namespaceUrl">命名空间，无则默认为null</param>  
        /// <returns></returns>  
        public static string XmlValidationByXsd(string xmlFile, string xsdFile, string namespaceUrl = null)
        {
            StringBuilder sb = new StringBuilder();
            XmlReaderSettings settings = new XmlReaderSettings();
            settings.ValidationType = ValidationType.Schema;
            settings.Schemas.Add(namespaceUrl, xsdFile);
            settings.ValidationEventHandler += (x, y) =>
            {
                sb.AppendFormat("{0}|", y.Message);
            };
            using (XmlReader reader = XmlReader.Create(xmlFile, settings))
            {
                try
                {
                    while (reader.Read()) ;
                }
                catch (XmlException ex)
                {
                    sb.AppendFormat("{0}|", ex.Message);
                }
            }
            return sb.ToString();
        }
        /// <summary>  
        /// 通过xsd验证xml格式是否正确，正确返回空字符串，错误返回提示  
        /// </summary>  
        /// <param name="xmlText">xml文本内容</param>  
        /// <param name="schemaFile">xsd文件</param>  
        /// <returns></returns>  
        public static string XmlValidateByXsd(string xmlText, string schemaFile)
        {
            StringBuilder sb = new StringBuilder();
            XmlReaderSettings settings = new XmlReaderSettings();
            settings.ValidationType = ValidationType.Schema;
            settings.Schemas.Add(null, schemaFile);
            settings.ValidationEventHandler += (x, y) =>
            {
                sb.AppendFormat("{0}\n", y.Message);
            };
            using (XmlReader reader = XmlReader.Create(new StringReader(xmlText), settings))
            {
                try
                {
                    while (reader.Read()) ;
                }
                catch (XmlException ex)
                {
                    sb.AppendFormat("{0}\n", ex.Message);
                }
            }
            return sb.ToString();
        }
        #endregion
       
        #region XSD校验二，可自定义XSD校验错误文字
        private int nErrors = 0;
        private string strErrorMsg = string.Empty;
        public string Errors { get { return strErrorMsg; } }
        public bool IsValidXml(string xmlPath, string xsdPath)
        {
            bool bStatus = false;
            try
            {
                // Declare local objects
                XmlReaderSettings rs = new XmlReaderSettings();
                rs.ValidationType = ValidationType.Schema;
                rs.ValidationFlags |= XmlSchemaValidationFlags.ProcessSchemaLocation | XmlSchemaValidationFlags.ReportValidationWarnings;
                // Event Handler for handling exception & 
                // this will be called whenever any mismatch between XML & XSD
                rs.ValidationEventHandler += new ValidationEventHandler(ValidationEventHandler);
                rs.Schemas.Add(null, XmlReader.Create(xsdPath));


                // reading xml
                using (XmlReader xmlValidatingReader = XmlReader.Create(xmlPath, rs))
                { while (xmlValidatingReader.Read()) { } }

                ////Exception if error.
                if (nErrors > 0) { throw new Exception(strErrorMsg); }
                else { bStatus = true; }//Success
            }
            catch (Exception error) { bStatus = false; }

            return bStatus;
        }
        void ValidationEventHandler(object sender, ValidationEventArgs e)
        {
            if (e.Severity == XmlSeverityType.Warning) strErrorMsg += "WARNING: ";
            else strErrorMsg += "ERROR: ";
            nErrors++;

            if (e.Exception.Message.Contains("'Email' element is invalid"))
            {
                strErrorMsg = strErrorMsg + getErrorString("Provided Email data is Invalid", "CAPIEMAIL007") + "\r\n";
            }
            if (e.Exception.Message.Contains("The element 'Person' has invalid child element"))
            {
                strErrorMsg = strErrorMsg + getErrorString("Provided XML contains invalid child element", "CAPINVALID005") + "\r\n";
            }
            else
            {
                strErrorMsg = strErrorMsg + e.Exception.Message + "\r\n";
            }
        }
        string getErrorString(string erroString, string errorCode)
    {
        StringBuilder errXMl = new StringBuilder();
        errXMl.Append("<MyError> <errorString> ERROR_STRING </errorString><errorCode> ERROR_CODE </errorCode> </MyError>");
        errXMl.Replace("ERROR_STRING", erroString);
        errXMl.Replace("ERROR_CODE", errorCode);
        return errXMl.ToString();
    }
        #endregion

        #region XSD校验三，解析报文后自定义错误文字
        public bool IsValidXmlByReader(string xmlPath)
        {
            string strTemp;
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.Load(fileName);
            XmlElement root = xmlDoc.DocumentElement;
            XmlNodeList nodeList;

            string prefix = "ns:";
            XmlNamespaceManager xmlns = new XmlNamespaceManager(xmlDoc.NameTable);
            xmlns.AddNamespace("ns", "http://xmlns.oracle.com");//直接读带有namespace的xml时，需要加上相应的namespace值

            strTemp = GetNodeText(root, "{0}报关单表头/{0}企业内部编号", prefix, xmlns);//企业内部编号
            if (strTemp.Length > 18)
            {
                strErrorMsg = strErrorMsg + getErrorString("企业内部编号不能超过18位", "ClientSeqNo") + "\r\n";
                return false;
            }

            nodeList = root.SelectNodes(string.Format("{0}商品信息", prefix), xmlns);
            if (nodeList == null || nodeList.Count == 0)
            {
                strErrorMsg = strErrorMsg + getErrorString("报关单表体的商品信息为空", "DecList") + "\r\n";
                return false;
            }
            return true;
        }
        public bool IsValidXmlByDataset(string xmlPath)
        {
            DataSet dsXML = new DataSet();
            dsXML.ReadXml(xmlPath);
            if (dsXML == null || dsXML.Tables.Count == 0)
            {
                strErrorMsg = strErrorMsg + getErrorString("报文无数据", "XMLEmpty") + "\r\n";
                return false;
            }
            if (dsXML.Tables.Contains("商品信息") == false || dsXML.Tables["商品信息"] == null || dsXML.Tables["商品信息"].Rows.Count == 0)
            {
                strErrorMsg = strErrorMsg + getErrorString("报关单表体的商品信息为空", "DecList") + "\r\n";
                return false;
            }
            return true;
        }

        private string GetNodeText(XmlNode parent, string nodeName, string prefix, XmlNamespaceManager xmlns)
        {
            if (parent == null || parent.ChildNodes.Count <= 0 || nodeName.Trim() == "")
            {
                return "";
            }
            XmlNode nodeTemp = parent.SelectSingleNode(string.Format(nodeName.Trim(), prefix), xmlns);
            if (nodeTemp == null || nodeTemp.InnerText.Trim() == "")
            {
                return "";
            }
            return nodeTemp.InnerText.Trim();
        }
        #endregion
    }
}
