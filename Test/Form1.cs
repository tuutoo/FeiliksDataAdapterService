using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using System.Collections;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Windows.Forms;
using System.Xml;
using System.Xml.Xsl;
using System.Text;
using System.IO;
using System.Messaging;
using System.Xml.Schema;

namespace Test
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }
        System.Data.DataSet settings = new DataSet();
        private int timer = 30;
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

        private void button1_Click(object sender, EventArgs e)
        {
            #region 获取设置
            //ESettings.xml,加密后的配置文件
            //Settings.xml，原始配置文件，此文件在生成了ESettings.xml后可以删除
            //this.WriteEntry("Get Settings");
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
                //IDataAccess DAC = GetDataAccess();
                #endregion
            }
            catch (System.Exception error)
            {
                this.WriteLog(error.Message);
                tcpStoped = true;
            }
            #endregion

            do
            {
                #region 1.校验 2.转换
                if (ConvertWaitPath.Trim() != "" && ((XSDFile != "" && File.Exists(XSDFile)) || (XSLTFile != "" && File.Exists(XSLTFile))))
                {
                    foreach (string fileFullName in Directory.GetFiles(ConvertWaitPath.Trim()))
                    {
                        fileInfo = new FileInfo(fileFullName);
                        if ((DateTime.Now - fileInfo.LastWriteTime).TotalSeconds <= 10)
                        {
                            continue;
                        }
                        fileName = fileInfo.Name;
                        if (fileInfo.Length <= 0)
                        {
                            if (File.Exists(ConvertBakPath + "\\" + fileName))
                            {
                                File.Delete(ConvertBakPath + "\\" + fileName);
                            }
                            File.Copy(fileFullName, ConvertBakPath + "\\" + fileName);
                            MoveFile(fileFullName, ConvertErrorPath);
                            this.WriteLog("报文校验异常：报文无内容.报文文件：" + fileName);
                            continue;
                        }
                        if (XSDFile != "" && File.Exists(XSDFile))
                        {
                            try
                            {
                                Errors = ""; nErrors = 0;
                                if (!IsValidXml(fileFullName, XSDFile))
                                {
                                    #region 回执
                                    string timed = DateTime.Now.ToString("yyyyMMddHHmmssffff");
                                    XmlTextWriter xtw = new XmlTextWriter(ReceiptPath + "\\PDA_" + timed + ".xml", Encoding.UTF8);
                                    xtw.Formatting = Formatting.Indented;
                                    xtw.IndentChar = '\t';

                                    xtw.WriteStartDocument();
                                    xtw.WriteStartElement("TCS101Message");

                                    xtw.WriteStartElement("MessageHead");
                                    #region MessageHead
                                    WriteElementSJ(xtw, "MessageType", "001");
                                    WriteElementSJ(xtw, "MessageId", "TCS001-" + timed);
                                    WriteElementSJ(xtw, "MessageTime", DateTime.Now.ToString("yyyyMMddHHmmss"));
                                    WriteElementSJ(xtw, "SenderId", "D999999999999");
                                    WriteElementSJ(xtw, "SenderAddress", "D999999999999@DPA1001");
                                    WriteElementSJ(xtw, "ReceiverId", "D999999999999");
                                    WriteElementSJ(xtw, "ReceiverAddress", "D999999999999@DPA1001");
                                    #endregion
                                    xtw.WriteEndElement();

                                    xtw.WriteStartElement("MessageBody");
                                    xtw.WriteStartElement("TcsFlow201Response");
                                    #region ResponseHead
                                    xtw.WriteStartElement("ResponseHead");
                                    WriteElementSJ(xtw, "MessageId", "T74391723100201510191038015770");
                                    WriteElementSJ(xtw, "RequestMessageId", "");
                                    WriteElementSJ(xtw, "BpNo", "");
                                    WriteElementSJ(xtw, "TaskId", "T320251K0010201510190000000001");
                                    WriteElementSJ(xtw, "CorpTaskId", "");
                                    xtw.WriteEndElement();
                                    #endregion

                                    #region ResponseList
                                    xtw.WriteStartElement("ResponseList");
                                    xtw.WriteRaw(Errors);
                                    xtw.WriteEndElement();
                                    #endregion
                                    xtw.WriteEndElement();
                                    xtw.WriteEndElement();

                                    xtw.WriteEndElement();
                                    xtw.WriteEndDocument();

                                    xtw.Flush();
                                    xtw.Close();
                                    #endregion
                                    if (File.Exists(ConvertBakPath + "\\" + fileName))
                                    {
                                        File.Delete(ConvertBakPath + "\\" + fileName);
                                    }
                                    File.Copy(fileFullName, ConvertBakPath + "\\" + fileName);
                                    MoveFile(fileFullName, ConvertErrorPath);
                                    this.WriteLog("报文校验异常:" + Errors);
                                    continue;
                                }
                                //如果XSD验证不通过，则返回错误回执
                                //string message = XmlValidationByXsd(fileFullName, XSDFile, null);
                                //if (message != "")
                                //{
                                //    File.Copy(fileFullName, ConvertBakPath + "\\" + fileName);
                                //    MoveFile(fileFullName, ConvertErrorPath);
                                //    this.WriteLog("报文校验异常:" + message);
                                //}
                            }
                            catch (Exception ex2)
                            {
                                if (File.Exists(ConvertBakPath + "\\" + fileName))
                                {
                                    File.Delete(ConvertBakPath + "\\" + fileName);
                                }
                                File.Copy(fileFullName, ConvertBakPath + "\\" + fileName);
                                MoveFile(fileFullName, ConvertErrorPath);
                                this.WriteLog("报文校验异常:" + ex2.Message + ex2.StackTrace);
                                continue;
                            }
                        }
                        if (XSLTFile != "" && File.Exists(XSLTFile))
                        {
                            try
                            {
                                System.Xml.Xsl.XslCompiledTransform transform = new XslCompiledTransform();
                                transform.Load(XSLTFile);
                                transform.Transform(fileFullName, SendPath + "\\" + fileName);  
                            }
                            catch (Exception ex2)
                            {
                                if (File.Exists(ConvertBakPath + "\\" + fileName))
                                {
                                    File.Delete(ConvertBakPath + "\\" + fileName);
                                }
                                File.Copy(fileFullName, ConvertBakPath + "\\" + fileName);
                                MoveFile(fileFullName, ConvertErrorPath);
                                this.WriteLog("报文转换异常：" + ex2.Message + ex2.StackTrace);
                                continue;
                            }
                        }
                        if (File.Exists(fileFullName))
                        {
                            MoveFile(fileFullName, ConvertBakPath);
                        }
                    }
                }
                #endregion
                #region 3.MQ发送
                if (SendPath.Trim() != "")
                {
                    foreach (string fileFullName in Directory.GetFiles(SendPath.Trim()))
                    {
                        fileInfo = new FileInfo(fileFullName);
                        if ((DateTime.Now - fileInfo.LastWriteTime).TotalSeconds <= 10)
                        {
                            continue;
                        }
                        fileName = fileInfo.Name;

                        if (fileInfo.Length <= 0)
                        {
                            if (File.Exists(SendBakPath + "\\" + fileName))
                            {
                                File.Delete(SendBakPath + "\\" + fileName);
                            }
                            File.Copy(fileFullName, SendBakPath + "\\" + fileName);
                            MoveFile(fileFullName, SendErrorPath);
                            this.WriteLog("发送失败：报文无内容.报文名：" + fileFullName);
                            continue;
                        }
                        try
                        {
                            isSuccess = SendToMSMQ(SendMQ, fileFullName, Path.GetFileNameWithoutExtension(fileName));
                            if (!isSuccess)
                            {
                                if (File.Exists(SendBakPath + "\\" + fileName))
                                {
                                    File.Delete(SendBakPath + "\\" + fileName);
                                }
                                File.Copy(fileFullName, SendBakPath + "\\" + fileName);
                                MoveFile(fileFullName, SendErrorPath);
                                continue;
                            }

                            MoveFile(fileFullName, SendBakPath);
                            WriteLog("文件" + fileName + "发送成功！");
                        }
                        catch (Exception ex)
                        {
                            MoveFile(fileFullName, SendErrorPath);
                            this.WriteLog("MQ发送异常:" + ex.Message + ex.StackTrace);
                        }
                    }
                }
                #endregion
                #region 4.MQ接收
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


        public void WriteLog(string message)
        {
            try
            {
                System.IO.StreamWriter sw = new System.IO.StreamWriter(System.Windows.Forms.Application.StartupPath + "\\Log\\DSEvent" + DateTime.Now.ToString("yyyyMMddHH") + ".log", true);
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
        //private IDataAccess GetDataAccess()
        //{
        //    try
        //    {
        //        IDataAccess idac = null;
        //        if (string.IsNullOrEmpty(ConnectionType) == false && string.IsNullOrEmpty(ConnectionString) == false)
        //        {
        //            switch (ConnectionType.ToUpper())
        //            {
        //                case "SQLSERVER":
        //                    idac = new SQLDataAccess(ConnectionString);
        //                    //WriteLog("Connect To SQL");
        //                    break;
        //                case "ORACLE":
        //                    idac = new OraDataAccess(ConnectionString);
        //                    WriteLog("Connect To ORA");
        //                    break;
        //                case "OLEDB":
        //                    idac = new OleDataAccess(ConnectionString);
        //                    WriteLog("Connect To OLE");
        //                    break;
        //            }

        //        }
        //        return idac;
        //    }
        //    catch (Exception ex)
        //    {
        //        WriteLog("数据库配置有误：" + ex.Message);
        //        return null;
        //    }
        //}

        private void WriteElementSJ(XmlTextWriter xtw, string el, string v)
        {
            xtw.WriteStartElement(el);
            xtw.WriteString(v);
            xtw.WriteEndElement();
        }

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


        private int nErrors = 0;
        private string strErrorMsg = string.Empty;
        public string Errors { get { return strErrorMsg; } set { strErrorMsg = value; } }
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
                {
                    while (xmlValidatingReader.Read()) { }
                }

                ////Exception if error.
                if (nErrors > 0)
                {
                    //throw new Exception(strErrorMsg);
                    bStatus = false;
                }
                else
                {
                    //Success
                    bStatus = true;
                }
            }
            catch (Exception error)
            {
                bStatus = false;
            }
            return bStatus;
        }

        void ValidationEventHandler(object sender, ValidationEventArgs e)
        {
            //if (e.Severity == XmlSeverityType.Warning)
            //{
            //    strErrorMsg += "WARNING: ";
            //}
            //else
            //{
            //    strErrorMsg += "ERROR: ";
            //}
            nErrors++;

            if (e.Exception.Message.Contains("企业内部编号"))
            {
                strErrorMsg = strErrorMsg + getErrorString("企业内部编号 必填并且必须是18位", "") ;
            }
            else if (e.Exception.Message.Contains("申报单位代码"))
            {
                strErrorMsg = strErrorMsg + getErrorString("申报单位代码 必填并且必须是10位", "") ;
            }
            else if (e.Exception.Message.Contains("申报单位名称"))
            {
                strErrorMsg = strErrorMsg + getErrorString("申报单位代码 必填并且必须少于60个字符", "");
            }
            else if (e.Exception.Message.Contains("归类标志"))
            {
                strErrorMsg = strErrorMsg + getErrorString("归类标志 不能大于一个字符", "");
            }
            else
            {
                strErrorMsg = strErrorMsg + e.Exception.Message ;
            }
        }

        string getErrorString(string erroString, string errorCode)
        {
            StringBuilder errXMl = new StringBuilder();
            errXMl.Append("<ErrorString>ERROR_STRING</ErrorString>");
            errXMl.Replace("ERROR_STRING", erroString);
            //errXMl.Replace("ERROR_CODE", errorCode);
            return errXMl.ToString();
        }
    }
}