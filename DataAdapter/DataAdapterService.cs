using System;
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

namespace FeiliksDataAdapter
{
    [System.ServiceProcess.ServiceProcessDescription("Feiliks Data Adapter Service")]
    partial class DataAdapterService : ServiceBase
    {
        System.Data.DataSet settings = new DataSet();
        private int timer = 30;
        //private bool tcpStoped;
        string ConnectionType = "";
        string ConnectionString = "";

        //定时器  
        System.Timers.Timer t = null;  
        public DataAdapterService()
        {
            InitializeComponent();
            //启用暂停恢复  
            base.CanPauseAndContinue = true; 
        }

        protected override void OnStart(string[] args)
        {
            string state = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "启动";
            WriteLog(state);
            //ESettings.xml,加密后的配置文件
            //Settings.xml，原始配置文件，此文件在生成了ESettings.xml后可以删除
            #region 获取设置
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
                    return;
                }
                #region 配置文件必填
                if (settings.Tables["App"].Columns.Contains("ConnectionType") == false || settings.Tables["App"].Rows[0]["ConnectionType"].ToString().Trim() == "")
                {
                    this.WriteLog("未配置数据库类型");
                    return;
                }
                if (settings.Tables["App"].Columns.Contains("ConnectionString") == false || settings.Tables["App"].Rows[0]["ConnectionString"].ToString().Trim() == "")
                {
                    this.WriteLog("未配置数据库连接");
                    return;
                }
                #endregion

                ConnectionType = settings.Tables["App"].Rows[0]["ConnectionType"].ToString().Trim();
                ConnectionString = settings.Tables["App"].Rows[0]["ConnectionString"].ToString().Trim();

                int intTemp;
                if (settings.Tables["App"].Rows[0]["Timer"] != null && settings.Tables["App"].Rows[0]["Timer"].ToString().Trim() != ""
                    && int.TryParse(settings.Tables["App"].Rows[0]["Timer"].ToString().Trim(), out intTemp) && intTemp > 0)
                {
                    timer = intTemp;
                }
            }
            catch (System.Exception error)
            {
                this.WriteLog(error.Message);
                return;
            }
            #endregion

            //每5秒执行一次  
            t = new System.Timers.Timer(timer * 1000);
            //设置是执行一次（false）还是一直执行(true)；  
            t.AutoReset = true;
            //是否执行System.Timers.Timer.Elapsed事件；  
            t.Enabled = true;
            //到达时间的时候执行事件(theout方法)；  
            t.Elapsed += new System.Timers.ElapsedEventHandler(DoService);  
        }

        protected override void OnStop()
        {
            string state = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "停止";
            WriteLog(state);  
            // TODO: Add code here to perform any tear-down necessary to stop your service.
        }
        //恢复服务执行  
        protected override void OnContinue()
        {
            string state = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "继续";
            WriteLog(state);
            t.Start();
        }

        //暂停服务执行  
        protected override void OnPause()
        {
            string state = DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "暂停";
            WriteLog(state);

            t.Stop();
        }  

        #region  服务所用方法

        public void DoService(object source, System.Timers.ElapsedEventArgs e)
        {
            //WriteLog("theout:" + DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss"));
            IDataAccess DAC = GetDataAccess();
            if (DAC == null)
            {
                this.WriteLog("数据库配置有误");
                return;
            }
            if (DAC.ErrMessages != "")
            {
                this.WriteLog(DAC.ErrMessages);
                return;
            }

            string strOriginalFileName = "";
            string strMonthDate = DateTime.Now.ToString("yyyyMM");
            //1. 从MQ、FTP等获取文件到本地文件夹。接收成功回执(是否发送回执,并且回执文件夹不为空，并且流程编号与报文中一致不为空)
            //2. 接收的文件校验。校验结果回执
            //3. XSLT报文格式转换。转换结果回执
            //4. MQ发送。如果是报关单，需要记录随附单证
            //回执部分：MQ接收原始回执，XSLT文件格式转换，MQ、FTP发送
            #region 1. 从MQ、FTP等获取文件到本地文件夹。
            //A. 全部接收到保存文件夹
            //B. 记录接收历史记录
            //C. 是否发送回执,并且回执文件夹不为空，并且流程编号不为空.生成回执文件
            DataTable dtConfigList = DAC.GetTable("SELECT * FROM C2K.FDA_ReceiveConfig WHERE isnull(IsEnabled,'0')='1' ");
            if (dtConfigList != null && dtConfigList.Rows.Count > 0)
            {
                string backupFolder = "";
                foreach (DataRow drConfig in dtConfigList.Rows)
                {
                    #region 数据库配置检验
                    if (drConfig["TransType"].ToString().Trim() == "")
                    {
                        WriteLog("报文接收的传输方式不能为空。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_ReceiveConfig", "报文接收的传输方式不能为空");
                        continue;
                    }
                    if (drConfig["TransType"].ToString().Trim().ToLower() != "mq" && drConfig["TransType"].ToString().Trim().ToLower() != "ftp"
                         && drConfig["TransType"].ToString().Trim().ToLower() != "folder")
                    {
                        WriteLog("报文接收的传输方式只能是：MQ、FTP、文件夹 其中之一。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_ReceiveConfig", "报文接收的传输方式只能是：MQ、FTP、文件夹 其中之一");
                        continue;
                    }
                    if (drConfig["URL"].ToString().Trim() == "")
                    {
                        WriteLog("报文接收的URL不能为空(MQ、FTP或文件夹路径)。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_ReceiveConfig", "报文接收的URL不能为空(MQ、FTP或文件夹路径)");
                        continue;
                    }

                    if (drConfig["TransType"].ToString().Trim().ToLower() == "ftp" && (drConfig["UserName"].ToString().Trim() == "" || drConfig["Password"].ToString().Trim() == ""))
                    {
                        WriteLog("报文接收的传输方式是:FTP时，用户名和密码不能为空。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_ReceiveConfig", "报文接收的传输方式是:FTP时，用户名和密码不能为空");
                        continue;
                    }

                    if (drConfig["TransType"].ToString().Trim().ToLower() == "folder" && Directory.Exists(drConfig["URL"].ToString().Trim()) == false)
                    {
                        WriteLog("报文接收的传输方式是:文件夹时，所配置的原始文件夹不存在。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_ReceiveConfig", "报文接收的传输方式是:文件夹时，所配置的原始文件夹不存在");
                        continue;
                    }
                    if (drConfig["SaveFolder"].ToString().Trim() == "")
                    {
                        WriteLog("报文接收的保存路径不能为空。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_ReceiveConfig", "报文接收的保存路径不能为空");
                        continue;
                    }
                    if (Directory.Exists(drConfig["SaveFolder"].ToString().Trim()) == false)
                    {
                        WriteLog("报文接收的保存路径不存在。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_ReceiveConfig", "报文接收的保存路径不存在");
                        continue;
                    }
                    backupFolder = drConfig["SaveFolder"].ToString().Trim() + "\\Backup\\" + strMonthDate;
                    if (!Directory.Exists(backupFolder))
                    {
                        Directory.CreateDirectory(backupFolder);
                    }
                    #endregion

                    if (drConfig["TransType"].ToString().Trim().ToLower() == "mq")
                    {
                        #region MQ
                        try
                        {
                            MessageQueue receiveQueue = new MessageQueue(drConfig["URL"].ToString().Trim());
                            receiveQueue.Formatter = new XmlMessageFormatter(new Type[] { typeof(XmlDocument) });
                            MessageEnumerator myEnumerator = receiveQueue.GetMessageEnumerator2();
                            while (myEnumerator.MoveNext())
                            {
                                System.Messaging.Message msg = myEnumerator.Current;
                                try
                                {
                                    if (msg != null && msg.Body != null)
                                    {
                                        XmlDocument docReply = (XmlDocument)msg.Body;
                                        strOriginalFileName = msg.Label.Trim() + ".xml";
                                        if (File.Exists(backupFolder + "\\" + strOriginalFileName))
                                        {
                                            File.Delete(backupFolder + "\\" + strOriginalFileName);
                                        }
                                        docReply.Save(backupFolder + "\\" + strOriginalFileName);
                                        File.Copy(backupFolder + "\\" + strOriginalFileName, drConfig["SaveFolder"].ToString().Trim() + "\\" + strOriginalFileName, true);
                                        WriteLog("MQ报文接收成功." + msg.Label.Trim());

                                        //记录接收历史记录
                                        SaveDataAdapterHistory(DAC, drConfig["SaveFolder"].ToString().Trim(), backupFolder + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Success, "MQ报文接收成功", ConfigType.Receive, strOriginalFileName);

                                        #region 生成回执到回执文件夹
                                        if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                                        {
                                            string[] splitFileName = msg.Label.Trim().Split(new char[] { '$' });
                                            if (splitFileName != null && splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                            {
                                                CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中报文接收成功", splitFileName[1].Trim(), "交换中报文接收成功");
                                            }
                                        }
                                        #endregion
                                    }
                                }
                                catch (Exception ex)
                                {
                                    this.WriteLog("获取MQ报文异常ex:" + ex.Message + ex.StackTrace);
                                }
                                myEnumerator.RemoveCurrent();
                                myEnumerator = receiveQueue.GetMessageEnumerator2();
                            }
                        }
                        catch (Exception ex)
                        {
                            this.WriteLog("获取MQ报文异常ex:" + ex.Message + ex.StackTrace);
                            SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_ReceiveConfig", "获取MQ报文异常ex:" + ex.Message + ex.StackTrace);
                        }
                        #endregion
                    }
                    else if (drConfig["TransType"].ToString().Trim().ToLower() == "ftp")
                    {
                        #region FTP
                        try
                        {
                            FtpWeb ftp = new FtpWeb(drConfig["URL"].ToString().Trim(), "", drConfig["UserName"].ToString().Trim(), drConfig["Password"].ToString().Trim());
                            string[] fileList = ftp.GetFileList("");
                            if (fileList != null && fileList.Length > 0)
                            {
                                //下载文件,删除FTP文件
                                FileInfo fileInfo;
                                foreach (string fileName in fileList)
                                {
                                    fileInfo = new FileInfo(fileName);
                                    if ((DateTime.Now - fileInfo.LastWriteTime).TotalSeconds <= 10)
                                    {
                                        continue;
                                    }
                                    ftp.Download(backupFolder, fileName);
                                    if (System.IO.File.Exists(backupFolder + "\\" + fileName))
                                    {
                                        ftp.Delete(fileName);
                                    }
                                    File.Copy(backupFolder + "\\" + fileName, drConfig["SaveFolder"].ToString().Trim() + "\\" + fileName);
                                    WriteLog("MQ报文接收成功." + fileName);

                                    //记录接收历史记录
                                    SaveDataAdapterHistory(DAC, drConfig["SaveFolder"].ToString().Trim(), backupFolder + "\\" + fileName, drConfig["Indx"].ToString().Trim(), StatusCode.Success, "FTP报文接收成功", ConfigType.Receive, fileName);

                                    #region 生成回执到回执文件夹
                                    if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                                    {
                                        string[] splitFileName = fileName.Trim().Split(new char[] { '$' });
                                        if (splitFileName != null && splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                        {
                                            CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中报文接收成功", splitFileName[1].Trim(), "交换中报文接收成功");
                                        }
                                    }
                                    #endregion
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            this.WriteLog("获取FTP报文异常ex:" + ex.Message + ex.StackTrace);
                            SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_ReceiveConfig", "获取FTP报文异常ex:" + ex.Message + ex.StackTrace);
                        }
                        #endregion
                    }
                    else if (drConfig["TransType"].ToString().Trim().ToLower() == "folder")
                    {
                        #region 文件夹
                        try
                        {
                            FileInfo fileInfo;
                            foreach (string file in Directory.GetFiles(drConfig["URL"].ToString().Trim()))
                            {
                                fileInfo = new FileInfo(file);
                                if ((DateTime.Now - fileInfo.LastWriteTime).TotalSeconds <= 10)
                                {
                                    continue;
                                }
                                string[] splitFileName = fileInfo.Name.Split(new char[] { '$' });
                                if (splitFileName == null || splitFileName.Length == 0 || splitFileName[0].Trim() == "" || splitFileName[0].Trim() != drConfig["TaskCode"].ToString().Trim())
                                {
                                    continue;
                                }
                                strOriginalFileName = Path.GetFileName(file);
                                File.Move(file, backupFolder + "\\" + strOriginalFileName);
                                File.Copy(backupFolder + "\\" + strOriginalFileName, drConfig["SaveFolder"].ToString().Trim() + "\\" + strOriginalFileName);
                                WriteLog("文件夹报文接收成功." + file);

                                //记录接收历史记录
                                SaveDataAdapterHistory(DAC, drConfig["SaveFolder"].ToString().Trim(), backupFolder + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Success, "文件夹报文接收成功", ConfigType.Receive, strOriginalFileName);

                                #region 生成回执到回执文件夹
                                if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                                {
                                    if (splitFileName != null && splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                    {
                                        CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中报文接收成功", splitFileName[1].Trim(), "交换中报文接收成功");
                                    }
                                }
                                #endregion
                            }
                        }
                        catch (Exception ex)
                        {
                            this.WriteLog("获取文件夹报文异常ex:" + ex.Message + ex.StackTrace);
                            SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_ReceiveConfig", "获取文件夹报文异常ex:" + ex.Message + ex.StackTrace);
                        }
                        #endregion
                    }
                }
            }
            #endregion
            #region 2. 接收的文件校验。校验结果回执
            dtConfigList = DAC.GetTable("SELECT * FROM C2K.FDA_CheckConfig WHERE isnull(IsEnabled,'0')='1' ");
            if (dtConfigList != null && dtConfigList.Rows.Count > 0)
            {
                string backupFolder, failedFolder;
                foreach (DataRow drConfig in dtConfigList.Rows)
                {
                    #region 数据库配置检验
                    if (drConfig["CheckFolder"].ToString().Trim() == "")
                    {
                        WriteLog("报文校验的校验路径不能为空。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_CheckConfig", "报文校验的校验路径不能为空");
                        continue;
                    }
                    if (Directory.Exists(drConfig["CheckFolder"].ToString().Trim()) == false)
                    {
                        WriteLog("报文校验的校验路径不存在。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_CheckConfig", "报文校验的校验路径不存在");
                        continue;
                    }
                    if (drConfig["SendFolder"].ToString().Trim() == "")
                    {
                        WriteLog("报文校验的校验成功路径不能为空。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_CheckConfig", "报文校验的校验成功路径不能为空");
                        continue;
                    }
                    if (Directory.Exists(drConfig["SendFolder"].ToString().Trim()) == false)
                    {
                        WriteLog("报文校验的校验成功路径不存在。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_CheckConfig", "报文校验的成功路径不存在");
                        continue;
                    }
                    if (drConfig["BakFolder"].ToString().Trim() == "")
                    {
                        WriteLog("报文校验的备份路径不能为空。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_CheckConfig", "报文校验的备份路径不能为空");
                        continue;
                    }
                    if (Directory.Exists(drConfig["BakFolder"].ToString().Trim()) == false)
                    {
                        WriteLog("报文校验的备份路径不存在。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_CheckConfig", "报文校验的备份路径不存在");
                        continue;
                    }
                    if (drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) == false)
                    {
                        WriteLog("报文校验的回执路径不存在。Indx：" + drConfig["Indx"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_CheckConfig", "报文校验的回执路径不存在");
                        continue;
                    }
                    backupFolder = drConfig["BakFolder"].ToString().Trim() + "\\" + strMonthDate;
                    if (!Directory.Exists(backupFolder))
                    {
                        Directory.CreateDirectory(backupFolder);
                    }
                    failedFolder = drConfig["BakFolder"].ToString().Trim() + "\\Failed";
                    if (!Directory.Exists(failedFolder))
                    {
                        Directory.CreateDirectory(failedFolder);
                    }

                    #endregion
                    FileInfo fileInfo; string fileName, TaskCode, tempFileName;
                    foreach (string fileFullName in Directory.GetFiles(drConfig["CheckFolder"].ToString().Trim()))
                    {
                        fileInfo = new FileInfo(fileFullName);
                        if ((DateTime.Now - fileInfo.LastWriteTime).TotalSeconds <= 10)//最后修改时间10秒内的文件不处理，防止文件未写完
                        {
                            continue;
                        }
                        string[] splitFileName = fileInfo.Name.Split(new char[] { '$' });
                        if (splitFileName == null || splitFileName.Length == 0 || splitFileName[0].Trim() == "" || splitFileName[0].Trim() != drConfig["TaskCode"].ToString().Trim())
                        {
                            continue;
                        }
                        strOriginalFileName = fileInfo.Name;
                        if (fileInfo.Length <= 0)
                        {
                            MoveFile(fileFullName, failedFolder);
                            WriteLog("报文无内容." + failedFolder + "\\" + strOriginalFileName);

                            //记录接收历史记录
                            SaveDataAdapterHistory(DAC, drConfig["SendFolder"].ToString().Trim(), failedFolder + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Falied, "报文校验失败：报文无内容", ConfigType.Check, strOriginalFileName);
                            if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                            {
                                if (splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                {
                                    CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中心报文校验失败：报文无内容", splitFileName[1].Trim(), "交换中心报文校验失败");
                                }
                            }
                            continue;
                        }
                        TaskCode = splitFileName[0].Trim();
                        XmlDocument xmlDoc = new XmlDocument();
                        xmlDoc.Load(fileFullName);
                        XmlElement root = xmlDoc.DocumentElement;
                        if (drConfig["CheckType"].ToString().Trim() == "V20")
                        {
                            //2.0海关报文校验
                        }
                        else if (drConfig["CheckType"].ToString().Trim() == "V30")
                        {
                            //3.0海关报文校验
                        }
                    }
                }
            }
            #endregion
            #region 3. XSLT报文格式转换。转换结果回执
            //为空时直接处理文件夹 20160317 del
            //dtConfigList = DAC.GetTable("SELECT * FROM C2K.FDA_TransformConfig WHERE isnull(IsEnabled,'0')='1' AND isnull(TaskCode,'')<> ''");
            dtConfigList = DAC.GetTable("SELECT * FROM C2K.FDA_TransformConfig WHERE isnull(IsEnabled,'0')='1'");
            if (dtConfigList != null && dtConfigList.Rows.Count > 0)
            {
                string backupFolder, faliedFolder,successFolder;
                foreach (DataRow drConfig in dtConfigList.Rows)
                {
                    #region  数据库配置检验
                    if (drConfig["XSLTFile"].ToString().Trim() == "")
                    {
                        WriteLog("报文转换的转换模板(XSLT)不能为空。流程编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_TransformConfig", "报文转换的转换模板(XSLT)不能为空");
                        continue;
                    }
                    if (File.Exists(drConfig["XSLTFile"].ToString().Trim()) == false)
                    {
                        WriteLog("报文转换的转换模板(XSLT) 不存在。流程编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_TransformConfig", "报文转换的转换模板(XSLT) 不存在");
                        continue;
                    }
                    if (drConfig["TransformFolder"].ToString().Trim() == "")
                    {
                        WriteLog("报文转换的待转换路径不能为空。流程编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_TransformConfig", "报文转换的待转换路径不能为空");
                        continue;
                    }
                    if (Directory.Exists(drConfig["TransformFolder"].ToString().Trim()) == false)
                    {
                        WriteLog("报文转换的待转换路径不存在。流程编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_TransformConfig", "报文转换的待转换路径不存在");
                        continue;
                    }
                    if (drConfig["TransformSuccessFolder"].ToString().Trim() == "")
                    {
                        WriteLog("报文转换的转换成功路径不能为空。流程编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_TransformConfig", "报文转换的转换成功路径不能为空");
                        continue;
                    }
                    if (Directory.Exists(drConfig["TransformSuccessFolder"].ToString().Trim()) == false)
                    {
                        WriteLog("报文转换的转换成功路径不存在。流程编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_TransformConfig", "报文转换的转换成功路径不存在");
                        continue;
                    }
                    if (drConfig["TransformBackupFolder"].ToString().Trim() == "")
                    {
                        WriteLog("报文转换的转换备份路径不能为空。流程编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_TransformConfig", "报文转换的转换备份路径不能为空");
                        continue;
                    }
                    if (Directory.Exists(drConfig["TransformBackupFolder"].ToString().Trim()) == false)
                    {
                        WriteLog("报文转换的转换备份路径不存在。流程编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_TransformConfig", "报文转换的转换备份路径不存在");
                        continue;
                    }
                    #endregion
                    backupFolder = drConfig["TransformBackupFolder"].ToString().Trim() + "\\BeforeTransform\\" + strMonthDate;
                    if (!Directory.Exists(backupFolder))
                    {
                        Directory.CreateDirectory(backupFolder);
                    }
                    faliedFolder = drConfig["TransformBackupFolder"].ToString().Trim() + "\\Failed";
                    if (!Directory.Exists(faliedFolder))
                    {
                        Directory.CreateDirectory(faliedFolder);
                    }
                    successFolder = drConfig["TransformBackupFolder"].ToString().Trim() + "\\Success\\" + strMonthDate;
                    if (!Directory.Exists(successFolder))
                    {
                        Directory.CreateDirectory(successFolder);
                    }

                    FileInfo fileInfo; string TaskCode;
                    foreach (string fileFullName in Directory.GetFiles(drConfig["TransformFolder"].ToString().Trim()))
                    {
                        fileInfo = new FileInfo(fileFullName);
                        if ((DateTime.Now - fileInfo.LastWriteTime).TotalSeconds <= 10)//最后修改时间10秒内的文件不处理，防止文件未写完
                        {
                            continue;
                        }
                        strOriginalFileName = fileInfo.Name;
                        if (fileInfo.Length <= 0)
                        {
                            continue;
                        }
                        string[] splitFileName = strOriginalFileName.Split(new char[] { '$' });
                        if (splitFileName == null || splitFileName.Length == 0)
                        {
                            continue;
                        }
                        TaskCode = splitFileName[0];
                        //为空时直接处理文件夹 20160317 del
                        //if (TaskCode == "")
                        //{
                        //    continue;
                        //}
                        //if (TaskCode != drConfig["TaskCode"].ToString().Trim())
                        if (TaskCode != drConfig["TaskCode"].ToString().Trim() && drConfig["TaskCode"].ToString().Trim() != "")
                        {
                            continue;
                        }
                        try
                        {
                            File.Copy(fileFullName, backupFolder + "\\" + strOriginalFileName, true);
                            File.Delete(fileFullName);

                            System.Xml.Xsl.XslCompiledTransform transform = new XslCompiledTransform();
                            transform.Load(drConfig["XSLTFile"].ToString().Trim());
                            transform.Transform(backupFolder + "\\" + strOriginalFileName, successFolder + "\\" + strOriginalFileName);
                            File.Copy(successFolder + "\\" + strOriginalFileName, drConfig["TransformSuccessFolder"].ToString().Trim() + "\\" + strOriginalFileName);

                            //记录接收历史记录
                            SaveDataAdapterHistory(DAC, drConfig["TransformSuccessFolder"].ToString().Trim(), backupFolder + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Success, "XSLT报文转换成功", ConfigType.Transform, strOriginalFileName);

                            #region 生成回执到回执文件夹
                            if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                            {
                                if (splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                {
                                    CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中心报文转换成功", splitFileName[1].Trim(), "交换中心报文转换成功");
                                }
                            }
                            #endregion
                        }
                        catch (Exception ex2)
                        {
                            //此处可以生成转换错误的回执文件，暂无
                            MoveFile(fileFullName, faliedFolder);
                            this.WriteLog("报文转换异常：" + ex2.Message);
                            //记录接收历史记录
                            SaveDataAdapterHistory(DAC, drConfig["TransformSuccessFolder"].ToString().Trim(), faliedFolder + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Falied, "XSLT报文转换失败:" + ex2.Message, ConfigType.Transform, strOriginalFileName);

                            #region 生成回执到回执文件夹
                            if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                            {
                                if (splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                {
                                    CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中心报文转换失败:" + ex2.Message, splitFileName[1].Trim(), "交换中心报文转换失败");
                                }
                            }
                            #endregion
                            continue;
                        }
                    }
                }
            }
            #endregion
            #region 4. MQ/FTP发送。如果是报关单，需要记录随附单证
            dtConfigList = DAC.GetTable("SELECT * FROM C2K.FDA_SendConfig WHERE isnull(IsEnabled,'0')='1' AND isnull(TaskCode,'')<> ''");
            if (dtConfigList != null && dtConfigList.Rows.Count > 0)
            {
                string BackupPath, ErrorPath;
                foreach (DataRow drConfig in dtConfigList.Rows)
                {
                    #region  数据库配置检验
                    if (drConfig["TransType"].ToString().Trim() == "")
                    {
                        WriteLog("报文发送的传输方式不能为空。任务编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_SendConfig", "报文发送的传输方式不能为空");
                        continue;
                    }
                    if (drConfig["TransType"].ToString().Trim().ToLower() != "mq" && drConfig["TransType"].ToString().Trim().ToLower() != "ftp")
                    {
                        WriteLog("报文发送的传输方式只能是MQ或FTP。任务编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_SendConfig", "报文发送的传输方式只能是MQ或FTP");
                        continue;
                    }
                    if (drConfig["URL"].ToString().Trim() == "")
                    {
                        WriteLog("报文发送的URL不能为空。任务编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_SendConfig", "报文发送的URL不能为空");
                        continue;
                    }

                    if (drConfig["SendFolder"].ToString().Trim() == "")
                    {
                        WriteLog("报文发送的待发送路径不能为空。任务编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_SendConfig", "报文发送的待发送路径不能为空");
                        continue;
                    }
                    if (Directory.Exists(drConfig["SendFolder"].ToString().Trim()) == false)
                    {
                        WriteLog("报文发送的待发送路径不存在。任务编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_SendConfig", "报文发送的待发送路径不存在");
                        continue;
                    }

                    if (drConfig["BakFolder"].ToString().Trim() == "")
                    {
                        WriteLog("报文发送的备份路径不能为空。任务编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_SendConfig", "报文发送的备份路径不能为空");
                        continue;
                    }
                    if (Directory.Exists(drConfig["BakFolder"].ToString().Trim()) == false)
                    {
                        WriteLog("报文发送的备份路径不存在。任务编号：" + drConfig["TaskCode"].ToString().Trim().ToLower());
                        SetConfigDisabled(DAC, drConfig["Indx"].ToString().Trim(), "FDA_SendConfig", "报文发送的备份路径不存在");
                        continue;
                    }
                    #endregion

                    BackupPath = drConfig["BakFolder"].ToString().Trim() + "\\" + strMonthDate;
                    if (!Directory.Exists(BackupPath))
                    {
                        Directory.CreateDirectory(BackupPath);
                    }
                    ErrorPath = drConfig["BakFolder"].ToString().Trim() + "\\Failed";
                    if (!Directory.Exists(ErrorPath))
                    {
                        Directory.CreateDirectory(ErrorPath);
                    }
                    FileInfo fileInfo; string TaskCode, tempFileName;
                    int intTemp1 = 0, intTemp2 = 0;
                    foreach (string fileFullName in Directory.GetFiles(drConfig["SendFolder"].ToString().Trim()))
                    {
                        fileInfo = new FileInfo(fileFullName);
                        if ((DateTime.Now - fileInfo.LastWriteTime).TotalSeconds <= 10)//最后修改时间10秒内的文件不处理，防止文件未写完
                        {
                            continue;
                        }
                        strOriginalFileName = fileInfo.Name;
                        string[] splitFileName = strOriginalFileName.Split(new char[] { '$' });
                        if (splitFileName == null || splitFileName.Length == 0)
                        {
                            continue;
                        }
                        TaskCode = splitFileName[0];
                        if (TaskCode == "")
                        {
                            continue;
                        }
                        if (TaskCode != drConfig["TaskCode"].ToString().Trim())
                        {
                            continue;
                        }
                        tempFileName = BackupPath + "\\" + strOriginalFileName;
                        File.Copy(fileFullName, tempFileName, true);
                        File.Delete(fileFullName);

                        #region 文件无内容
                        if (fileInfo.Length <= 0)
                        {
                            MoveFile(tempFileName, ErrorPath);
                            WriteLog("报文无内容." + strOriginalFileName);
                            //记录接收历史记录
                            SaveDataAdapterHistory(DAC, drConfig["SendFolder"].ToString().Trim(), ErrorPath + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Falied, "报文发送失败:报文无内容", ConfigType.Send, strOriginalFileName);

                            #region 生成回执到回执文件夹
                            if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                            {
                                if (splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                {
                                    CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中心报文发送失败:报文无内容", splitFileName[1].Trim(), "交换中心报文发送失败");
                                }
                            }
                            #endregion
                            continue;
                        }
                        #endregion
                        #region 报文发送次数已达最大限制
                        if (drConfig["SendCountLimit"].ToString().Trim() != "" && int.TryParse(drConfig["SendCountLimit"].ToString().Trim(), out intTemp1) && intTemp1 > 0)
                        {
                            DataTable SendLimit = DAC.GetTable("SELECT Indx FROM C2K.FDA_DataAdapterHistory WHERE Status='" + ((uint)StatusCode.Success).ToString() + "' AND ConfigType='" + ((uint)ConfigType.Send).ToString() + "' AND OriginalFileName='" + strOriginalFileName + "'");
                            if (SendLimit != null && SendLimit.Rows.Count > 0 && SendLimit.Rows.Count > intTemp1)
                            {
                                MoveFile(tempFileName, ErrorPath);
                                WriteLog("报文发送次数已达最大限制.流程编号（" + TaskCode + "）发送次数限制为（" + drConfig["SendCountLimit"].ToString().Trim() + "）.报文：" + strOriginalFileName);
                                //记录接收历史记录
                                SaveDataAdapterHistory(DAC, drConfig["SendFolder"].ToString().Trim(), ErrorPath + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Falied, "报文发送失败:报文发送次数已达最大限制", ConfigType.Send, strOriginalFileName);

                                #region 生成回执到回执文件夹
                                if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                                {
                                    if (splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                    {
                                        CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中心报文发送失败:报文发送次数已达最大限制", splitFileName[1].Trim(), "交换中心报文发送失败");
                                    }
                                }
                                #endregion
                            }
                        }
                        #endregion

                        if (drConfig["TransType"].ToString().Trim().ToLower() == "mq")
                        {
                            #region MQ 发送
                            try
                            {
                                MessageQueue sendQueue = new MessageQueue(drConfig["URL"].ToString().Trim());
                                sendQueue.Formatter = new System.Messaging.XmlMessageFormatter(new Type[] { typeof(XmlDocument) });

                                XmlDocument docReply = new XmlDocument();
                                docReply.Load(tempFileName);

                                System.Messaging.Message msg = new System.Messaging.Message();
                                msg.Body = docReply;
                                msg.Label = Path.GetFileNameWithoutExtension(tempFileName);
                                sendQueue.Send(msg, MessageQueueTransactionType.Single);

                                //MQ发送成功，记录历史和回执
                                SaveDataAdapterHistory(DAC, drConfig["SendFolder"].ToString().Trim(), BackupPath + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Success, "MQ报文发送成功", ConfigType.Send, strOriginalFileName);

                                #region 生成回执到回执文件夹
                                if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                                {
                                    if (splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                    {
                                        CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中心报文发送成功", splitFileName[1].Trim(), "交换中心报文发送成功");
                                    }
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.WriteLog("发送MQ异常:" + ex.Message + ex.StackTrace);
                                MoveFile(tempFileName, ErrorPath);
                                //记录接收历史记录
                                SaveDataAdapterHistory(DAC, drConfig["SendFolder"].ToString().Trim(), ErrorPath + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Falied, "MQ报文发送失败:" + ex.Message, ConfigType.Send, strOriginalFileName);

                                #region 生成回执到回执文件夹
                                if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                                {
                                    if (splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                    {
                                        CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中心报文发送失败:" + ex.Message, splitFileName[1].Trim(), "交换中心报文发送失败");
                                    }
                                }
                                #endregion
                            }
                            #endregion
                        }
                        else if (drConfig["TransType"].ToString().Trim().ToLower() == "ftp")
                        {
                            #region FTP
                            try
                            {
                                FtpWeb ftp = new FtpWeb(drConfig["URL"].ToString().Trim(), "", drConfig["UserName"].ToString().Trim(), drConfig["Password"].ToString().Trim());
                                ftp.Upload(tempFileName);

                                //FTP发送成功，记录历史和回执
                                SaveDataAdapterHistory(DAC, drConfig["SendFolder"].ToString().Trim(), BackupPath + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Success, "FTP报文发送成功", ConfigType.Send, strOriginalFileName);

                                #region 生成回执到回执文件夹
                                if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                                {
                                    if (splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                    {
                                        CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中心报文发送成功", splitFileName[1].Trim(), "交换中心报文发送成功");
                                    }
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                this.WriteLog("发送FTP异常:" + ex.Message + ex.StackTrace);
                                MoveFile(tempFileName, ErrorPath);
                                //记录接收历史记录
                                SaveDataAdapterHistory(DAC, drConfig["SendFolder"].ToString().Trim(), ErrorPath + "\\" + strOriginalFileName, drConfig["Indx"].ToString().Trim(), StatusCode.Falied, "FTP报文发送失败:" + ex.Message, ConfigType.Send, strOriginalFileName);

                                #region 生成回执到回执文件夹
                                if (drConfig["IsSendReceipt"].ToString().Trim() == "1" && drConfig["ReceiptFolder"].ToString().Trim() != "" && Directory.Exists(drConfig["ReceiptFolder"].ToString().Trim()) && drConfig["TaskCode"].ToString().Trim() != "")
                                {
                                    if (splitFileName.Length >= 2 && splitFileName[0].Trim() != "" && splitFileName[0].Trim() == drConfig["TaskCode"].ToString().Trim() && splitFileName[1].Trim() != "")
                                    {
                                        CreateReceiptFile(drConfig["ReceiptFolder"].ToString().Trim() + "\\" + splitFileName[0].Trim() + "$" + splitFileName[1].Trim() + "$Receipt$" + DateTime.Now.ToString("yyyyMMddHHmmssfffffff") + ".xml", "交换中心报文发送失败:" + ex.Message, splitFileName[1].Trim(), "交换中心报文发送失败");
                                    }
                                }
                                #endregion
                            }
                            #endregion
                        }
                    }
                }
            }
            #endregion
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
        private void SetConfigDisabled(IDataAccess DAC, string indx, string tableName, string message)
        {
            DAC.ExecuteSql("UPDATE " + tableName + " SET IsEnabled='0',IsEnabledCN='否',Remark='" + message + ";'+ISNULL(Remark,'') WHERE indx=" + indx);
        }
        enum StatusCode
        {
            Success = 1,
            Falied = 0
        }
        enum ConfigType
        {
            Receive = 1,
            Check = 2,
            Transform = 3,
            Send = 4
        }
        private void SaveDataAdapterHistory(IDataAccess DAC, string SaveFolder, string FileBackupPath, string ReceiveConfigIndx,
            StatusCode status, string msg, ConfigType configType, string OriginalFileName)
        {
            DataTable dtTemp = DAC.GetTable("SELECT * FROM C2K.FDA_DataAdapterHistory WHERE 1=2");
            DataRow drTemp = dtTemp.NewRow();
            drTemp["MakeTime"] = DateTime.Now.ToString();
            drTemp["SaveFolder"] = SaveFolder;
            drTemp["FileBackupPath"] = FileBackupPath;
            drTemp["ReceiveConfigIndx"] = ReceiveConfigIndx;
            drTemp["Status"] = (uint)status;
            drTemp["Msg"] = msg;
            drTemp["ConfigType"] = (uint)configType;
            drTemp["OriginalFileName"] = OriginalFileName;
            DAC.Insert("FDA_DataAdapterHistory", drTemp);
        }
        private void CreateReceiptFile(string fileName, string message, string refCode, string status)
        {
            XmlTextWriter xtw = new XmlTextWriter(fileName, Encoding.UTF8);

            xtw.Formatting = System.Xml.Formatting.Indented;
            xtw.IndentChar = '\t';
            xtw.WriteStartDocument();

            xtw.WriteStartElement("回执信息");
            WriteElement(xtw, "企业编号", refCode);//相关号
            WriteElement(xtw, "文档编号", "");
            WriteElement(xtw, "统一编号", "");//统一编号
            WriteElement(xtw, "海关编号", "");//海关编号
            WriteElement(xtw, "收到回执日期", DateTime.Now.ToString());//
            WriteElement(xtw, "回执状态", status);//回执编码
            WriteElement(xtw, "回执内容", message);//回执文字信息
            WriteElement(xtw, "主管海关", "");
            WriteElement(xtw, "代理企业", "");
            WriteElement(xtw, "报关员卡号", "");
            WriteElement(xtw, "经营单位代码", "");
            WriteElement(xtw, "申报日期", "");
            WriteElement(xtw, "件数", "");
            WriteElement(xtw, "净重", "");
            WriteElement(xtw, "毛重", "");
            WriteElement(xtw, "提运单号", "");
            WriteElement(xtw, "运输方式", "");
            WriteElement(xtw, "运输工具名称", "");
            WriteElement(xtw, "放行回执日期", "");
            xtw.WriteEndElement();

            xtw.WriteEndDocument();
            xtw.Close();
        }
        private void WriteElement(XmlTextWriter xtw, string el, string v)
        {
            xtw.WriteStartElement(el);
            if (v == "")
            {
                //xtw.WriteAttributeString("xsi:nil", "true");
            }
            else
            {
                xtw.WriteString(v);
            }
            xtw.WriteEndElement();
        }
        #endregion
    }
}
