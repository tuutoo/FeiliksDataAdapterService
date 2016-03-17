using System;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Data;

namespace Test
{
    public enum LogType
    { File, SQL, System ,None}

    public class SysLog
    {
        private LogType LogsType;
        private IDataAccess _DataAccess;

        public IDataAccess DataAccess
        {
            get { return _DataAccess; }
            set { 
                _DataAccess = value.Clone();
                _DataAccess.WriteLogType = LogType.None;
            }
        }
        private string _ErrMessages;
        private bool writing = false;

        public string ErrMessages
        {
            get { return _ErrMessages; }
            set { _ErrMessages = value; }
        }

        public SysLog( LogType logtype)
        {
            LogsType = logtype;
        }


        #region 公共方法写日至
        public bool WriteLog(string Message)
        {
            bool rt = false;
            while (writing)
            {
                System.Threading.Thread.Sleep(100);
            }
            writing = true;
            switch (LogsType)
            {
                case LogType.File:
                    rt = WriteLogFile("0", Message);
                    break;
                case LogType.SQL :
                    rt = WriteLogSQL("0", Message);
                    break;
                case LogType.System :
                    rt = WriteLogSystem("0", Message);
                    break;
            }
            writing = false;
            return rt;
        }

        public bool WriteLog(string User, string Message)
        {
            bool rt = false ;
            while (writing)
            {
                System.Threading.Thread.Sleep(100);
            }
            writing = true;
            switch (LogsType)
            {
                case LogType.File:
                    rt = WriteLogFile(User, Message);
                    break;
                case LogType.SQL:
                    rt = WriteLogSQL(User, Message);
                    break;
                case LogType.System:
                    rt = WriteLogSystem(User, Message);
                    break;
            }
            writing = false;
            return rt;
        }
        #endregion

        #region 分类型写日至
        private bool WriteLogFile(string User, string Message)
        {
            bool rt = false;
            FileStream fs = CreateLogFile();
            try
            {
                if (fs != null)
                {
                    StreamWriter sw = new StreamWriter(fs, Encoding.Default);
                    if(User != "0")
                        sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + Message + " BY " + User);
                    else
                        sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + Message);
                    sw.Close();
                    fs.Close();
                    rt = true;
                }
            }
            catch (Exception err)
            {
                _ErrMessages = err.Message;
            }
            return rt;

        }

        private bool WriteLogSQL(string User, string Message)
        {
            bool rt = false;
            if (CreateSQL())
            {
                try
                {
                    DataTable log = DataAccess.GetTable("Select TOP 0 * FROM Sys_Logs");
                    DataAccess.WriteLogType = LogType.None;
                    DataRow LogRow = log.NewRow();
                    LogRow["MakeTime"] = DateTime.Now;
                    LogRow["Maker"] = User;
                    LogRow["Messages"] = Message;
                    if (DataAccess.Insert("Sys_Logs", LogRow) == "")
                    {
                        _ErrMessages = DataAccess.ErrMessages;
                    }
                    else
                    {
                        rt = true;
                    }
                }
                catch (Exception err)
                {
                    _ErrMessages = err.Message ;
                }
                if (_ErrMessages != "")
                {
                    WriteLogFile(User, Message);
                }
            }
            return rt;
        }

        private bool WriteLogSystem(string User, string Message)
        {
            bool rt = false;
            EventLog log = CreateSystem();
            if (log != null)
            {
                try
                {
                    if (User != "0")
                        log.WriteEntry(Message + " BY " + User);
                    else
                        log.WriteEntry(Message);
                }
                catch (Exception err)
                {
                    _ErrMessages = err.Message;
                }
            }
            return rt;
        }
        #endregion

        #region 生成日至文件
        private FileStream CreateLogFile()
        {
            FileStream fs = null;
            try
            {
                string fName = System.AppDomain.CurrentDomain.BaseDirectory + "\\Logs\\" + DateTime.Now.ToString("yyyyMMdd") + ".Log";
                if (!Directory.Exists(System.AppDomain.CurrentDomain.BaseDirectory + "\\Logs"))
                {
                    Directory.CreateDirectory(System.AppDomain.CurrentDomain.BaseDirectory + "\\Logs");
                }
                if (!File.Exists(fName))
                {
                    StreamWriter sw = File.CreateText(fName);
                    sw.Close();
                }
                fs = new FileStream(fName, FileMode.Append);
            }
            catch(Exception err)
            {
                _ErrMessages = err.Message;
            }
            return fs;
        }

        private bool CreateSQL()
        {
            bool rt = false;
            if (DataAccess != null)
            {
                string sql = @"if not exists(select name from dbo.sysobjects where name = 'Sys_Logs')
                Create Table Sys_Logs(
	                Indx	bigint IDENTITY (1, 1) NOT NULL ,
	                Maker	bigint,
	                MakeTime	datetime,
	                Messages	nvarchar(3000)
	                CONSTRAINT
	                PK_Sys_Logs PRIMARY KEY CLUSTERED 
	                (
	                    Indx
	                )
                    ) ON [PRIMARY]";
                rt = DataAccess.ExecuteSql(sql);
                _ErrMessages = DataAccess.ErrMessages;
            }
            return rt;
        }

        private EventLog CreateSystem()
        {
            string fName = System.AppDomain.CurrentDomain.BaseDirectory + "\\Logs\\" + DateTime.Now.ToString("yyyyMMdd");
            if (!Directory.Exists(System.AppDomain.CurrentDomain.BaseDirectory + "\\Logs"))
            {
                Directory.CreateDirectory(System.AppDomain.CurrentDomain.BaseDirectory + "\\Logs");
            }
            EventLog log = null;
            try
            {
                log = new EventLog(fName);
            }
            catch(Exception err)
            {
                _ErrMessages = err.Message;
            }
            return log;
        }
        #endregion
    }
}
