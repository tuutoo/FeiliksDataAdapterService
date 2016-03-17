using System;
using System.Data;
using System.Xml;
using System.Data.SqlClient;
using System.Collections;
using System.Text;

namespace FeiliksDataAdapter
{
	/// <summary>
	/// 通用数据访问 For SQL Server
	/// Create By Duck Lee 2005-4-15
	/// </summary>
	public class SQLDataAccess:IDataAccess ,System.IDisposable
	{
		#region 私有变量
		private System.Data.SqlClient.SqlConnection cn;
		private System.Data.SqlClient.SqlTransaction tran;
		private string _ErrMessages;
		private bool canbreak = false;
        private bool running = false;
        private LogType _logType;
        private SysLog SysLogs;
		#endregion

        private int _CommandTimeOut;

        public int CommandTimeOut
        {
            get { return _CommandTimeOut; }
            set { _CommandTimeOut = value; }
        }

		/// <summary>
		/// 构造函数
		/// </summary>
        /// <param name="cnString">用于数据连接的字符串</param>
		public SQLDataAccess(string cnString)
		{
			_ErrMessages = "";
            //_logType = LogType.File;
            WriteLogType = LogType.File;
            tran = null;
			try
			{
				cn = new System.Data.SqlClient.SqlConnection(cnString);
				cn.Open();
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
			}
            _CommandTimeOut = 0;
		}

		#region 消息和连接
		/// <summary>
		/// 当出现错误时返回错误提示字符串
		/// </summary>
		public string ErrMessages
		{
			get
			{
				return _ErrMessages;
			}
		}

		/// <summary>
        /// 返回当前数据连接
		/// </summary>
		public System.Data.SqlClient.SqlConnection ActiveCN
		{
			get 
			{
				return cn;
			}
		}

        /// <summary>
        /// 写日至类型
        /// </summary>
        public LogType WriteLogType
        {
            get
            {
                return _logType;
            }
            set
            {
                _logType = value;
                switch (value)
                {
                    case LogType.File:
                        SysLogs = new SysLog(value);
                        break;
                    case LogType.SQL:
                        SysLogs = new SysLog(value);
                        SysLogs.DataAccess = this;
                        break;
                    case LogType.System:
                        SysLogs = new SysLog(value);
                        break;
                    case LogType.None :
                        SysLogs = null;
                        break;
                }
            }
        }

		#endregion

		#region 事务
		/// <summary>
		/// 开始一个事务
		/// </summary>
		/// <returns></returns>
		public bool BeginTrans()
		{
			_ErrMessages = "";
			if(tran == null)
			{
				try
				{
                    if (cn.State != ConnectionState.Open)
                        cn.Open();
					tran = cn.BeginTransaction();
					return true;
				}
				catch(System.Exception err)
				{
					_ErrMessages = err.Message;
					return false;
				}
			}
			else
			{
				_ErrMessages = "事务已经存在";
				return false;
			}
		}

		/// <summary>
		/// 提交事务
		/// </summary>
		/// <returns></returns>
		public bool ComitTrans()
		{
            _ErrMessages = "";
			if(tran != null)
			{
				try
				{
					tran.Commit();
					tran.Dispose();
                    tran = null;
					return true;
				}
				catch(System.Exception err)
				{
					_ErrMessages = err.Message;
					return false;
				}
			}
			else
			{
				_ErrMessages = "事务不存在请使用BegingTrans";
                return false;
			}
		}

		/// <summary>
		/// 事务回滚
		/// </summary>
		/// <returns></returns>
		public bool RoolbackTrans()
		{
            _ErrMessages = "";
			if(tran != null)
			{
				try
				{
					tran.Rollback();
					tran.Dispose();
                    tran = null;
					return true;
				}
				catch(System.Exception err)
				{
					_ErrMessages = err.Message;
					return false;
				}
			}
			else
			{
				_ErrMessages = "事务不存在请使用BeginTrans";
				return false;
			}
		}
		#endregion

		#region 获取数据
		/// <summary>
		/// 获取数据表
		/// </summary>
		/// <param name="SQL">用于获取数据表的SQL语句</param>
		/// <returns></returns>
		public DataTable GetTable(string SQL)
		{
            _ErrMessages = "";
            while (running)
            {
                System.Threading.Thread.Sleep(100);
            }
            running = true ;
			DataTable rtTable = new DataTable();
			try
            {
                if (cn.State != ConnectionState.Open)
                    cn.Open();

				System.Data.SqlClient.SqlDataAdapter sda = new System.Data.SqlClient.SqlDataAdapter(SQL,cn);
				if(tran!=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
                sda.SelectCommand.CommandTimeout = _CommandTimeOut;
				sda.Fill(rtTable);
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
				rtTable = null;
			}
            running = false;
			return rtTable;
		}

		/// <summary>
		/// 获取数据表
		/// </summary>
		/// <param name="SQL">>用于获取数据表的SQL语句</param>
		/// <param name="parms">参数集合</param>
		/// <returns></returns>
		public DataTable GetTable(string SQL, System.Collections.Hashtable parms)
		{
            _ErrMessages = "";
            while (running)
            {
                System.Threading.Thread.Sleep(100);
            }
            running = true;

			DataTable rtTable = new DataTable();
            try
            {
                if (cn.State != ConnectionState.Open)
                    cn.Open();

                System.Data.SqlClient.SqlDataAdapter sda = new System.Data.SqlClient.SqlDataAdapter(SQL, cn);
                if (tran != null)
                {
                    sda.SelectCommand.Transaction = tran;
                    
                }
                sda.SelectCommand.CommandTimeout = _CommandTimeOut;
                DataTable pList = new DataTable();
                int rowcount = 0;
                foreach (System.Collections.DictionaryEntry p in parms)
                {
                    if (p.Value is ArrayList)
                    {
                        pList.Columns.Add(p.Key.ToString());
                        if (rowcount == 0)
                        {
                            rowcount = ((ArrayList)p.Value).Count;
                        }
                        else
                        {
                            rowcount = rowcount * ((ArrayList)p.Value).Count;
                        }
                    }
                    else
                    {
                        sda.SelectCommand.Parameters.Add(p.Key.ToString(), p.Value);
                    }
                }
                if (rowcount > 0)
                {
                    DataRow r = pList.NewRow();
                    DataTable tmp = new DataTable();
                    for (int i = 0; i < rowcount; i++)
                    {
                        foreach (DataColumn col in pList.Columns)
                        {
                            ArrayList vs = (ArrayList)parms[col.ColumnName];
                            int currow = i % vs.Count;
                            r[col] = vs[currow];

                            if (sda.SelectCommand.Parameters.Contains(col.ColumnName))
                            {
                                sda.SelectCommand.Parameters[col.ColumnName].Value = vs[currow];
                            }
                            else
                            {
                                sda.SelectCommand.Parameters.Add(col.ColumnName, vs[currow]);
                            }
                        }
                        sda.Fill(tmp);
                    }
					ArrayList orders = new ArrayList();
                    //List<string> orders = new List<string>();
                    for (int i = 0; i < tmp.Columns.Count; i++)
                    {
                        DataColumn col = tmp.Columns[i];
                        rtTable.Columns.Add(col.ColumnName, col.DataType);
                        orders.Add(col.ColumnName);
                    }
                    DataView finder = new DataView(rtTable);
                    finder.Sort = string.Join(",", (string[])orders.ToArray());
                    foreach (DataRow tmprow in tmp.Rows)
                    {
                        if(finder.FindRows(tmprow.ItemArray).Length == 0)
                            rtTable.ImportRow(tmprow);
                    }
                    rtTable.AcceptChanges();
                }
                else
                {
                    sda.Fill(rtTable);
                }
            }
            catch (System.Exception err)
            {
                _ErrMessages = err.Message;
                rtTable = null;
            }
            running = false;
			return rtTable;
		}


//        public DataTable GetTable(string SQL, int startpage, int pagesize)
//        {
//            _ErrMessages = "";
//            while (running)
//            {
//                System.Threading.Thread.Sleep(100);
//            }
//            running = true;
//            DataTable rtTable = new DataTable();
//            try
//            {
//                if (cn.State != ConnectionState.Open)
//                    cn.Open();
//
//                System.Data.SqlClient.SqlDataAdapter sda = new System.Data.SqlClient.SqlDataAdapter(SQL, cn);
//                if (tran != null)
//                {
//                    sda.SelectCommand.Transaction = tran;
//                    
//                }
//                sda.SelectCommand.CommandTimeout = _CommandTimeOut;
//                sda.Fill((startpage - 1) * pagesize, pagesize, new DataTable[] { rtTable });
//            }
//            catch (System.Exception err)
//            {
//                _ErrMessages = err.Message;
//                rtTable = null;
//            }
//            running = false;
//            return rtTable;
//
//        }

		/// <summary>
		/// 获取DataSet
		/// </summary>
		/// <param name="SQL">获取DataSet所用的SQL</param>
		/// <returns></returns>
		public DataSet GetDataSet(string SQL)
		{
            _ErrMessages = "";
            while (running)
            {
                System.Threading.Thread.Sleep(100);
            }
            running = true;

			DataSet rt = new DataSet();
			try
			{
                if (cn.State != ConnectionState.Open)
                    cn.Open();

				System.Data.SqlClient.SqlDataAdapter sda = new System.Data.SqlClient.SqlDataAdapter(SQL,cn);
				if(tran!=null)
				{
					sda.SelectCommand.Transaction = tran;
                    
				}
                sda.SelectCommand.CommandTimeout = _CommandTimeOut;
				sda.Fill(rt);
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
				rt = null;
			}
            running = false;
			return rt;
		}

		/// <summary>
		/// 获取DataSet
		/// </summary>
		/// <param name="SQL"></param>
		/// <param name="parms"></param>
		/// <returns></returns>
		public DataSet GetDataSet(string SQL,System.Collections.Hashtable parms)
		{
            _ErrMessages = "";
            while (running)
            {
                System.Threading.Thread.Sleep(100);
            }
            running = true;

			DataSet rt = new DataSet();
			try
			{
                if (cn.State != ConnectionState.Open)
                    cn.Open();

				System.Data.SqlClient.SqlDataAdapter sda = new System.Data.SqlClient.SqlDataAdapter(SQL,cn);
				if(tran!=null)
				{
					sda.SelectCommand.Transaction = tran;
                    
				}
                sda.SelectCommand.CommandTimeout = _CommandTimeOut;
				foreach(System.Collections.DictionaryEntry  p in parms)
					sda.SelectCommand.Parameters.Add(p.Key.ToString(),p.Value);
				sda.Fill(rt);
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
				rt = null;
			}
            running = false;
			return rt;
		}

		#endregion

		#region 插入数据
		/// <summary>
		/// 新增一条纪录默认索引字段为Indx
		/// </summary>
		/// <param name="TableName">用于新增纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <returns>返回当前新增的数据索引</returns>
		public string Insert(string TableName,DataRow Data)
		{
			string indx = Insert(TableName,Data,"Indx");
			return indx;
		}

		/// <summary>
		/// 新增一条纪录
		/// </summary>
		/// <param name="TableName">用于新增纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <param name="IndxField">索引字段</param>
		/// <returns>返回当前新增的数据索引</returns>
		public string Insert(string TableName,DataRow Data,string IndxField)
		{
			_ErrMessages = "";
            if (IndxField == "")
                IndxField = "Indx";
            while (running)
            {
                System.Threading.Thread.Sleep(100);
            }
            running = true;
            string message = "";
            try
            {
                if (cn.State != ConnectionState.Open)
                    cn.Open();

                //string fileds = "";
                //string values = "";

                #region 获取表设置
                SqlDataAdapter sda = new SqlDataAdapter("Select TOP 0 * From " + TableName, cn);
                if (tran != null)
                {
                    sda.SelectCommand.Transaction = tran;
                    
                }
                sda.SelectCommand.CommandTimeout = _CommandTimeOut;
                DataTable tmp = new DataTable();
                sda.Fill(tmp);
                #endregion

                SqlCommand sci = cn.CreateCommand();
                sci.CommandTimeout = _CommandTimeOut;
                if (tran != null)
                {
                    sci.Transaction = tran;
                }
                StringBuilder sbfields = new StringBuilder();
                StringBuilder sbvalues = new StringBuilder();
                StringBuilder values = new StringBuilder();

                #region 生成命令所需字符串
                int n = 1;
                foreach (DataColumn col in tmp.Columns)
                {
                    if (Data.Table.Columns.Contains(col.ColumnName))
                    {
                        if (!Data.IsNull(col.ColumnName) ||
                            Data.Table.Columns[col.ColumnName].ExtendedProperties["DateTime"] != null)
                        {
                            sbfields.Append("[" + col.ColumnName + "],");
                            if (Data.Table.Columns[col.ColumnName].ExtendedProperties["DateTime"] != null &&
                                Data.Table.Columns[col.ColumnName].ExtendedProperties["DateTime"].ToString() == "System")
                            {
                                sbvalues.Append("GetDate(),");
                                values.Append("GetDate(),");
                            }
                            else
                            {
                                sbvalues.Append("@P" + n.ToString() + ",");
                                sci.Parameters.Add("@P" + n.ToString(), Data[col.ColumnName]);
                                values.Append("'" + Data[col.ColumnName].ToString() + "',");
                                n++;
                            }

                        }
                    }
                }
                #endregion

                if (sbfields.Length > 0)
                {
                    #region 生成命令
                    sbfields.Length = sbfields.Length - 1;
                    sbvalues.Length = sbvalues.Length - 1;
                    values.Length = values.Length - 1;
                    StringBuilder sql = new StringBuilder();
                    sql.Append("Insert Into ");
                    sql.Append(TableName);
                    sql.Append(" (");
                    sql.Append(sbfields.ToString());
                    sql.Append(") Values (");
                    sql.Append(sbvalues.ToString());
                    sql.Append(") ");

                    if (!Data.IsNull(IndxField))
                    {
                        sql.Append(" Select '" + Data[IndxField].ToString() + "'");
                    }
                    else
                    {
                        sql.Append(" Select SCOPE_IDENTITY()");
                    }

                    sci.CommandText = sql.ToString();
                    message = "Insert Into " + TableName + "(" + sbfields.ToString() + ") Values (" + values.ToString() + ")";
                    #endregion

                    object rt = sci.ExecuteScalar();
                    running = false;
                    if (rt != null)
                    {
                        if (SysLogs != null)
                        {
                            message = message + " Return Indx " + rt.ToString();
                            SysLogs.WriteLog(message);
                        }
                        return rt.ToString();
                    }
                    else
                    {
                        if (SysLogs != null)
                            SysLogs.WriteLog(message);
                        return "";
                    }
                }
                else
                {
                    if (SysLogs != null)
                        SysLogs.WriteLog("No Value To Insert");
                    return "";
                }

			}
			catch(System.Exception e)
			{
				_ErrMessages = e.Message;
                running = false;
                if (SysLogs != null)
                {
                    message = message + " ErrMessage: " + _ErrMessages ;
                    SysLogs.WriteLog(message);
                }
				return "";
			}
		}
		
		/// <summary>
		/// 按照整个表做新增动作
		/// </summary>
		/// <param name="TableName">用于新增纪录的数据库表名</param>
		/// <param name="Data">数据表</param>
		/// <returns>返回插入的行数</returns>
		public int Insert(string TableName,DataTable Data)
		{
            _ErrMessages = "";
            int rt = 0;
            while (running)
            {
                System.Threading.Thread.Sleep(100);
            }
            running = true;
            string message = "";
            try
            {
                if (cn.State != ConnectionState.Open)
                    cn.Open();

                //string fileds = "";
                //string values = "";

                #region 获取表设置
                SqlDataAdapter sda = new SqlDataAdapter("Select TOP 0 * From " + TableName, cn);
                if (tran != null)
                {
                    sda.SelectCommand.Transaction = tran;
                }
                sda.SelectCommand.CommandTimeout = _CommandTimeOut;
                DataTable tmp = new DataTable();
                sda.Fill(tmp);
                sda.Dispose();
                sda  = null;
                #endregion

                SqlCommand sci = cn.CreateCommand();
                sci.CommandTimeout = _CommandTimeOut;
                if (tran != null)
                    sci.Transaction = tran;

                StringBuilder sbfields = new StringBuilder();
                StringBuilder sbvalues = new StringBuilder();
                StringBuilder values = new StringBuilder();
                foreach (DataRow r in Data.Rows)
                {
                    #region 初始化
                    sci.Parameters.Clear();
                    sci.CommandText = "";
                    sbfields.Length = 0;
                    sbvalues.Length = 0;
                    values.Length = 0;
                    #endregion

                    #region 生成命令所需字符串
                    int n = 1;
                    foreach (DataColumn col in tmp.Columns)
                    {
                        if (Data.Columns.Contains(col.ColumnName))
                        {
                            if (!r.IsNull(col.ColumnName) ||
                                Data.Columns[col.ColumnName].ExtendedProperties["DateTime"] != null)
                            {
                                sbfields.Append("[" + col.ColumnName + "],");
                                if (Data.Columns[col.ColumnName].ExtendedProperties["DateTime"] != null &&
                                    Data.Columns[col.ColumnName].ExtendedProperties["DateTime"].ToString() == "System")
                                {
                                    sbvalues.Append("GetDate(),");
                                    values.Append("GetDate(),");
                                }
                                else
                                {
                                    sbvalues.Append("@P" + n.ToString() + ",");
                                    sci.Parameters.Add("@P" + n.ToString(), r[col.ColumnName]);
                                    values.Append("'" + r[col.ColumnName].ToString() + "',");
                                    n++;
                                }
                            }
                        }
                    }
                    #endregion

                    if (sbfields.Length > 0)
                    {
                        #region 生成命令
                        sbfields.Length = sbfields.Length - 1;
                        sbvalues.Length = sbvalues.Length - 1;
                        values.Length = values.Length - 1;
                        StringBuilder sql = new StringBuilder();
                        sql.Append("Insert Into ");
                        sql.Append(TableName);
                        sql.Append(" (");
                        sql.Append(sbfields.ToString());
                        sql.Append(") Values (");
                        sql.Append(sbvalues.ToString());
                        sql.Append(") ");

                        sci.CommandText = sql.ToString();
                        message = "Insert Into " + TableName + "(" + sbfields.ToString() + ") Values (" + values.ToString() + ")";
                        #endregion

                        if (SysLogs != null)
                        {
                            running = false;
                            SysLogs.WriteLog(message);
                            running = true;
                        }
                        int i = sci.ExecuteNonQuery();
                        if (i > 0)
                            rt++;
                    }
                }
                running = false;
                return rt;

            }
            catch (System.Exception e)
            {
                _ErrMessages = e.Message;
                running = false;
                if (SysLogs != null)
                {
                    message = message + " ErrMessage: " + _ErrMessages;
                    SysLogs.WriteLog(message);
                }
                return -1;
            }
        }

        /// <summary>
        /// 按照行集添加数据
        /// </summary>
        /// <remarks>
        /// 行集所在的表需要设置ExtendedProperties确定添加时设置
        /// KeyField 设置添加表中的主键字段
        /// UpdateKey 设置是否连同主键一起插入，若不存在则为true
        /// 若UpdateKey=false则将最近插入的主键最大值保存至主键字段
        /// 若列设置ExtendedProperties["SystemDate"] = true
        /// 则使用GetDate()替换参数
        /// </remarks>
        /// <param name="TableName">添加数据的数据库表名</param>
        /// <param name="InsertRows">插入数据的行集</param>
        /// <returns>插入数据条数</returns>
        public int Insert(string TableName, DataRow[] InsertRows)
        {
            int rt = 0;
            _ErrMessages = "";
            if (InsertRows.Length > 0)
            {
                if (cn.State != ConnectionState.Open)
                    cn.Open();

                DataTable iTable = InsertRows[0].Table;
                if (iTable != null)
                {
                    #region 检查设置
                    string KeyField = "";
                    bool UpdateKey = false;
                    if (iTable.ExtendedProperties["KeyField"] != null)
                    {
                        KeyField = iTable.ExtendedProperties["KeyField"].ToString();
                    }
                    if (iTable.ExtendedProperties["UpdateKey"] != null)
                    {
                        UpdateKey = (bool)iTable.ExtendedProperties["UpdateKey"];
                    }
                    #endregion

                    ArrayList cols = new ArrayList() ;
                    ArrayList parms = new ArrayList();

                    #region 设置SQL
                    for ( int i = 0 ; i < iTable.Columns.Count ; i ++)
                    {
                        if (UpdateKey || (KeyField != iTable.Columns[i].ColumnName && !UpdateKey))
                        {
                            cols.Add("[" + iTable.Columns[i].ColumnName + "]");
                            if (iTable.Columns[i].ExtendedProperties["SystemDate"] != null)
                                parms.Add("GetDate()");
                            else
                                parms.Add("@P" + i.ToString());
                        }
                    }
					string SQL = "Insert Into " + TableName + "(" +
						string.Join(",", (string[])cols.ToArray()) + ") Values (" +
						string.Join(",", (string[]) parms.ToArray()) + ")";
                    if (KeyField != "" && !UpdateKey)
                    {
                        SQL += ";Select SCOPE_IDENTITY()";
                    }
                    #endregion

                    SqlCommand cmd = new SqlCommand(SQL, cn);
                    cmd.CommandTimeout = _CommandTimeOut;
                    if (tran != null)
                    {
                        cmd.Transaction = tran;
                    }
                    foreach (DataRow iRow in InsertRows)
                    {
                        for (int i = 0; i < iTable.Columns.Count; i++)
                        {
                            #region 添加数据
                            if (UpdateKey || (KeyField != iTable.Columns[i].ColumnName && !UpdateKey))
                            {
                                bool sysTime = false;
                                if (iTable.Columns[i].ExtendedProperties["SystemDate"] != null)
                                {
                                    sysTime = true;
                                }
                                string pName = string.Format("@P{0}",i);
                                if (cmd.Parameters.Contains(pName))
                                {
                                    if (!sysTime)
                                    {
                                        cmd.Parameters[pName].Value = iRow[i];
                                    }
                                }
                                else
                                {
                                    if (!sysTime)
                                    {
                                        cmd.Parameters.Add(pName, iRow[i]);
                                    }
                                }
                            }
                            #endregion

                        }
                        try
                        {
                            #region 执行
                            if (!UpdateKey && KeyField != "")
                            {
                                object obj = cmd.ExecuteScalar();
                                if (obj != null)
                                {
                                    rt++;
                                    iRow[KeyField] = obj;
                                }
                            }
                            else
                            {
                                rt += cmd.ExecuteNonQuery();
                            }
                            #endregion
                        }
                        catch (System.Exception err)
                        {
                            rt = 0;
                            _ErrMessages = err.Message;
                            break;
                        }
                    }
                }
                else
                {
                    _ErrMessages = "行集没有对应内存表";
                }
            }
            return rt;
        }

		#endregion

		#region 更新数据

		/// <summary>
		/// 更新一条纪录默认索引字段为Indx
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <returns></returns>
		public string Update(string TableName,DataRow Data)
		{
			string indx = Update(TableName,Data,"Indx");
			return indx;
		}

        public string Update(string TableName, DataRow Data, string IndxField)
        {
            return Update(TableName, Data, IndxField, "");
        }
		/// <summary>
		/// 更新一条纪录
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <param name="IndxField">索引字段</param>
		/// <returns></returns>
		public string Update(string TableName,DataRow Data,string IndxField , string CheckFields)
		{
			_ErrMessages = "";
            while (running)
            {
                System.Threading.Thread.Sleep(100);
            }
            running = true;

			try
			{
                if (cn.State != ConnectionState.Open)
                    cn.Open();

                string key = Data[IndxField].ToString();
                if (Data.Table.ExtendedProperties.Contains("OldKeys"))
                {
                    Hashtable oldkeys = (Hashtable)Data.Table.ExtendedProperties["OldKeys"];
                    if(oldkeys.Contains(key))
                        key = oldkeys[key].ToString();
                }
                running = false;
                DataTable cmdTable = GetTable("SELECT * FROM " + TableName + " WHERE " + IndxField + " = '" + key + "'");
                running = true;
                if (CheckFields.Length > 0)
                {
                    string[] ckfs = CheckFields.Split(",".ToCharArray());
                    foreach (string chf in ckfs)
                    {
                        if (chf != "")
                        {
                            if (Data.Table.Columns.Contains(chf) &&
                                cmdTable.Columns.Contains(chf))
                            {
                                if (cmdTable.Rows.Count > 0)
                                {
                                    if (cmdTable.Columns[chf].DataType == typeof(byte[]))
                                    {
                                        byte[] a = (byte[])Data[chf];
                                        byte[] b = (byte[])cmdTable.Rows[0][chf];
                                        if (a.Length != b.Length)
                                        {
                                            _ErrMessages = chf + " Changed";
                                            running = false;
                                            return "";
                                        }
                                        for (int i = 0; i < a.Length;i++ )
                                        {
                                            if (a[i] != b[i])
                                            {
                                                _ErrMessages = chf + " Changed";
                                                running = false;
                                                return "";

                                            }
                                        }
                                    }
                                    else
                                    {
                                        if (cmdTable.Rows[0][chf] != Data[chf])
                                        {
                                            _ErrMessages = chf + " Changed";
                                            running = false;
                                            return "";
                                        }
                                    }
                                }
                                cmdTable.Columns.Remove(chf);
                            }
                        }
                    }
                }
                System.Text.StringBuilder sb = new System.Text.StringBuilder();
                System.Text.StringBuilder logsb = new System.Text.StringBuilder();
                sb.Append("Update ");
                sb.Append(TableName);
                sb.Append(" SET \r\n");
                logsb.Append("Update ");
                logsb.Append(TableName);
                logsb.Append(" SET ");
                System.Data.SqlClient.SqlCommand sc = cn.CreateCommand();
                sc.CommandTimeout = _CommandTimeOut;
                if (tran != null)
                    sc.Transaction = tran;

                bool canUpdate = false;
                if (cmdTable.Rows.Count > 0)
                {
                    foreach (DataColumn col in cmdTable.Columns)
                    {
                        
                        if (Data.Table.Columns.Contains(col.ColumnName))
                        {
                            if (Data[col.ColumnName].ToString() != cmdTable.Rows[0][col].ToString()
                                || Data.Table.Columns[col.ColumnName].ExtendedProperties["DateTime"] != null
                                || col.DataType == typeof(byte[]))
                            {
                                
                                if (Data.Table.Columns[col.ColumnName].ExtendedProperties["DateTime"] != null
                                    && Data.Table.Columns[col.ColumnName].ExtendedProperties["DateTime"].ToString() == "System")
                                {
                                    #region 系统时间
                                    sb.Append(col.ColumnName);
                                    sb.Append(" = GetDate()");
                                    sb.Append(",\r\n");

                                    logsb.Append(col.ColumnName);
                                    logsb.Append(" = GetDate()");
                                    logsb.Append("',");
                                    #endregion
                                    canUpdate = true;
                                }
                                else
                                {

                                    if (Data.Table.Columns[col.ColumnName].ExtendedProperties["Update"] != null)
                                    {
                                        
                                        if (Data.Table.Columns[col.ColumnName].ExtendedProperties["Update"].ToString().ToLower() == "true")
                                        {
                                            #region 根据列更新设置决定是否更新列
                                            sb.Append(col.ColumnName);
                                            sb.Append(" = @");
                                            sb.Append(col.ColumnName);
                                            sb.Append(",\r\n");

                                            logsb.Append(col.ColumnName);
                                            if (Data.IsNull(col.ColumnName))
                                            {
                                                logsb.Append(" = NULL ,");
                                            }
                                            else
                                            {
                                                logsb.Append(" = '");
                                                logsb.Append(Data[col.ColumnName].ToString());
                                                logsb.Append("',");
                                            }
                                            sc.Parameters.Add("@" + col.ColumnName, Data[col.ColumnName]);
                                            #endregion
                                            canUpdate = true;
                                        }
                                        
                                    }
                                    else
                                    {
                                        bool u = false;
                                        if (col.DataType == typeof(byte[]))
                                        {
                                            #region 二进制处理
                                            if (Data.IsNull(col.ColumnName) ||
                                                cmdTable.Rows[0].IsNull(col.ColumnName))
                                            {
                                                u = true;
                                            }
                                            else
                                            {
                                                byte[] a = (byte[])Data[col.ColumnName];
                                                byte[] b = (byte[])cmdTable.Rows[0][col.ColumnName];
                                                if (a.Length != b.Length)
                                                {
                                                    u = true;
                                                }
                                                else
                                                {
                                                    for (int i = 0; i < a.Length; i++)
                                                    {
                                                        if (a[i] != b[i])
                                                        {
                                                            u = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            #endregion
                                        }
                                        else
                                        {
                                            #region 普通处理
                                            if (Data[col.ColumnName].ToString() != cmdTable.Rows[0][col.ColumnName].ToString())
                                                u = true;
                                            #endregion
                                        }
                                        if (u)
                                        {
                                            #region 允许更新
                                            if (Data.IsNull(col.ColumnName))
                                            {
                                                sb.Append(col.ColumnName);
                                                sb.Append(" = NULL,\r\n");
                                            }
                                            else
                                            {
                                                sb.Append(col.ColumnName);
                                                sb.Append(" = @");
                                                sb.Append(col.ColumnName);
                                                sb.Append(",\r\n");
                                                sc.Parameters.Add("@" + col.ColumnName, Data[col.ColumnName]);
                                            }

                                            logsb.Append(col.ColumnName);
                                            if (Data.IsNull(col.ColumnName))
                                            {
                                                logsb.Append(" = NULL ,");
                                            }
                                            else
                                            {
                                                logsb.Append(" = '");
                                                logsb.Append(Data[col.ColumnName].ToString());
                                                logsb.Append("',");
                                            }
                                            #endregion
                                            canUpdate = true;
                                        }
                                    }
                                }
                                
                            }
                        }
                    }
                    if (canUpdate)
                    {
                        sb.Remove(sb.Length - 3, 1);
                        sb.Append(" WHERE \r\n");
                        sb.Append(IndxField);
                        sb.Append(" = @Indx");

                        logsb.Remove(logsb.Length - 1, 1);
                        logsb.Append(" WHERE ");
                        logsb.Append(IndxField);
                        logsb.Append(" = " + Data[IndxField].ToString());

                        sc.Parameters.Add("@Indx", key);
                        sc.CommandText = sb.ToString();
                        if (tran != null)
                            sc.Transaction = tran;
                        sc.ExecuteNonQuery();
                        if (SysLogs != null)
                        {
                            running = false;
                            SysLogs.WriteLog(logsb.ToString());
                            running = true;
                        }
                    }
                }
                #region 代码重写
                //SqlDataAdapter sda = new SqlDataAdapter("Select * From " + TableName,cn);
                //if(tran!=null)
                //    sda.SelectCommand.Transaction = tran;

                //System.Data.SqlClient.SqlCommandBuilder scb = new SqlCommandBuilder(sda);
                //System.Data.SqlClient.SqlCommand sc = scb.GetUpdateCommand();
                //foreach(System.Data.SqlClient.SqlParameter sp in sc.Parameters)
                //{
                //    try
                //    {
                //        if(sp.SourceColumn != "")
                //        {
                //            if(Data.Table.Columns[sp.SourceColumn]!=null)
                //                sp.Value = Data[sp.SourceColumn];
                //            else
                //                sp.Value = DBNull.Value;
                //        }
                //    }
                //    catch
                //    {
                //        _ErrMessages += sp.SourceColumn + ":";
                //    }
                //}
                //int pos = sc.CommandText.IndexOf("WHERE");
                //if( pos > 0)
                //    sc.CommandText = sc.CommandText.Substring(0,pos);
                //sc.CommandText += " WHERE " + IndxField + " = '" + Data[IndxField].ToString() + "'";
                //sc.ExecuteNonQuery();
                #endregion

                sb.Length = 0;
                sc.Dispose();
                running = false;
				return Data[IndxField].ToString();
			}
			catch(System.Exception e)
			{
				_ErrMessages = e.Message;
                running = false;
                if (SysLogs != null)
                    SysLogs.WriteLog(_ErrMessages);
				return "";
				
			}
			
		}

		/// <summary>
		/// 按照整个表做更新动作
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据表</param>
		/// <remarks>默认IndxField为"Indx"</remarks>
		/// <returns>返回被更新的行数</returns>
		public int Update(string TableName,DataTable Data)
		{
			return Update(TableName,Data,"Indx");
		}
		/// <summary>
		/// 按照整个表做更新动作
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据表</param>
		/// <param name="IndxField">数据的索引列名称</param>
		/// <returns>返回被更新的行数</returns>
		public int Update(string TableName,DataTable Data,string IndxField)
		{
            return Update(TableName, "", Data, IndxField);
            #region 更新方法
            //int rt = 0;
            //int i = 0;
            //_ErrMessages = "";
            //while (running)
            //{
            //    System.Threading.Thread.Sleep(100);
            //}
            //running = true;

            //string fileds = "";
            //string values = "";
            //string message = "";

            //try
            //{
            //    if (cn.State != ConnectionState.Open)
            //        cn.Open();

            //    SqlDataAdapter sda = new SqlDataAdapter("Select * From " + TableName,cn);
            //    if(tran!=null)
            //        sda.SelectCommand.Transaction = tran;

            //    System.Data.SqlClient.SqlCommandBuilder scb = new SqlCommandBuilder(sda);
            //    System.Data.SqlClient.SqlCommand sc = scb.GetUpdateCommand();
            //    System.Data.SqlClient.SqlCommand sci = scb.GetInsertCommand();

            //    int pos = sc.CommandText.IndexOf("WHERE");
            //    if( pos > 0)
            //        sc.CommandText = sc.CommandText.Substring(0,pos);
            //    sc.CommandText += " WHERE " + IndxField + " = @" + IndxField;
            //    if (!sc.Parameters.Contains("@" + IndxField))
            //    {
            //        sc.Parameters.Add("@" + IndxField, SqlDbType.NVarChar);
            //        sc.Parameters["@" + IndxField].SourceColumn = IndxField;
            //    }
            //    foreach(System.Data.DataRow r in Data.Rows)
            //    {
            //        fileds = "";
            //        values = "";
            //        if(r.IsNull(IndxField))
            //        {
            //            #region 插入数据
            //            foreach (System.Data.SqlClient.SqlParameter sp in sci.Parameters)
            //            {
            //                try
            //                {
            //                    if(SysLogs != null)
            //                        fileds += sp.SourceColumn + ",";
            //                    if(r.Table.Columns[sp.SourceColumn]!=null)
            //                    {
            //                        if(sp.SourceColumn != "")
            //                        {
            //                            if (Data.Columns[sp.SourceColumn].ExtendedProperties["DateTime"] != null
            //                                && Data.Columns[sp.SourceColumn].ExtendedProperties["DateTime"].ToString() == "System")
            //                            {
            //                                sci.CommandText = sc.CommandText.Replace(sp.ParameterName + ",", "GetDate(),");
            //                                sp.Value = DBNull.Value;
            //                            }
            //                            else
            //                                sp.Value = r[sp.SourceColumn];
            //                        }
            //                    }
            //                    else
            //                    {
            //                        sp.Value = DBNull.Value;
            //                    }
            //                }
            //                catch
            //                {
            //                    _ErrMessages += sp.SourceColumn + ":";
            //                }
            //                values += sp.Value == DBNull.Value ? "NULL," : sp.Value.ToString() + ",";
            //            }
            //            i = sci.ExecuteNonQuery();
            //            if (SysLogs != null)
            //            {
            //                fileds = fileds.TrimEnd(",".ToCharArray());
            //                values = values.TrimEnd(",".ToCharArray());
            //                message = "Insert Into " + TableName + "(" + fileds + ") Values (" + values + ")";
            //                running = false;
            //                SysLogs.WriteLog(message);
            //                running = true;
            //            }
            //            #endregion
            //        }
            //        else
            //        {
            //            #region 更新数据
            //            System.Text.StringBuilder logsb = new System.Text.StringBuilder();
                        
            //            logsb.Append("Update ");
            //            logsb.Append(TableName);
            //            logsb.Append(" SET ");

            //            foreach (System.Data.SqlClient.SqlParameter sp in sc.Parameters)
            //            {
            //                try
            //                {
            //                    if (r.Table.Columns[sp.SourceColumn] != null)
            //                    {
            //                        if (sp.SourceColumn != "")
            //                        {
            //                            if (Data.Columns[sp.SourceColumn].ExtendedProperties["DateTime"] != null
            //                                && Data.Columns[sp.SourceColumn].ExtendedProperties["DateTime"].ToString() == "System")
            //                            {
            //                                sci.CommandText = sc.CommandText.Replace(sp.ParameterName + ",", "GetDate(),");
            //                                sp.Value = DBNull.Value;
            //                            }
            //                            else
            //                                sp.Value = r[sp.SourceColumn];
            //                        }
            //                    }
            //                    else
            //                    {
            //                        sp.Value = DBNull.Value;
            //                    }
            //                }
            //                catch
            //                {
            //                    _ErrMessages += sp.SourceColumn + ":";
            //                }
            //                if (sp.SourceColumn != IndxField)
            //                {
            //                    if (sp.Value != DBNull.Value)
            //                    {
            //                        logsb.Append(sp.SourceColumn);
            //                        logsb.Append(" = '");
            //                        logsb.Append(sp.Value.ToString());
            //                        logsb.Append("',");
            //                    }
            //                    else
            //                    {
            //                        logsb.Append(sp.SourceColumn + " = NULL ,");
            //                    }
            //                }
            //                else
            //                {
            //                    message = " WHERE " + IndxField + " = " + sp.Value.ToString();
            //                }
            //            }

            //            logsb.Remove(logsb.Length - 1, 1);
            //            logsb.Append(message);
                        
            //            if (SysLogs != null)
            //            {
            //                running = false;
            //                SysLogs.WriteLog(logsb.ToString());
            //                running = true;
            //            }

            //            i = sc.ExecuteNonQuery();
            //            #endregion
            //        }
            //        if(i>0)rt ++;
            //        if(OnProgress!=null)OnProgress(this,rt);
            //        if(canbreak)
            //        {
            //            _ErrMessages = "Cancle By User";
            //            break;
            //        }

            //    }
            //}
            //catch(System.Exception e)
            //{
            //    _ErrMessages = e.Message;
            //    rt = -1;
				
            //}
            //running = false;
            //return rt;
            #endregion
        }


        public int Update(string TableName, string Filter, DataTable Data, string IndxField)
        {
            return Update(TableName, Filter, Data, IndxField, true);
        }

        /// <summary>
        /// 更新表，若过滤条件满足的行作更新或插入，不满足的行作删除
        /// </summary>
        /// <param name="TableName">数据库表</param>
        /// <param name="Filter">过滤条件</param>
        /// <param name="Data">数据表</param>
        /// <returns>更新或插入的记录行数</returns>
        public int Update(string TableName, string Filter, DataTable Data, string IndxField, bool delete)
        {
            int rt = 0;
            int i = 0;
            _ErrMessages = "";
            while (running)
            {
                System.Threading.Thread.Sleep(100);
            }
            running = true;

            string message = "";

            try
            {
                if (cn.State != ConnectionState.Open)
                    cn.Open();

                SqlDataAdapter sda = new SqlDataAdapter("Select TOP 0 * From " + TableName, cn);
                //设置更新命令
                System.Data.SqlClient.SqlCommand sc = cn.CreateCommand();
                sc.CommandTimeout = _CommandTimeOut;
                //设置新增命令
                System.Data.SqlClient.SqlCommand sci = cn.CreateCommand();
                sci.CommandTimeout = _CommandTimeOut;
                //设置删除命令
                System.Data.SqlClient.SqlCommand scd = cn.CreateCommand();
                scd.CommandTimeout = _CommandTimeOut;

                scd.CommandText = "Delete " + TableName + " Where " + IndxField + " = @Indx";
                scd.Parameters.Add("@Indx", SqlDbType.NVarChar);
                if (tran != null)
                {
                    sda.SelectCommand.Transaction = tran;
                    sc.Transaction = tran;
                    sci.Transaction = tran;
                    scd.Transaction = tran;
                }
                DataTable tmp = new DataTable();
                sda.Fill(tmp);
                sda.Dispose();

                if (!tmp.Columns.Contains(IndxField))
                {
                    _ErrMessages = "No KeyField Setting " + IndxField;
                    return rt;
                }

                
                DataRow[] rs;
                Data.AcceptChanges();
                rs = Data.Select(Filter);
                bool insertkey = false;
                if (Data.Columns[IndxField].ExtendedProperties["Update"] != null)
                {

                    if (Data.Columns[IndxField].ExtendedProperties["Update"].ToString().ToLower() == "true")
                    {
                        insertkey = true;
                    }
                }

                foreach (System.Data.DataRow r in rs)
                {
                    if (canbreak)
                    {
                        _ErrMessages = "Cancle By User";
                        rt = -1;
                        break;
                    }
                    if (r.IsNull(IndxField) || insertkey)
                    {
                        #region 插入数据

                        StringBuilder sbfields = new StringBuilder();
                        StringBuilder sbvalues = new StringBuilder();
                        StringBuilder values = new StringBuilder();
                        sc.Parameters.Clear();
                        sc.CommandText = "";
                        int n = 1;
                        foreach (DataColumn col in tmp.Columns)
                        {
                            if (r.Table.Columns.Contains(col.ColumnName))
                            {
                                if (!r.IsNull(col.ColumnName) ||
                                    Data.Columns[col.ColumnName].ExtendedProperties["DateTime"] != null)
                                {
                                    sbfields.Append("[" + col.ColumnName + "],");
                                    if (Data.Columns[col.ColumnName].ExtendedProperties["DateTime"] != null &&
                                        Data.Columns[col.ColumnName].ExtendedProperties["DateTime"].ToString() == "System")
                                    {
                                        sbvalues.Append("GetDate(),");
                                        values.Append("GetDate(),");
                                    }
                                    else
                                    {
                                        sbvalues.Append("@P" + n.ToString() + ",");
                                        sci.Parameters.Add("@P" + n.ToString(), r[col.ColumnName]);
                                        values.Append("'" + r[col.ColumnName].ToString() + "',");
                                        n++;
                                    }

                                }
                            }
                        }
                        if (sbfields.Length > 0)
                        {
                            sbfields.Length = sbfields.Length - 1;
                            sbvalues.Length = sbvalues.Length - 1;
                            values.Length = values.Length - 1;

                            sci.CommandText = "Insert Into " + TableName + "(" + sbfields.ToString() + ") Values (" + sbvalues.ToString() + ")";

                            if (SysLogs != null)
                            {
                                message = "Insert Into " + TableName + "(" + sbfields.ToString() + ") Values (" + values.ToString() + ")";
                                running = false;
                                SysLogs.WriteLog(message);
                                running = true;
                            }

                            #region 改动
                            //foreach (System.Data.SqlClient.SqlParameter sp in sci.Parameters)
                            //{
                            //    try
                            //    {
                            //        if (SysLogs != null)
                            //            fileds += sp.SourceColumn + ",";
                            //        if (r.Table.Columns[sp.SourceColumn] != null)
                            //        {
                            //            if (sp.SourceColumn != "")
                            //            {
                            //                if (Data.Columns[sp.SourceColumn].ExtendedProperties["DateTime"] != null
                            //                    && Data.Columns[sp.SourceColumn].ExtendedProperties["DateTime"].ToString() == "System")
                            //                {
                            //                    sci.CommandText = sc.CommandText.Replace(sp.ParameterName + ",", "GetDate(),");
                            //                    sp.Value = DBNull.Value;
                            //                }
                            //                else
                            //                    sp.Value = r[sp.SourceColumn];
                            //            }
                            //        }
                            //        else
                            //        {
                            //            sp.Value = DBNull.Value;
                            //        }
                            //    }
                            //    catch
                            //    {
                            //        _ErrMessages += sp.SourceColumn + ":";
                            //    }
                            //    values += sp.Value == DBNull.Value ? "NULL," : sp.Value.ToString() + ",";

                            //}
                            #endregion

                            i = sci.ExecuteNonQuery();
                            sci.Parameters.Clear();
                        }
                        else
                        {
                            i = 0;
                        }
                        #endregion
                    }
                    else
                    {
                        #region 更新数据

                        string key = r[IndxField].ToString();
                        if (Data.ExtendedProperties.Contains("OldKeys"))
                        {
                            Hashtable oldkeys = (Hashtable)Data.ExtendedProperties["OldKeys"];
                            if (oldkeys.Contains(key))
                            {
                                key = oldkeys[key].ToString();
                            }
                        }

                        sc.CommandText = "";
                        sc.Parameters.Clear();

                        System.Text.StringBuilder logsb = new System.Text.StringBuilder();

                        logsb.Append("Update ");
                        logsb.Append(TableName);
                        logsb.Append(" SET ");

                        StringBuilder sql = new StringBuilder();

                        sql.Append("Update ");
                        sql.Append(TableName);
                        sql.Append(" SET ");
                        int slen = sql.Length;
                        int n = 0;
                        foreach (DataColumn col in tmp.Columns)
                        {
                            if (r.Table.Columns.Contains(col.ColumnName))
                            {
                                if (col.ColumnName.ToLower() != IndxField.ToLower())
                                {
                                    if (!r.IsNull(col.ColumnName) ||
                                        Data.Columns[col.ColumnName].ExtendedProperties["DateTime"] != null)
                                    {
                                        if (Data.Columns[col.ColumnName].ExtendedProperties["DateTime"] != null &&
                                            Data.Columns[col.ColumnName].ExtendedProperties["DateTime"].ToString() == "System")
                                        {
                                            logsb.Append("[" + col.ColumnName + "] = GetDate(),");
                                            sql.Append("[" + col.ColumnName + "] = GetDate(),");
                                        }
                                        else
                                        {
                                            if (Data.Columns[col.ColumnName].ExtendedProperties["Update"] != null &&
                                                Data.Columns[col.ColumnName].ExtendedProperties["Update"].ToString().ToLower() == "true")
                                            {
                                                logsb.Append("[" + col.ColumnName + "] = '" + r[col.ColumnName].ToString() + "',");
                                                sql.Append("[" + col.ColumnName + "] = @P" + n.ToString() + ",");
                                                sc.Parameters.Add("@P" + n.ToString(), r[col.ColumnName]);
                                                n++;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        if (Data.Columns[col.ColumnName].ExtendedProperties["Update"] != null &&
                                            Data.Columns[col.ColumnName].ExtendedProperties["Update"].ToString().ToLower() == "true")
                                        {
                                            logsb.Append("[" + col.ColumnName + "] = NULL ,");
                                            sql.Append("[" + col.ColumnName + "] = NULL,");
                                        }
                                    }
                                }
                            }
                        }
                        if (sql.Length > slen)
                        {
                            sql.Length = sql.Length - 1;

                            sql.Append(" WHERE [" + IndxField + "] = @Indx");
                            logsb.Append(" Where [" + IndxField + "] = " + key);
                            sc.Parameters.Add("@Indx", key);
                            sc.CommandText = sql.ToString();

                            #region 更改
                            //foreach (System.Data.SqlClient.SqlParameter sp in sc.Parameters)
                            //{
                            //    try
                            //    {
                            //        if (r.Table.Columns[sp.SourceColumn] != null)
                            //        {
                            //            if (sp.SourceColumn != "")
                            //            {
                            //                if (Data.Columns[sp.SourceColumn].ExtendedProperties["DateTime"] != null
                            //                    && Data.Columns[sp.SourceColumn].ExtendedProperties["DateTime"].ToString() == "System")
                            //                {
                            //                    sci.CommandText = sc.CommandText.Replace(sp.ParameterName + ",", "GetDate(),");
                            //                    sp.Value = DBNull.Value;
                            //                }
                            //                else
                            //                    sp.Value = r[sp.SourceColumn];
                            //            }
                            //        }
                            //        else
                            //        {
                            //            sp.Value = DBNull.Value;
                            //        }
                            //    }
                            //    catch
                            //    {
                            //        _ErrMessages += sp.SourceColumn + ":";
                            //    }
                            //    if (sp.SourceColumn != IndxField)
                            //    {
                            //        if (sp.Value != DBNull.Value)
                            //        {
                            //            logsb.Append(sp.SourceColumn);
                            //            logsb.Append(" = '");
                            //            logsb.Append(sp.Value.ToString());
                            //            logsb.Append("',");
                            //        }
                            //        else
                            //        {
                            //            logsb.Append(sp.SourceColumn + " = NULL ,");
                            //        }
                            //    }
                            //    else
                            //    {
                            //        message = " WHERE " + IndxField + " = " + sp.Value.ToString();
                            //    }
                            //}

                            //logsb.Remove(logsb.Length - 1, 1);
                            //logsb.Append(message);
                            #endregion

                            if (SysLogs != null)
                            {
                                running = false;
                                SysLogs.WriteLog(logsb.ToString());
                                running = true;
                            }

                            i = sc.ExecuteNonQuery();
                            sc.Parameters.Clear();
                        }
                        else
                        {
                            i = 0;
                        }
                        #endregion
                    }
                    if (i > 0) rt++;
                    if (OnProgress != null) OnProgress(this, rt);
                }

                if (Filter != "" && delete)
                {
                    rs = Data.Select("Not (" + Filter + ")");
                    foreach (DataRow r in rs)
                    {
                        if (canbreak)
                        {
                            _ErrMessages = "Cancle By User";
                            rt = -1;
                            break;
                        }
                        if (!r.IsNull(IndxField))
                        {
                            scd.Parameters["@Indx"].Value = r[IndxField].ToString();
                            scd.ExecuteNonQuery();
                            rt++;
                        }
                    }
                }
            }
            catch (System.Exception e)
            {
                _ErrMessages = e.Message;
                rt = -1;

            }
            running = false;
            return rt;

        }

        /// <summary>
        /// 更新表，若过滤条件满足的行作更新或插入，不满足的行作删除
        /// 默认主索引为Indx
        /// </summary>
        /// <param name="TableName">数据库表</param>
        /// <param name="Filter">过滤条件</param>
        /// <param name="Data">数据表</param>
        /// <returns>更新或插入的记录行数</returns>
        public int Update(string TableName, string Filter, DataTable Data)
        {
            return Update(TableName, Filter, Data, "Indx");   
        }

        /// <summary>
        /// 按照行集更新数据
        /// </summary>
        /// <remarks>
        /// 行集所在的表需要设置ExtendedProperties确定添加时设置
        /// KeyField 设置添加表中的主键字段
        /// 若列设置ExtendedProperties["SystemDate"] = true
        /// 则使用GetDate()替换参数
        /// 若列设置ExtendedProperties["CanUpdate"] = false
        /// 则不将该列加入更新列表中此方法适用于单纪录更新
        /// </remarks>
        /// <param name="TableName"></param>
        /// <param name="UpdateRows"></param>
        /// <returns></returns>
        public int Update(string TableName, DataRow[] UpdateRows)
        {
            int rt = 0;
            _ErrMessages = "";
            if (UpdateRows.Length > 0)
            {
                DataTable uTable = UpdateRows[0].Table;
                if (uTable != null)
                {
                    if (cn.State != ConnectionState.Open)
                        cn.Open();

                    string KeyField = "";
                    if(uTable.ExtendedProperties["KeyField"] != null)
                        KeyField = uTable.ExtendedProperties["KeyField"].ToString();
                    if (KeyField != "")
                    {
                        ArrayList fields = new ArrayList();
                        #region 设置SQL
                        for (int i = 0; i < uTable.Columns.Count; i++)
                        {
                            bool canUpdate = false;
                            DataColumn col = uTable.Columns[i];
                            if (col.ExtendedProperties["Update"] != null)
                            {
                                canUpdate = (bool)col.ExtendedProperties["Update"];
                            }
                            if (canUpdate)
                            {
                                if (col.ColumnName != KeyField)
                                {
                                    if (col.ExtendedProperties["SystemDate"] != null)
                                        fields.Add("[" + col.ColumnName + "] = GetDate()");
                                    else
                                        fields.Add("[" + col.ColumnName + "] = @P" + i.ToString());
                                }
                            }
                        }
                        #endregion

                        if (fields.Count > 0)
                        {
                            string SQL = "UPDATE " + TableName + " SET " +
                                string.Join(",", (string[])fields.ToArray()) + " WHERE [" +
                                KeyField + "] = @Indx";

                            SqlCommand cmd = new SqlCommand(SQL, cn);
                            cmd.CommandTimeout = _CommandTimeOut;
                            if (tran != null)
                                cmd.Transaction = tran;

                            foreach (DataRow uRow in UpdateRows)
                            {
                                #region 设置数据
                                for (int i = 0; i < uTable.Columns.Count; i++)
                                {
                                    bool canUpdate = true;
                                    DataColumn col = uTable.Columns[i];
                                    if (col.ExtendedProperties["Update"] != null)
                                    {
                                        canUpdate = (bool)col.ExtendedProperties["Update"];
                                    }
                                    if (canUpdate)
                                    {

                                        if (col.ColumnName != KeyField)
                                        {
                                            if (col.ExtendedProperties["SystemDate"] == null)
                                            {
                                                string pName = string.Format("@P{0}", i);
                                                if (cmd.Parameters.Contains(pName))
                                                {
                                                    cmd.Parameters[pName].Value = uRow[i];
                                                }
                                                else
                                                {
                                                    cmd.Parameters.Add(pName, uRow[i]);
                                                }
                                            }
                                        }
                                        else
                                        {
                                            string key = uRow[KeyField].ToString();
                                            if (uRow.Table.ExtendedProperties.Contains("OldKeys"))
                                            {
                                                Hashtable oldkeys = (Hashtable)uRow.Table.ExtendedProperties["OldKeys"];
                                                if(oldkeys.Contains(key))
                                                    key = oldkeys[key].ToString();
                                            }


                                            if (cmd.Parameters.Contains("@Indx"))
                                            {
                                                cmd.Parameters["@Indx"].Value = key;
                                            }
                                            else
                                            {
                                                cmd.Parameters.Add("@Indx", key);
                                            }
                                        }
                                    }
                                }
                                #endregion

                                try
                                {
                                    cmd.ExecuteNonQuery();
                                    rt++;
                                }
                                catch (System.Exception err)
                                {
                                    rt = 0;
                                    _ErrMessages = err.Message;
                                }
                            }
                        }
                        else
                        {
                            _ErrMessages = "没有需要更新的列";
                        }
                    }
                    else
                    {
                        _ErrMessages = "没有主键设置";
                    }
                }
                else
                {
                    _ErrMessages = "行集没有对应内存表";
                }
            }
            return rt;
        }

        #endregion

		#region 删除数据
		/// <summary>
		/// 删除一条纪录默认索引字段为Indx
		/// </summary>
		/// <param name="TableName">用于删除纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <returns>返回是否删除成功</returns>
		public bool Delete(string TableName,DataRow Data)
		{
			bool rt = Delete(TableName,Data,"Indx");
			return rt;
		}

		/// <summary>
		/// 删除一条纪录
		/// </summary>
		/// <param name="TableName">用于删除纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <param name="IndxField">索引字段</param>
		/// <returns>返回是否删除成功</returns>
		public bool Delete(string TableName,DataRow Data,string IndxField)
		{
            _ErrMessages = "";
			string sql = "Delete From " + TableName + " WHERE " + IndxField + " = '" + Data[IndxField].ToString() + "'";
            if (cn.State != ConnectionState.Open)
                cn.Open();
            bool rt = this.ExecuteSql(sql);
            if (SysLogs != null)
                SysLogs.WriteLog(sql);
            return rt;
		}
		#endregion

		#region 执行语句
		/// <summary>
		/// 执行SQL语句
		/// </summary>
		/// <param name="SQL"></param>
		/// <returns></returns>
		public bool ExecuteSql(string SQL)
		{
            _ErrMessages = "";
            while (running)
            {
                System.Threading.Thread.Sleep(100);
            }
            running = true;
            
            if (cn.State != ConnectionState.Open)
                cn.Open();

			SqlCommand cmd = new SqlCommand(SQL,cn);
            cmd.CommandTimeout = _CommandTimeOut;
			if(tran != null)
				cmd.Transaction = tran;
			try
			{
				cmd.ExecuteNonQuery();
                running = false;
                if (SysLogs != null)
                    SysLogs.WriteLog(SQL);
				return true;
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
                running = false;
                if (SysLogs != null)
                    SysLogs.WriteLog(SQL + " ERR : " + _ErrMessages);
                return false;
			}
		}

		/// <summary>
		/// 执行SQL语句
		/// </summary>
		/// <param name="SQL"></param>
		/// <param name="parms">参数集合</param>
		/// <returns></returns>
		public bool ExecuteSql(string SQL, System.Collections.Hashtable parms)
		{
            _ErrMessages = "";
            while (running)
            {
                System.Threading.Thread.Sleep(100);
            }
            running = true;
            
            if (cn.State != ConnectionState.Open)
                cn.Open();

			SqlCommand cmd = new SqlCommand(SQL,cn);
			if(tran != null)
				cmd.Transaction = tran;
			try
			{
                System.Text.StringBuilder logsb = new System.Text.StringBuilder(SQL);
                foreach (System.Collections.DictionaryEntry p in parms)
                {
                    cmd.Parameters.Add(p.Key.ToString(), p.Value);
                    if(p.Value != DBNull.Value)
                        logsb.Append(p.Key.ToString() + " = '" + p.Value.ToString() + "',");
                    else
                        logsb.Append(p.Key.ToString() + " = NULL ,");
                }
				cmd.ExecuteNonQuery();
                running = false;
                if (SysLogs != null)
                    SysLogs.WriteLog(logsb.ToString());
				return true;
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
                running = false;
				return false;
			}
		}

		#endregion

		#region 析构和释放
		/// <summary>
		/// 析构
		/// </summary>
		public void Despose()
		{
			if(tran!=null)tran.Dispose();
            try
            {
                if (cn.State == ConnectionState.Open)
                    cn.Close();
                if (SysLogs.DataAccess != null)
                {
                    SysLogs.DataAccess.Despose();
                }
            }
            catch { }
			cn.Dispose();
		}

		/// <summary>
		/// 释放连接
		/// </summary>
		public void Dispose()
		{
			Despose();
		}
		#endregion

		#region 批处理方法
		/// <summary>
		/// Insert和Update表时处理事件
		/// </summary>
		public event ProgressHandler OnProgress;

		/// <summary>
		/// 强制终止按表添加和更新
		/// </summary>
		public void Break()
		{
			canbreak = true;
		}
		#endregion
        
        /// <summary>
        /// 将列设置为已更改
        /// </summary>
        /// <param name="Col"></param>
        public void UpdateColumn(DataColumn Col,bool canUpdate)
        {
            if (Col.ExtendedProperties.Contains("Update"))
                Col.ExtendedProperties["Update"] = canUpdate ? "True" : "False";
            else
                Col.ExtendedProperties.Add("Update", canUpdate ? "True" : "False");

        }

        /// <summary>
        /// 将当前列设置为系统时间
        /// </summary>
        /// <param name="Col"></param>
        public void SetSystemTime(DataColumn Col)
        {
            if (Col.ExtendedProperties.Contains("DateTime"))
                Col.ExtendedProperties["DateTime"] = "System";
            else
                Col.ExtendedProperties.Add("DateTime", "System");
        }

        #region 添加表设置
        /// <summary>
        /// 设置列为数据库系统时间(OLD)
        /// </summary>
        /// <param name="t">设置的表</param>
        /// <param name="col">设置的列名</param>
        public void SetSysTime(DataTable t, string col)
        {
            if (t.Columns.Contains(col))
                
            {
                if (t.Columns[col].ExtendedProperties.Contains("DateTime"))
                    t.Columns[col].ExtendedProperties["DateTime"] = "System";
                else
                    t.Columns[col].ExtendedProperties.Add("DateTime", "System");
            }
        }

        /// <summary>
        /// 设置表主键字段
        /// </summary>
        /// <param name="t">设置表</param>
        /// <param name="KeyField">值</param>
        public void SetKeyFiled(DataTable t, string KeyField)
        {
            if (t != null)
            {
                if (!t.ExtendedProperties.Contains("KeyField"))

                    t.ExtendedProperties.Add("KeyField", KeyField);
                else
                    t.ExtendedProperties["KeyField"] = KeyField;
            }
        }

        /// <summary>
        /// 设置是否更新主键
        /// </summary>
        /// <param name="t">设置表</param>
        /// <param name="UpdateKey">值</param>
        public void SetUpdateKey(DataTable t, bool UpdateKey)
        {
            if (t != null)
            {
                if (!t.ExtendedProperties.Contains("UpdateKey"))
                    t.ExtendedProperties.Add("UpdateKey", UpdateKey);
                else
                    t.ExtendedProperties["UpdateKey"] = UpdateKey;
            }
        }

        /// <summary>
        /// 这是列为数据库系统时间
        /// </summary>
        /// <param name="col">设置列</param>
        public void SetSystemDate(DataColumn col)
        {
            if (col != null )
            {
                if(col.ExtendedProperties.Contains("SystemDate"))
                    col.ExtendedProperties["SystemDate"] = true;
                else
                    col.ExtendedProperties.Add("SystemDate", true);
            }
        }

        public void RemoveSystemDate(DataColumn col)
        {
            if (col != null)
            {
                if (col.ExtendedProperties.Contains("SystemDate"))
                    col.ExtendedProperties.Remove("SystemDate");
            }
        }


        public void CheckModfyKey(DataTable table, string keyfield)
        {
            DataView vo = new DataView(table, "", "", DataViewRowState.ModifiedOriginal);
            DataView vc = new DataView(table, "", "", DataViewRowState.ModifiedCurrent);
            Hashtable oldkyes = null;
            for (int i = 0; i < vo.Count; i++)
            {
                foreach (DataColumn col in table.Columns)
                {
                    if (i < vc.Count)
                    {
                        #region 设置记录主键的Hashtable
                        if (table.ExtendedProperties.Contains("OldKeys"))
                        {
                            oldkyes = (Hashtable)table.ExtendedProperties["OldKeys"];
                        }
                        else
                        {
                            oldkyes = new Hashtable();
                            table.ExtendedProperties.Add("OldKeys", oldkyes);
                        }
                        #endregion

                        if (vo[i][col.ColumnName].ToString() != vc[i][col.ColumnName].ToString())
                        {
                            #region 设置更新主键
                            if (keyfield == col.ColumnName)
                            {
                                UpdateColumn(col, true);
                                if (oldkyes.Contains(vo[i][col.ColumnName].ToString()))
                                {
                                    oldkyes[vc[i][col.ColumnName].ToString()] = vo[i][col.ColumnName];
                                }
                                else
                                {
                                    oldkyes.Add(vc[i][col.ColumnName].ToString(), vo[i][col.ColumnName]);

                                }
                            }
                            #endregion
                        }
                        else
                        {
                            #region 取消更新主键
                            if (keyfield == col.ColumnName)
                            {
                                UpdateColumn(col, false);
                                if (oldkyes.Contains(vc[i][col.ColumnName].ToString()))
                                {
                                    oldkyes.Remove(vc[i][col.ColumnName].ToString());
                                }
                            }
                            #endregion
                        }

                    }
                }
            }

        }
        #endregion



        public IDataAccess Clone()
        {
            return new SQLDataAccess(cn.ConnectionString);
        }
    }
}
