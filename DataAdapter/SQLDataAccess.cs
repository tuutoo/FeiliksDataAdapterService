using System;
using System.Data;
using System.Xml;
using System.Data.SqlClient;
using System.Collections;
using System.Text;

namespace FeiliksDataAdapter
{
	/// <summary>
	/// ͨ�����ݷ��� For SQL Server
	/// Create By Duck Lee 2005-4-15
	/// </summary>
	public class SQLDataAccess:IDataAccess ,System.IDisposable
	{
		#region ˽�б���
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
		/// ���캯��
		/// </summary>
        /// <param name="cnString">�����������ӵ��ַ���</param>
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

		#region ��Ϣ������
		/// <summary>
		/// �����ִ���ʱ���ش�����ʾ�ַ���
		/// </summary>
		public string ErrMessages
		{
			get
			{
				return _ErrMessages;
			}
		}

		/// <summary>
        /// ���ص�ǰ��������
		/// </summary>
		public System.Data.SqlClient.SqlConnection ActiveCN
		{
			get 
			{
				return cn;
			}
		}

        /// <summary>
        /// д��������
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

		#region ����
		/// <summary>
		/// ��ʼһ������
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
				_ErrMessages = "�����Ѿ�����";
				return false;
			}
		}

		/// <summary>
		/// �ύ����
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
				_ErrMessages = "���񲻴�����ʹ��BegingTrans";
                return false;
			}
		}

		/// <summary>
		/// ����ع�
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
				_ErrMessages = "���񲻴�����ʹ��BeginTrans";
				return false;
			}
		}
		#endregion

		#region ��ȡ����
		/// <summary>
		/// ��ȡ���ݱ�
		/// </summary>
		/// <param name="SQL">���ڻ�ȡ���ݱ��SQL���</param>
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
		/// ��ȡ���ݱ�
		/// </summary>
		/// <param name="SQL">>���ڻ�ȡ���ݱ��SQL���</param>
		/// <param name="parms">��������</param>
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
		/// ��ȡDataSet
		/// </summary>
		/// <param name="SQL">��ȡDataSet���õ�SQL</param>
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
		/// ��ȡDataSet
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

		#region ��������
		/// <summary>
		/// ����һ����¼Ĭ�������ֶ�ΪIndx
		/// </summary>
		/// <param name="TableName">����������¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <returns>���ص�ǰ��������������</returns>
		public string Insert(string TableName,DataRow Data)
		{
			string indx = Insert(TableName,Data,"Indx");
			return indx;
		}

		/// <summary>
		/// ����һ����¼
		/// </summary>
		/// <param name="TableName">����������¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <param name="IndxField">�����ֶ�</param>
		/// <returns>���ص�ǰ��������������</returns>
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

                #region ��ȡ������
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

                #region �������������ַ���
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
                    #region ��������
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
		/// ��������������������
		/// </summary>
		/// <param name="TableName">����������¼�����ݿ����</param>
		/// <param name="Data">���ݱ�</param>
		/// <returns>���ز��������</returns>
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

                #region ��ȡ������
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
                    #region ��ʼ��
                    sci.Parameters.Clear();
                    sci.CommandText = "";
                    sbfields.Length = 0;
                    sbvalues.Length = 0;
                    values.Length = 0;
                    #endregion

                    #region �������������ַ���
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
                        #region ��������
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
        /// �����м��������
        /// </summary>
        /// <remarks>
        /// �м����ڵı���Ҫ����ExtendedPropertiesȷ�����ʱ����
        /// KeyField ������ӱ��е������ֶ�
        /// UpdateKey �����Ƿ���ͬ����һ����룬����������Ϊtrue
        /// ��UpdateKey=false�����������������ֵ�����������ֶ�
        /// ��������ExtendedProperties["SystemDate"] = true
        /// ��ʹ��GetDate()�滻����
        /// </remarks>
        /// <param name="TableName">������ݵ����ݿ����</param>
        /// <param name="InsertRows">�������ݵ��м�</param>
        /// <returns>������������</returns>
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
                    #region �������
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

                    #region ����SQL
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
                            #region �������
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
                            #region ִ��
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
                    _ErrMessages = "�м�û�ж�Ӧ�ڴ��";
                }
            }
            return rt;
        }

		#endregion

		#region ��������

		/// <summary>
		/// ����һ����¼Ĭ�������ֶ�ΪIndx
		/// </summary>
		/// <param name="TableName">���ڸ��¼�¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
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
		/// ����һ����¼
		/// </summary>
		/// <param name="TableName">���ڸ��¼�¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <param name="IndxField">�����ֶ�</param>
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
                                    #region ϵͳʱ��
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
                                            #region �����и������þ����Ƿ������
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
                                            #region �����ƴ���
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
                                            #region ��ͨ����
                                            if (Data[col.ColumnName].ToString() != cmdTable.Rows[0][col.ColumnName].ToString())
                                                u = true;
                                            #endregion
                                        }
                                        if (u)
                                        {
                                            #region �������
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
                #region ������д
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
		/// ���������������¶���
		/// </summary>
		/// <param name="TableName">���ڸ��¼�¼�����ݿ����</param>
		/// <param name="Data">���ݱ�</param>
		/// <remarks>Ĭ��IndxFieldΪ"Indx"</remarks>
		/// <returns>���ر����µ�����</returns>
		public int Update(string TableName,DataTable Data)
		{
			return Update(TableName,Data,"Indx");
		}
		/// <summary>
		/// ���������������¶���
		/// </summary>
		/// <param name="TableName">���ڸ��¼�¼�����ݿ����</param>
		/// <param name="Data">���ݱ�</param>
		/// <param name="IndxField">���ݵ�����������</param>
		/// <returns>���ر����µ�����</returns>
		public int Update(string TableName,DataTable Data,string IndxField)
		{
            return Update(TableName, "", Data, IndxField);
            #region ���·���
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
            //            #region ��������
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
            //            #region ��������
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
        /// ���±�����������������������»���룬�����������ɾ��
        /// </summary>
        /// <param name="TableName">���ݿ��</param>
        /// <param name="Filter">��������</param>
        /// <param name="Data">���ݱ�</param>
        /// <returns>���»����ļ�¼����</returns>
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
                //���ø�������
                System.Data.SqlClient.SqlCommand sc = cn.CreateCommand();
                sc.CommandTimeout = _CommandTimeOut;
                //������������
                System.Data.SqlClient.SqlCommand sci = cn.CreateCommand();
                sci.CommandTimeout = _CommandTimeOut;
                //����ɾ������
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
                        #region ��������

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

                            #region �Ķ�
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
                        #region ��������

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

                            #region ����
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
        /// ���±�����������������������»���룬�����������ɾ��
        /// Ĭ��������ΪIndx
        /// </summary>
        /// <param name="TableName">���ݿ��</param>
        /// <param name="Filter">��������</param>
        /// <param name="Data">���ݱ�</param>
        /// <returns>���»����ļ�¼����</returns>
        public int Update(string TableName, string Filter, DataTable Data)
        {
            return Update(TableName, Filter, Data, "Indx");   
        }

        /// <summary>
        /// �����м���������
        /// </summary>
        /// <remarks>
        /// �м����ڵı���Ҫ����ExtendedPropertiesȷ�����ʱ����
        /// KeyField ������ӱ��е������ֶ�
        /// ��������ExtendedProperties["SystemDate"] = true
        /// ��ʹ��GetDate()�滻����
        /// ��������ExtendedProperties["CanUpdate"] = false
        /// �򲻽����м�������б��д˷��������ڵ���¼����
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
                        #region ����SQL
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
                                #region ��������
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
                            _ErrMessages = "û����Ҫ���µ���";
                        }
                    }
                    else
                    {
                        _ErrMessages = "û����������";
                    }
                }
                else
                {
                    _ErrMessages = "�м�û�ж�Ӧ�ڴ��";
                }
            }
            return rt;
        }

        #endregion

		#region ɾ������
		/// <summary>
		/// ɾ��һ����¼Ĭ�������ֶ�ΪIndx
		/// </summary>
		/// <param name="TableName">����ɾ����¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <returns>�����Ƿ�ɾ���ɹ�</returns>
		public bool Delete(string TableName,DataRow Data)
		{
			bool rt = Delete(TableName,Data,"Indx");
			return rt;
		}

		/// <summary>
		/// ɾ��һ����¼
		/// </summary>
		/// <param name="TableName">����ɾ����¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <param name="IndxField">�����ֶ�</param>
		/// <returns>�����Ƿ�ɾ���ɹ�</returns>
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

		#region ִ�����
		/// <summary>
		/// ִ��SQL���
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
		/// ִ��SQL���
		/// </summary>
		/// <param name="SQL"></param>
		/// <param name="parms">��������</param>
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

		#region �������ͷ�
		/// <summary>
		/// ����
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
		/// �ͷ�����
		/// </summary>
		public void Dispose()
		{
			Despose();
		}
		#endregion

		#region ��������
		/// <summary>
		/// Insert��Update��ʱ�����¼�
		/// </summary>
		public event ProgressHandler OnProgress;

		/// <summary>
		/// ǿ����ֹ������Ӻ͸���
		/// </summary>
		public void Break()
		{
			canbreak = true;
		}
		#endregion
        
        /// <summary>
        /// ��������Ϊ�Ѹ���
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
        /// ����ǰ������Ϊϵͳʱ��
        /// </summary>
        /// <param name="Col"></param>
        public void SetSystemTime(DataColumn Col)
        {
            if (Col.ExtendedProperties.Contains("DateTime"))
                Col.ExtendedProperties["DateTime"] = "System";
            else
                Col.ExtendedProperties.Add("DateTime", "System");
        }

        #region ��ӱ�����
        /// <summary>
        /// ������Ϊ���ݿ�ϵͳʱ��(OLD)
        /// </summary>
        /// <param name="t">���õı�</param>
        /// <param name="col">���õ�����</param>
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
        /// ���ñ������ֶ�
        /// </summary>
        /// <param name="t">���ñ�</param>
        /// <param name="KeyField">ֵ</param>
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
        /// �����Ƿ��������
        /// </summary>
        /// <param name="t">���ñ�</param>
        /// <param name="UpdateKey">ֵ</param>
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
        /// ������Ϊ���ݿ�ϵͳʱ��
        /// </summary>
        /// <param name="col">������</param>
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
                        #region ���ü�¼������Hashtable
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
                            #region ���ø�������
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
                            #region ȡ����������
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
