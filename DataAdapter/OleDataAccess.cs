using System;
using System.Data;
using System.Xml;
using System.Collections;

namespace FeiliksDataAdapter
{
	/// <summary>
	/// 通用数据访问
	/// </summary>
	public class OleDataAccess:IDataAccess,System.IDisposable
	{
		private System.Data.OleDb.OleDbConnection cn;
		private System.Data.OleDb.OleDbTransaction tran;
		private string _ErrMessages;
		private bool canbreak = false;
        private LogType _logType;
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
		public OleDataAccess(string cnString)
		{
			_ErrMessages = "";
			try
			{
				cn = new System.Data.OleDb.OleDbConnection(cnString);
				cn.Open();
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
			}
		}

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
		public System.Data.OleDb.OleDbConnection ActiveCN
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
            }
        }

		/// <summary>
		/// 获取数据表
		/// </summary>
		/// <param name="SQL">用于获取数据表的SQL语句</param>
		/// <returns></returns>
		public DataTable GetTable(string SQL)
		{
			DataTable rtTable = new DataTable();
			try
			{
				System.Data.OleDb.OleDbDataAdapter sda = new System.Data.OleDb.OleDbDataAdapter(SQL,cn);
				if(tran!=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
				sda.Fill(rtTable);
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
				rtTable = null;
			}
			return rtTable;
		}

		/// <summary>
		/// 获取数据表
		/// </summary>
		/// <param name="SQL">用于获取数据表的SQL语句</param>
		/// <param name="parms"></param>
		/// <returns></returns>
		public DataTable GetTable(string SQL,System.Collections.Hashtable parms)
		{
			DataTable rtTable = new DataTable();
			try
			{
				System.Data.OleDb.OleDbDataAdapter sda = new System.Data.OleDb.OleDbDataAdapter(SQL,cn);
				foreach(System.Collections.DictionaryEntry p in parms)
				{
					sda.SelectCommand.Parameters.Add(p.Key.ToString() ,p.Value);
				}
				if(tran!=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
				sda.Fill(rtTable);
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
				rtTable = null;
			}
			return rtTable;
		}


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
			try
			{
				System.Data.OleDb.OleDbDataAdapter sda = new System.Data.OleDb.OleDbDataAdapter("Select * From " + TableName ,cn);
				if(tran !=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
				System.Data.OleDb.OleDbCommandBuilder scb = new System.Data.OleDb.OleDbCommandBuilder(sda);
				System.Data.OleDb.OleDbCommand sc = scb.GetInsertCommand();
				foreach(System.Data.OleDb.OleDbParameter sp in sc.Parameters)
				{
					if(sp.SourceColumn != "")
					{
						try
						{
							if(Data.Table.Columns[sp.SourceColumn]!=null)
							{
								if(Data[sp.SourceColumn] is DBNull)
								{
									sp.Value = Data.Table.Columns[sp.SourceColumn].DefaultValue;
								}
								else
								{
									sp.Value = Data[sp.SourceColumn];
								}
							}
							else
							{
								sp.Value = DBNull.Value;
							}
						}
						catch
						{
							_ErrMessages += sp.SourceColumn + ":";
						}
					}
				}
				sc.CommandText += ";Select Max(" + IndxField + ") From " + TableName ;
				object rt = sc.ExecuteScalar();
				if(rt!=null)
					return rt.ToString();
				else
					return "";
			}
			catch(System.Exception e)
			{
				_ErrMessages = e.Message;
				return "";
			}
		}
		
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

		/// <summary>
		/// 更新一条纪录
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <param name="IndxField">索引字段</param>
		/// <returns></returns>
		public string Update(string TableName,DataRow Data,string IndxField)
		{
			_ErrMessages = "";
			try
			{
				System.Data.OleDb.OleDbDataAdapter sda = new System.Data.OleDb.OleDbDataAdapter("Select * From " + TableName,cn);
				if(tran!=null)
					sda.SelectCommand.Transaction = tran;

				System.Data.OleDb.OleDbCommandBuilder scb = new System.Data.OleDb.OleDbCommandBuilder(sda);
				System.Data.OleDb.OleDbCommand sc = scb.GetUpdateCommand();
				foreach(System.Data.SqlClient.SqlParameter sp in sc.Parameters)
				{
					if(sp.SourceColumn != "")
					{
						try
						{
							if(Data.Table.Columns[sp.SourceColumn]!=null)
							{
								sp.Value = Data[sp.SourceColumn];
							}
							else
							{
								sp.Value = DBNull.Value;
							}
						}
						catch
						{
							_ErrMessages += sp.SourceColumn + ":";
						}
					}
				}
				int pos = sc.CommandText.IndexOf("WHERE");
				if( pos > 0)
					sc.CommandText = sc.CommandText.Substring(0,pos);
				sc.CommandText += " WHERE " + IndxField + " = '" + Data[IndxField].ToString() + "'";
				sc.ExecuteNonQuery();
				return Data[IndxField].ToString();
			}
			catch(System.Exception e)
			{
				_ErrMessages = e.Message;
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
			int rt = 0;
			int i = 0;
			_ErrMessages = "";
			try
			{
				System.Data.OleDb.OleDbDataAdapter sda = new System.Data.OleDb.OleDbDataAdapter("Select * From " + TableName ,cn);
				if(tran !=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
				System.Data.OleDb.OleDbCommandBuilder scb = new System.Data.OleDb.OleDbCommandBuilder(sda);
				System.Data.OleDb.OleDbCommand sc = scb.GetInsertCommand();
				foreach(System.Data.DataRow r in Data.Rows)
				{
					foreach(System.Data.OleDb.OleDbParameter sp in sc.Parameters)
					{
						if(sp.SourceColumn != "")
						{
							try
							{
								if(r.Table.Columns[sp.SourceColumn]!=null)
								{
									if(r[sp.SourceColumn] is DBNull)
									{
										sp.Value = Data.Columns[sp.SourceColumn].DefaultValue;
									}
									else
									{
										sp.Value = r[sp.SourceColumn];
									}
								}
								else
								{
									sp.Value = DBNull.Value;
								}
							}
							catch
							{
								_ErrMessages += sp.SourceColumn + ":";
							}
						}
					}
					i = sc.ExecuteNonQuery();
					if(i>0)rt++;
					if(OnProgress!=null)OnProgress(this,rt);
					if(canbreak)
					{
						_ErrMessages = "Cancle By User";
						break;
					}

				}
			}
			catch(System.Exception e)
			{
				_ErrMessages = e.Message;
				rt = -1;
			}
			return rt;
		}


		/// <summary>
		/// 按照整个表做更新动作
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据表</param>
		/// <returns>返回被更新的行数</returns>
		public int Update(string TableName,DataTable Data)
		{
			return Update(TableName,Data);
		}

		/// <summary>
		/// 按照整个表做更新动作
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据表</param>
		/// <param name="IndxField">数据的索引列</param>
		/// <returns>返回被更新的行数</returns>
		public int Update(string TableName,DataTable Data,string IndxField)
		{
			int rt = 0;
			int i = 0;
			_ErrMessages = "";
			try
			{
				System.Data.OleDb.OleDbDataAdapter sda = new System.Data.OleDb.OleDbDataAdapter("Select * From " + TableName,cn);
				if(tran!=null)
					sda.SelectCommand.Transaction = tran;

				System.Data.OleDb.OleDbCommandBuilder scb = new System.Data.OleDb.OleDbCommandBuilder(sda);
				System.Data.OleDb.OleDbCommand sc = scb.GetUpdateCommand();
				
				int pos = sc.CommandText.IndexOf("WHERE");
				if( pos > 0)
					sc.CommandText = sc.CommandText.Substring(0,pos);
				sc.CommandText += " WHERE " + IndxField + " = @" + IndxField ;
				if(!sc.Parameters.Contains("@" + IndxField))
					sc.Parameters.Add("@" + IndxField,System.Data.OleDb.OleDbType.BigInt);

				foreach(System.Data.DataRow r in Data.Rows)
				{
					foreach(System.Data.SqlClient.SqlParameter sp in sc.Parameters)
					{
						if(sp.SourceColumn != "")
						{
							try
							{
								if(r.Table.Columns[sp.SourceColumn]!=null)
								{
									sp.Value = r[sp.SourceColumn];
								}
								else
								{
									sp.Value = DBNull.Value;
								}
							}
							catch
							{
								_ErrMessages += sp.SourceColumn + ":";
							}
						}
					}
					i = sc.ExecuteNonQuery();
					if(i>0)rt++;
					if(OnProgress!=null)OnProgress(this,rt);
					if(canbreak)
					{
						_ErrMessages = "Cancle By User";
						break;
					}
				}
			}
			catch(System.Exception e)
			{
				_ErrMessages = e.Message;
				rt = -1;
			}

			return rt;
		}

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
			string sql = "Delete From " + TableName + " WHERE " + IndxField + " = '" + Data[IndxField].ToString() + "'";
			return this.ExecuteSql(sql);
		}


		/// <summary>
		/// 析构
		/// </summary>
		public void Despose()
		{
			cn.Close();
			cn.Dispose();
		}

		/// <summary>
		/// 强制终止按表添加和更新
		/// </summary>
		public void Break()
		{
			canbreak = true;
		}

		/// <summary>
		/// Insert和Update表时处理事件
		/// </summary>
		public event ProgressHandler OnProgress;

		#region IDataAccess 成员

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
			if(tran != null)
			{
				try
				{
					tran.Commit();
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
			if(tran != null)
			{
				try
				{
					tran.Rollback();
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

		/// <summary>
		/// 执行SQL语句
		/// </summary>
		/// <param name="SQL">需要执行的SQL</param>
		/// <returns>Bool值返回是否执行成功</returns>
		public bool ExecuteSql(string SQL)
		{
			System.Data.OleDb.OleDbCommand  cmd = new System.Data.OleDb.OleDbCommand(SQL,cn);
			if(tran != null)
				cmd.Transaction = tran;
			try
			{
				cmd.ExecuteNonQuery();
				return true;
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
				return false;
			}
		}

		/// <summary>
		/// 执行SQL语句
		/// </summary>
		/// <param name="SQL">需要执行的SQL</param>
		/// <param name="parms">参数</param>
		/// <returns></returns>
		public bool ExecuteSql(string SQL, System.Collections.Hashtable parms)
		{
			System.Data.OleDb.OleDbCommand  cmd = new System.Data.OleDb.OleDbCommand(SQL,cn);
			if(tran != null)
				cmd.Transaction = tran;
			try
			{
				foreach(System.Collections.DictionaryEntry p in parms)
					cmd.Parameters.Add(p.Key.ToString(),p.Value);
				cmd.ExecuteNonQuery();
				return true;
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
				return false;
			}
		}

		/// <summary>
		/// 返回一个DataSet
		/// </summary>
		/// <param name="SQL"></param>
		/// <returns></returns>
		public DataSet GetDataSet(string SQL)
		{
			DataSet rt = new DataSet();
			try
			{
				System.Data.OleDb.OleDbDataAdapter sda = new System.Data.OleDb.OleDbDataAdapter(SQL,cn);
				if(tran!=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
				sda.Fill(rt);
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
				rt = null;
			}
			return rt;

		}

		/// <summary>
		/// 返回一个DataSet
		/// </summary>
		/// <param name="SQL"></param>
		/// <param name="parms"></param>
		/// <returns></returns>
		public DataSet GetDataSet(string SQL, System.Collections.Hashtable parms)
		{
			DataSet rt = new DataSet();
			try
			{
				System.Data.OleDb.OleDbDataAdapter sda = new System.Data.OleDb.OleDbDataAdapter(SQL,cn);
				if(tran!=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
				foreach(System.Collections.DictionaryEntry  p in parms)
					sda.SelectCommand.Parameters.Add(p.Key.ToString(),p.Value);
				sda.Fill(rt);
			}
			catch(System.Exception err)
			{
				_ErrMessages = err.Message;
				rt = null;
			}
			return rt;
		}

		#endregion

		#region IDisposable 成员

		/// <summary>
		/// 析构，释放资源和连接
		/// </summary>
		public void Dispose()
		{
			Despose();
		}

		#endregion



        #region IDataAccess 成员


        public int Update(string TableName, string Filter, DataTable Data, string IndxField)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public int Update(string TableName, string Filter, DataTable Data)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public IDataAccess Clone()
        {
            return new OleDataAccess(cn.ConnectionString);
        }
        public int Update(string TableName, string Filter, DataTable Data, string IndxField, bool delete)
        {
            throw new Exception("The method or operation is not implemented.");
        }
        public string Update(string TableName, DataRow Data, string IndxField, string CheckFields)
        {
            throw new Exception("The method or operation is not implemented.");
        }
        /// <summary>
        /// 将列设置为已更改
        /// </summary>
        /// <param name="Col"></param>
        public void UpdateColumn(DataColumn Col, bool canUpdate)
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
                            UpdateColumn(col, true);
                            if (keyfield == col.ColumnName)
                            {
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
                            UpdateColumn(col, false);
                            if (keyfield == col.ColumnName)
                            {
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
    }
}
