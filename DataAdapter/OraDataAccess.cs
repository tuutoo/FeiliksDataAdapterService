/*DataAccess For Oracle
 * 
 * 
 */
using System;
using System.Data;
using System.Xml;
using System.Collections;

namespace FeiliksDataAdapter
{
	/// <summary>
	/// 通用数据访问
	/// </summary>
	public class OraDataAccess:IDataAccess ,System.IDisposable
	{
		private System.Data.OracleClient.OracleConnection cn;
		private System.Data.OracleClient.OracleTransaction tran;
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
		public OraDataAccess(string cnString)
		{
			_ErrMessages = "";
			try
			{
				cn = new System.Data.OracleClient.OracleConnection(cnString);
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
		public System.Data.OracleClient.OracleConnection ActiveCN
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
				System.Data.OracleClient.OracleDataAdapter sda = new System.Data.OracleClient.OracleDataAdapter(SQL,cn);
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
		/// <param name="SQL">>用于获取数据表的SQL语句</param>
		/// <param name="parms">参数集合</param>
		/// <returns></returns>
		public DataTable GetTable(string SQL, System.Collections.Hashtable parms)
		{
			DataTable rtTable = new DataTable();
			try
			{
				System.Data.OracleClient.OracleDataAdapter sda = new System.Data.OracleClient.OracleDataAdapter(SQL,cn);
				if(tran!=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
				foreach(System.Collections.DictionaryEntry  p in parms)
				{
					sda.SelectCommand.Parameters.Add(p.Key.ToString(),p.Value);
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
				System.Data.OracleClient.OracleDataAdapter sda = new System.Data.OracleClient.OracleDataAdapter("Select * From " + TableName ,cn);
				if(tran !=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
				System.Data.OracleClient.OracleCommandBuilder scb = new System.Data.OracleClient.OracleCommandBuilder(sda);
				System.Data.OracleClient.OracleCommand sc = scb.GetInsertCommand();
				foreach(System.Data.OracleClient.OracleParameter sp in sc.Parameters)
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
									if(sp.OracleType == System.Data.OracleClient.OracleType.Clob)
									{
										System.Data.OracleClient.OracleBinary v = new System.Data.OracleClient.OracleBinary((byte[])Data[sp.SourceColumn]);
										sp.Value = v;
									}
									else
									{
										if(Data[sp.SourceColumn].Equals(""))
											sp.Value = DBNull.Value;
									}
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
				System.Data.OracleClient.OracleString rowid = new System.Data.OracleClient.OracleString("");
				int i = 0;
				try
				{
					i = sc.ExecuteOracleNonQuery(out rowid);
				}
				catch(System.Data.OracleClient.OracleException orerr)
				{
					_ErrMessages += orerr.Message;
				}
				if(i>0)
				{
					string sql = "SELECT " + IndxField.ToUpper() + " FROM " + TableName.ToUpper() + " WHERE ROWID = '" + rowid.Value + "'";
					sc.Dispose();
					sc = new System.Data.OracleClient.OracleCommand(sql,cn);
					object rt = sc.ExecuteOracleScalar();
					if(rt!=null)
						return rt.ToString();
					else
						return "";
				}
				else
				{
					return "";
				}
			}
			catch(System.Exception e)
			{
				_ErrMessages += e.Message;
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
			int i = 0;
			try
			{
				
				System.Data.OracleClient.OracleDataAdapter sda = new System.Data.OracleClient.OracleDataAdapter("Select * From " + TableName ,cn);
				if(tran !=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
				System.Data.OracleClient.OracleCommandBuilder scb = new System.Data.OracleClient.OracleCommandBuilder(sda);
				System.Data.OracleClient.OracleCommand sc = scb.GetInsertCommand();
				foreach(System.Data.DataRow r in Data.Rows)
				{
					foreach(System.Data.OracleClient.OracleParameter sp in sc.Parameters)
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
										if(sp.OracleType == System.Data.OracleClient.OracleType.Clob)
										{
											System.Data.OracleClient.OracleBinary v = new System.Data.OracleClient.OracleBinary((byte[])r[sp.SourceColumn]);
											sp.Value = v;
										}
										else
										{
											if(r[sp.SourceColumn].Equals(""))
												sp.Value = DBNull.Value;
											else
												sp.Value = r[sp.SourceColumn];
										}
									}
								}
								else
								{
									sp.Value=DBNull.Value;
								}
							}
							catch
							{
								_ErrMessages += sp.SourceColumn + ":";
							}
						}
					}
					try
					{
						i = sc.ExecuteNonQuery();
					}
					catch(System.Data.OracleClient.OracleException oraerr)
					{
						_ErrMessages += oraerr.Message;
					}
					if(i>0)rt ++;
					if(OnProgress!=null)OnProgress(this,rt);
					if(canbreak)
					{
						_ErrMessages = "Cancle By User";
						break;
					}

				}
			}
			catch(System.Exception err)
			{
				_ErrMessages += err.Message;
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
			return Update(TableName,Data,"Indx");
		}

		/// <summary>
		/// 按照整个表做更新动作
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据表</param>
		/// <param name="IndxField">更新表的索引列</param>
		/// <returns>返回被更新的行数</returns>
		public int Update(string TableName,DataTable Data,string IndxField)
		{
			int rt = 0;
			int i = 0;
			_ErrMessages = "";
			try
			{
				System.Data.OracleClient.OracleDataAdapter sda = new System.Data.OracleClient.OracleDataAdapter("Select * From " + TableName,cn);
				if(tran!=null)
					sda.SelectCommand.Transaction = tran;

				System.Data.OracleClient.OracleCommandBuilder scb = new System.Data.OracleClient.OracleCommandBuilder(sda);
				System.Data.OracleClient.OracleCommand sc = scb.GetUpdateCommand();

				int pos = sc.CommandText.IndexOf("WHERE");
				if( pos > 0)
					sc.CommandText = sc.CommandText.Substring(0,pos);
				sc.CommandText += " WHERE \"" + IndxField + "\" = @" + IndxField;

				if(!sc.Parameters.Contains("@" + IndxField))
					sc.Parameters.Add("@" + IndxField,System.Data.OracleClient.OracleType.Int32);

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
				System.Data.OracleClient.OracleDataAdapter sda = new System.Data.OracleClient.OracleDataAdapter("Select * From " + TableName,cn);
				if(tran!=null)
					sda.SelectCommand.Transaction = tran;

				System.Data.OracleClient.OracleCommandBuilder scb = new System.Data.OracleClient.OracleCommandBuilder(sda);
				System.Data.OracleClient.OracleCommand sc = scb.GetUpdateCommand();
				
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
			string sql = "Delete From " + TableName + " WHERE \"" + IndxField + "\" = '" + Data[IndxField].ToString() + "'";
			return this.ExecuteSql(sql);
		}


		/// <summary>
		/// 析构
		/// </summary>
		public void Despose()
		{
			if(tran!=null)tran.Dispose();
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
		/// 执行事务
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
					tran.Dispose();
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
					tran.Dispose();
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
		/// 执行SQL
		/// </summary>
		/// <param name="SQL"></param>
		/// <returns></returns>
		public bool ExecuteSql(string SQL)
		{
			System.Data.OracleClient.OracleCommand  cmd = new System.Data.OracleClient.OracleCommand(SQL,cn);
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
		/// <param name="SQL"></param>
		/// <param name="parms"></param>
		/// <returns></returns>
		public bool ExecuteSql(string SQL, System.Collections.Hashtable parms)
		{
			System.Data.OracleClient.OracleCommand  cmd = new System.Data.OracleClient.OracleCommand(SQL,cn);
			if(tran != null)
				cmd.Transaction = tran;
			try
			{
				foreach(System.Collections.DictionaryEntry p in parms)
                    cmd.Parameters.Add(p.Key.ToString(), p.Value);
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
		/// 获取DataSet
		/// </summary>
		/// <param name="SQL"></param>
		/// <returns></returns>
		public DataSet GetDataSet(string SQL)
		{
			
			DataSet rt = new DataSet();
			if(SQL!=null && SQL != "")
			{
				try
				{
					System.Data.OracleClient.OracleCommand ocmd = new System.Data.OracleClient.OracleCommand();
					ocmd.Connection = cn;
					System.Data.OracleClient.OracleDataAdapter sda = new System.Data.OracleClient.OracleDataAdapter();
					sda.SelectCommand = ocmd;
					if(tran!=null)
					{
						ocmd.Transaction = tran;
					}
					string[] cmds = SQL.Split(';');
					for(int i = 0;i<cmds.Length ; i++)
					{
						if(cmds[i] != "")
						{
							ocmd.CommandText = cmds[i];
							sda.Fill(rt);
							rt.Tables[i].TableName = "Table" + i.ToString();
						}
					}
				}
				catch(System.Exception err)
				{
					_ErrMessages = err.Message;
					rt = null;
				}
			}
			return rt;

		}

		/// <summary>
		/// 获取DataSet
		/// </summary>
		/// <param name="SQL"></param>
		/// <param name="parms"></param>
		/// <returns></returns>
		public DataSet GetDataSet(string SQL, System.Collections.Hashtable parms)
		{
			DataSet rt = new DataSet();
			try
			{
				System.Data.OracleClient.OracleDataAdapter sda = new System.Data.OracleClient.OracleDataAdapter(SQL,cn);
				if(tran!=null)
				{
					sda.SelectCommand.Transaction = tran;
				}
				foreach(System.Collections.DictionaryEntry  p in parms)
                    sda.SelectCommand.Parameters.Add(p.Key.ToString(), p.Value);
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
		/// 释放连接
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
            return new OraDataAccess(cn.ConnectionString);
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
