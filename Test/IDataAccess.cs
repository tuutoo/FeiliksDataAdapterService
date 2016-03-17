using System;
using System.Data;
using System.Xml;

namespace Test
{
	/// <summary>
	/// Insert和Update表时处理事件委托
	/// </summary>
	public delegate void ProgressHandler(object sender,int rowsCount);

	/// <summary>
	/// 通用数据访问
	/// </summary>
	public interface IDataAccess
	{

		/// <summary>
		/// 获取错误信息
		/// </summary>
		string ErrMessages{get;}

		/// <summary>
		/// 开始一个事务
		/// </summary>
		/// <returns></returns>
		bool BeginTrans();

		/// <summary>
		/// 提交事务
		/// </summary>
		/// <returns></returns>
		 bool ComitTrans();

		/// <summary>
		/// 事务回滚
		/// </summary>
		/// <returns></returns>
		bool RoolbackTrans();

		/// <summary>
		/// Insert和Update表时处理事件
		/// </summary>
		event ProgressHandler OnProgress;

		/// <summary>
		/// 获取数据表
		/// </summary>
		/// <param name="SQL">用于获取数据表的SQL语句</param>
		/// <returns></returns>
		DataTable GetTable(string SQL);

		/// <summary>
		/// 获取数据表
		/// </summary>
		/// <param name="SQL">>用于获取数据表的SQL语句</param>
		/// <param name="parms">参数集合</param>
		/// <returns></returns>
		DataTable GetTable(string SQL,System.Collections.Hashtable parms);

		/// <summary>
		/// 新增一条纪录默认索引字段为Indx
		/// </summary>
		/// <param name="TableName">用于新增纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <returns>返回当前新增的数据索引</returns>
		string Insert(string TableName,DataRow Data);

		/// <summary>
		/// 新增一条纪录
		/// </summary>
		/// <param name="TableName">用于新增纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <param name="IndxField">索引字段</param>
		/// <returns>返回当前新增的数据索引</returns>
		string Insert(string TableName,DataRow Data,string IndxField);

		/// <summary>
		/// 按照整个表做新增动作
		/// </summary>
		/// <param name="TableName">用于新增纪录的数据库表名</param>
		/// <param name="Data">数据表</param>
		/// <returns>返回插入的行数</returns>
		int Insert(string TableName,DataTable Data);
		
		/// <summary>
		/// 更新一条纪录默认索引字段为Indx
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <returns></returns>
		string Update(string TableName,DataRow Data);

		/// <summary>
		/// 更新一条纪录
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <param name="IndxField">索引字段</param>
		/// <returns></returns>
		string Update(string TableName,DataRow Data,string IndxField);

        /// <summary>
        /// 更新一条纪录
        /// </summary>
        /// <param name="TableName">用于更新纪录的数据库表名</param>
        /// <param name="Data">数据纪录</param>
        /// <param name="IndxField">索引字段</param>
        /// <param name="CheckFields">用于检查数据一致性的字段列表</param>
        /// <returns></returns>
        string Update(string TableName, DataRow Data, string IndxField, string CheckFields);
        

		/// <summary>
		/// 按照整个表做更新动作
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据表</param>
		/// <returns>返回被更新的行数</returns>
		int Update(string TableName,DataTable Data);

		/// <summary>
		/// 按照整个表做更新动作
		/// </summary>
		/// <param name="TableName">用于更新纪录的数据库表名</param>
		/// <param name="Data">数据表</param>
		/// <param name="IndxField">更新数据的索引列</param>
		/// <returns>返回被更新的行数</returns>
		int Update(string TableName,DataTable Data,string IndxField);


        /// <summary>
        /// 更新表，若过滤条件满足的行作更新或插入，不满足的行作删除
        /// </summary>
        /// <param name="TableName">数据库表</param>
        /// <param name="Filter">过滤条件</param>
        /// <param name="Data">数据表</param>
        /// <param name="IndxField">主索引</param>
        /// <returns>更新或插入的记录行数</returns>
        int Update(string TableName, string Filter, DataTable Data, string IndxField);
        
        /// <summary>
        /// 更新表，若过滤条件满足的行作更新或插入,根据设置决定是否删除不满足的行
        /// </summary>
        /// <param name="TableName">数据库表</param>
        /// <param name="Filter">过滤条件</param>
        /// <param name="Data">数据表</param>
        /// <param name="IndxField">主索引</param>
        /// <param name="delete">是否删除未过滤的</param>
        /// <returns>更新或插入的记录行数</returns>
        int Update(string TableName, string Filter, DataTable Data, string IndxField, bool delete);
        

        /// <summary>
        /// 更新表，若过滤条件满足的行作更新或插入，不满足的行作删除
        /// 默认主索引为Indx
        /// </summary>
        /// <param name="TableName">数据库表</param>
        /// <param name="Filter">过滤条件</param>
        /// <param name="Data">数据表</param>
        /// <returns>更新或插入的记录行数</returns>
        int Update(string TableName, string Filter, DataTable Data);

		/// <summary>
		/// 删除一条纪录默认索引字段为Indx
		/// </summary>
		/// <param name="TableName">用于删除纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <returns>返回是否删除成功</returns>
		bool Delete(string TableName,DataRow Data);

		/// <summary>
		/// 删除一条纪录
		/// </summary>
		/// <param name="TableName">用于删除纪录的数据库表名</param>
		/// <param name="Data">数据纪录</param>
		/// <param name="IndxField">索引字段</param>
		/// <returns>返回是否删除成功</returns>
		bool Delete(string TableName,DataRow Data,string IndxField);

		/// <summary>
		/// 执行SQL语句
		/// </summary>
		/// <param name="SQL"></param>
		/// <returns></returns>
		bool ExecuteSql(string SQL);

		/// <summary>
		/// 执行SQL语句
		/// </summary>
		/// <param name="SQL"></param>
		/// <param name="parms"></param>
		/// <returns></returns>
		bool ExecuteSql(string SQL,System.Collections.Hashtable parms);

		/// <summary>
		/// 获取DataSet
		/// </summary>
		/// <param name="SQL">获取DataSet所用的SQL</param>
		/// <returns></returns>
		DataSet GetDataSet(string SQL);

		/// <summary>
		/// 获取DataSet
		/// </summary>
		/// <param name="SQL">获取DataSet所用的SQL</param>
		/// <param name="parms">命令所用的参数</param>
		/// <returns></returns>
		DataSet GetDataSet(string SQL,System.Collections.Hashtable parms);

		/// <summary>
		/// 强制终止按表添加和更新
		/// </summary>
		void Break();

        /// <summary>
        /// 写日至类型
        /// </summary>
        LogType WriteLogType{get ; set;}

		/// <summary>
		/// 析构
		/// </summary>
		void Despose();

        /// <summary>
        /// 复制
        /// </summary>
        /// <returns></returns>
        IDataAccess Clone();

        /// <summary>
        /// 将列设置为已更改
        /// </summary>
        /// <param name="Col"></param>
        void UpdateColumn(DataColumn Col,bool canUpdate);


        /// <summary>
        /// 将当前列设置为系统时间
        /// </summary>
        /// <param name="Col"></param>
        void SetSystemTime(DataColumn Col);

        void CheckModfyKey(DataTable table,string keyfield);

        int CommandTimeOut{get ; set;}
	}
}
