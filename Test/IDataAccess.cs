using System;
using System.Data;
using System.Xml;

namespace Test
{
	/// <summary>
	/// Insert��Update��ʱ�����¼�ί��
	/// </summary>
	public delegate void ProgressHandler(object sender,int rowsCount);

	/// <summary>
	/// ͨ�����ݷ���
	/// </summary>
	public interface IDataAccess
	{

		/// <summary>
		/// ��ȡ������Ϣ
		/// </summary>
		string ErrMessages{get;}

		/// <summary>
		/// ��ʼһ������
		/// </summary>
		/// <returns></returns>
		bool BeginTrans();

		/// <summary>
		/// �ύ����
		/// </summary>
		/// <returns></returns>
		 bool ComitTrans();

		/// <summary>
		/// ����ع�
		/// </summary>
		/// <returns></returns>
		bool RoolbackTrans();

		/// <summary>
		/// Insert��Update��ʱ�����¼�
		/// </summary>
		event ProgressHandler OnProgress;

		/// <summary>
		/// ��ȡ���ݱ�
		/// </summary>
		/// <param name="SQL">���ڻ�ȡ���ݱ��SQL���</param>
		/// <returns></returns>
		DataTable GetTable(string SQL);

		/// <summary>
		/// ��ȡ���ݱ�
		/// </summary>
		/// <param name="SQL">>���ڻ�ȡ���ݱ��SQL���</param>
		/// <param name="parms">��������</param>
		/// <returns></returns>
		DataTable GetTable(string SQL,System.Collections.Hashtable parms);

		/// <summary>
		/// ����һ����¼Ĭ�������ֶ�ΪIndx
		/// </summary>
		/// <param name="TableName">����������¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <returns>���ص�ǰ��������������</returns>
		string Insert(string TableName,DataRow Data);

		/// <summary>
		/// ����һ����¼
		/// </summary>
		/// <param name="TableName">����������¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <param name="IndxField">�����ֶ�</param>
		/// <returns>���ص�ǰ��������������</returns>
		string Insert(string TableName,DataRow Data,string IndxField);

		/// <summary>
		/// ��������������������
		/// </summary>
		/// <param name="TableName">����������¼�����ݿ����</param>
		/// <param name="Data">���ݱ�</param>
		/// <returns>���ز��������</returns>
		int Insert(string TableName,DataTable Data);
		
		/// <summary>
		/// ����һ����¼Ĭ�������ֶ�ΪIndx
		/// </summary>
		/// <param name="TableName">���ڸ��¼�¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <returns></returns>
		string Update(string TableName,DataRow Data);

		/// <summary>
		/// ����һ����¼
		/// </summary>
		/// <param name="TableName">���ڸ��¼�¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <param name="IndxField">�����ֶ�</param>
		/// <returns></returns>
		string Update(string TableName,DataRow Data,string IndxField);

        /// <summary>
        /// ����һ����¼
        /// </summary>
        /// <param name="TableName">���ڸ��¼�¼�����ݿ����</param>
        /// <param name="Data">���ݼ�¼</param>
        /// <param name="IndxField">�����ֶ�</param>
        /// <param name="CheckFields">���ڼ������һ���Ե��ֶ��б�</param>
        /// <returns></returns>
        string Update(string TableName, DataRow Data, string IndxField, string CheckFields);
        

		/// <summary>
		/// ���������������¶���
		/// </summary>
		/// <param name="TableName">���ڸ��¼�¼�����ݿ����</param>
		/// <param name="Data">���ݱ�</param>
		/// <returns>���ر����µ�����</returns>
		int Update(string TableName,DataTable Data);

		/// <summary>
		/// ���������������¶���
		/// </summary>
		/// <param name="TableName">���ڸ��¼�¼�����ݿ����</param>
		/// <param name="Data">���ݱ�</param>
		/// <param name="IndxField">�������ݵ�������</param>
		/// <returns>���ر����µ�����</returns>
		int Update(string TableName,DataTable Data,string IndxField);


        /// <summary>
        /// ���±�����������������������»���룬�����������ɾ��
        /// </summary>
        /// <param name="TableName">���ݿ��</param>
        /// <param name="Filter">��������</param>
        /// <param name="Data">���ݱ�</param>
        /// <param name="IndxField">������</param>
        /// <returns>���»����ļ�¼����</returns>
        int Update(string TableName, string Filter, DataTable Data, string IndxField);
        
        /// <summary>
        /// ���±�����������������������»����,�������þ����Ƿ�ɾ�����������
        /// </summary>
        /// <param name="TableName">���ݿ��</param>
        /// <param name="Filter">��������</param>
        /// <param name="Data">���ݱ�</param>
        /// <param name="IndxField">������</param>
        /// <param name="delete">�Ƿ�ɾ��δ���˵�</param>
        /// <returns>���»����ļ�¼����</returns>
        int Update(string TableName, string Filter, DataTable Data, string IndxField, bool delete);
        

        /// <summary>
        /// ���±�����������������������»���룬�����������ɾ��
        /// Ĭ��������ΪIndx
        /// </summary>
        /// <param name="TableName">���ݿ��</param>
        /// <param name="Filter">��������</param>
        /// <param name="Data">���ݱ�</param>
        /// <returns>���»����ļ�¼����</returns>
        int Update(string TableName, string Filter, DataTable Data);

		/// <summary>
		/// ɾ��һ����¼Ĭ�������ֶ�ΪIndx
		/// </summary>
		/// <param name="TableName">����ɾ����¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <returns>�����Ƿ�ɾ���ɹ�</returns>
		bool Delete(string TableName,DataRow Data);

		/// <summary>
		/// ɾ��һ����¼
		/// </summary>
		/// <param name="TableName">����ɾ����¼�����ݿ����</param>
		/// <param name="Data">���ݼ�¼</param>
		/// <param name="IndxField">�����ֶ�</param>
		/// <returns>�����Ƿ�ɾ���ɹ�</returns>
		bool Delete(string TableName,DataRow Data,string IndxField);

		/// <summary>
		/// ִ��SQL���
		/// </summary>
		/// <param name="SQL"></param>
		/// <returns></returns>
		bool ExecuteSql(string SQL);

		/// <summary>
		/// ִ��SQL���
		/// </summary>
		/// <param name="SQL"></param>
		/// <param name="parms"></param>
		/// <returns></returns>
		bool ExecuteSql(string SQL,System.Collections.Hashtable parms);

		/// <summary>
		/// ��ȡDataSet
		/// </summary>
		/// <param name="SQL">��ȡDataSet���õ�SQL</param>
		/// <returns></returns>
		DataSet GetDataSet(string SQL);

		/// <summary>
		/// ��ȡDataSet
		/// </summary>
		/// <param name="SQL">��ȡDataSet���õ�SQL</param>
		/// <param name="parms">�������õĲ���</param>
		/// <returns></returns>
		DataSet GetDataSet(string SQL,System.Collections.Hashtable parms);

		/// <summary>
		/// ǿ����ֹ������Ӻ͸���
		/// </summary>
		void Break();

        /// <summary>
        /// д��������
        /// </summary>
        LogType WriteLogType{get ; set;}

		/// <summary>
		/// ����
		/// </summary>
		void Despose();

        /// <summary>
        /// ����
        /// </summary>
        /// <returns></returns>
        IDataAccess Clone();

        /// <summary>
        /// ��������Ϊ�Ѹ���
        /// </summary>
        /// <param name="Col"></param>
        void UpdateColumn(DataColumn Col,bool canUpdate);


        /// <summary>
        /// ����ǰ������Ϊϵͳʱ��
        /// </summary>
        /// <param name="Col"></param>
        void SetSystemTime(DataColumn Col);

        void CheckModfyKey(DataTable table,string keyfield);

        int CommandTimeOut{get ; set;}
	}
}
