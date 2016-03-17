using System;
using System.Text;
using System.Collections;
using System.Data;

namespace Test
{
    #region SQL处理器
    public class SQLProcer
    {
        private Hashtable parm;
        private Hashtable evns;
        private string _SQL;
        private DataSet datas;
        public string SQL
        {
            get
            {
                return _SQL;
            }
        }

        public SQLProcer(string sql, Hashtable parms, Hashtable evn, DataSet data)
        {
            parm = parms;
            evns = evn;
            datas = data;
            _SQL = System.Text.RegularExpressions.Regex.Replace(sql, "@([a-zA-Z0-9_\\u4e00-\\u9fa5]*)\\.([a-zA-Z0-9_\\u4e00-\\u9fa5]*)", new System.Text.RegularExpressions.MatchEvaluator(Replace));
            _SQL = System.Text.RegularExpressions.Regex.Replace(_SQL, "@([a-zA-Z0-9_\\u4e00-\\u9fa5]*)", new System.Text.RegularExpressions.MatchEvaluator(Replace));
            _SQL = System.Text.RegularExpressions.Regex.Replace(_SQL, "#([a-zA-Z0-9_\\u4e00-\\u9fa5]*)", new System.Text.RegularExpressions.MatchEvaluator(Replace));
        }

        string Replace(System.Text.RegularExpressions.Match m)
        {
            string rp = m.Value;
            if (m.Groups.Count > 2)
            {
                #region 匹配@Table.Field模式
                if (datas.Tables.Contains(m.Groups[1].Value))
                {
                    if (datas.Tables[m.Groups[1].Value].Columns.Contains(m.Groups[2].Value) &&
                        datas.Tables[m.Groups[1].Value].Rows.Count > 0)
                    {
                        rp = "@" + m.Groups[1].Value + "_" + m.Groups[2].Value;
                        if (!parm.Contains(rp.ToUpper()))
                        {
                            if (datas.Tables[m.Groups[1].Value].Rows.Count > 1)
                            {
                                ArrayList values = new ArrayList();
                                foreach (DataRow tr in datas.Tables[m.Groups[1].Value].Rows)
                                {
                                    values.Add(tr[m.Groups[2].Value]);
                                }
                                parm.Add(rp.ToUpper(), values);
                            }
                            else
                            {
                                parm.Add(rp.ToUpper(), datas.Tables[m.Groups[1].Value].Rows[0][m.Groups[2].Value]);
                            }
                        }
                    }
                    else
                    {
                        rp = "NULL";
                    }
                }
                else
                {
                    rp = "NULL";
                }
                #endregion
            }
            else
            {
                if (m.Value.StartsWith("@"))
                {
                    #region 匹配@Var模式
                    if (!parm.Contains(rp.ToUpper()))
                    {
                        if (evns.Contains(m.Groups[1].Value))
                        {
                            parm.Add(rp.ToUpper(), evns[m.Groups[1].Value]);
                        }
                        else if (rp.ToUpper() == "@NOW")
                        {
                            rp = "GETDATE()";
                        }
                        else
                        {
                            rp = "NULL";
                        }
                    }
                    #endregion
                }
                else if (m.Value.StartsWith("#"))
                {
                    #region 匹配#var模式，完全字符串替换模式
                    if (evns.Contains(m.Groups[1].Value))
                    {
                        rp = evns[m.Groups[1].Value].ToString();
                    }
                    else
                    {
                        rp = "";
                    }
                    #endregion
                }
            }
            return rp;
        }
    }
    #endregion

}
