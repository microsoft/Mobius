using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;

namespace AdapterTest.Mocks
{
    internal class MockSparkCLRProxy : ISparkCLRProxy
    {
        public ISparkConfProxy CreateSparkConf(bool loadDefaults = true)
        {
            return new MockSparkConfProxy();
        }
        
        public ISparkContextProxy CreateSparkContext(ISparkConfProxy conf)
        {
            string master = null;
            string appName = null;
            string sparkHome =  null;
            
            if (conf != null)
            {
                MockSparkConfProxy proxy = conf as MockSparkConfProxy;
                if (proxy.stringConfDictionary.ContainsKey("mockmaster"))
                    master = proxy.stringConfDictionary["mockmaster"];
                if (proxy.stringConfDictionary.ContainsKey("mockappName"))
                    appName = proxy.stringConfDictionary["mockappName"];
                if (proxy.stringConfDictionary.ContainsKey("mockhome"))
                    sparkHome = proxy.stringConfDictionary["mockhome"];
            }

            return new MockSparkContextProxy(conf);
        }


        public IStructFieldProxy CreateStructField(string name, string dataType, bool isNullable)
        {
            throw new NotImplementedException();
        }

        public IStructTypeProxy CreateStructType(List<StructField> fields)
        {
            throw new NotImplementedException();
        }

        public ISparkContextProxy SparkContextProxy
        {
            get { throw new NotImplementedException(); }
        }
    }
}
