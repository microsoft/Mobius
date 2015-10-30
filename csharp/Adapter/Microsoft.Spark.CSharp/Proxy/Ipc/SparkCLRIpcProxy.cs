using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    internal class SparkCLRIpcProxy : ISparkCLRProxy
    {
        private SparkContextIpcProxy sparkContextProxy;

        private static IJvmBridge jvmBridge = new JvmBridge();
        internal static IJvmBridge JvmBridge
        {
            get
            {
                return jvmBridge;
            }
        }
        public SparkCLRIpcProxy()
        {
            int portNo = SparkCLREnvironment.ConfigurationService.BackendPortNumber;

            if (portNo == 0) //fail early
            {
                throw new Exception("Port number is not set");
            }

            Console.WriteLine("CSharpBackend port number to be used in JvMBridge is " + portNo);//TODO - send to logger
            JvmBridge.Initialize(portNo);
        }

        ~SparkCLRIpcProxy()
        {
            JvmBridge.Dispose();
        }
        public ISparkContextProxy SparkContextProxy { get { return sparkContextProxy; } }
        
        public ISparkConfProxy CreateSparkConf(bool loadDefaults = true)
        {
            return new SparkConfIpcProxy(JvmBridge.CallConstructor("org.apache.spark.SparkConf", new object[] { loadDefaults }));
        }
        
        public ISparkContextProxy CreateSparkContext(ISparkConfProxy conf)
        {
            JvmObjectReference jvmSparkContextReference = JvmBridge.CallConstructor("org.apache.spark.SparkContext", (conf as SparkConfIpcProxy).JvmSparkConfReference);
            JvmObjectReference jvmJavaContextReference = JvmBridge.CallConstructor("org.apache.spark.api.java.JavaSparkContext", new object[] { jvmSparkContextReference });
            sparkContextProxy = new SparkContextIpcProxy(jvmSparkContextReference, jvmJavaContextReference);
            return sparkContextProxy;
        }

        public IStructFieldProxy CreateStructField(string name, string dataType, bool isNullable)
        {
            return new StructFieldIpcProxy(
                    new JvmObjectReference(
                        JvmBridge.CallStaticJavaMethod(
                            "org.apache.spark.sql.api.csharp.SQLUtils", "createStructField",
                            new object[] { name, dataType, isNullable }).ToString()
                        )
                    );
        }

        public IStructTypeProxy CreateStructType(List<StructField> fields)
        {
            var fieldsReference = fields.Select(s => (s.StructFieldProxy as StructFieldIpcProxy).JvmStructFieldReference).ToList().Cast<JvmObjectReference>();
            //var javaObjectReferenceList = objectList.Cast<JvmObjectReference>().ToList();
            var seq =
                new JvmObjectReference(
                    JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils",
                        "toSeq", new object[] { fieldsReference }).ToString());

            return new StructTypeIpcProxy(
                    new JvmObjectReference(
                        JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "createStructType", new object[] { seq }).ToString()
                        )
                    );
        }
    }
}
