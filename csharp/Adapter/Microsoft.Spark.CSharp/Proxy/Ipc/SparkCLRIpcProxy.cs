// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    /// <summary>
    /// calling SparkCLR jvm side API
    /// </summary>
    internal class SparkCLRIpcProxy : ISparkCLRProxy
    {
        private SparkContextIpcProxy sparkContextProxy;

        private static readonly IJvmBridge jvmBridge = new JvmBridge();
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkCLRIpcProxy));
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

            logger.LogInfo("CSharpBackend port number to be used in JvMBridge is " + portNo);
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

        public IDStreamProxy CreateCSharpDStream(IDStreamProxy jdstream, byte[] func, string deserializer)
        {
            var jvmDStreamReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.csharp.CSharpDStream",
                new object[] { (jdstream as DStreamIpcProxy).jvmDStreamReference, func, deserializer });

            var javaDStreamReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDStreamReference, "asJavaDStream"));
            return new DStreamIpcProxy(javaDStreamReference, jvmDStreamReference);
        }

        public IDStreamProxy CreateCSharpTransformed2DStream(IDStreamProxy jdstream, IDStreamProxy jother, byte[] func, string deserializer, string deserializerOther)
        {
            var jvmDStreamReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.csharp.CSharpTransformed2DStream",
                new object[] { (jdstream as DStreamIpcProxy).jvmDStreamReference, (jother as DStreamIpcProxy).jvmDStreamReference, func, deserializer, deserializerOther });

            var javaDStreamReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDStreamReference, "asJavaDStream"));
            return new DStreamIpcProxy(javaDStreamReference, jvmDStreamReference);
        }

        public IDStreamProxy CreateCSharpReducedWindowedDStream(IDStreamProxy jdstream, byte[] func, byte[] invFunc, int windowSeconds, int slideSeconds, string deserializer)
        {
            var windowDurationReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.Duration", new object[] { windowSeconds * 1000 });
            var slideDurationReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.Duration", new object[] { slideSeconds * 1000 });

            var jvmDStreamReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.csharp.CSharpReducedWindowedDStream",
                new object[] { (jdstream as DStreamIpcProxy).jvmDStreamReference, func, invFunc, windowDurationReference, slideDurationReference, deserializer });

            var javaDStreamReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDStreamReference, "asJavaDStream"));
            return new DStreamIpcProxy(javaDStreamReference, jvmDStreamReference);
        }

        public IDStreamProxy CreateCSharpStateDStream(IDStreamProxy jdstream, byte[] func, string deserializer)
        {
            var jvmDStreamReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.csharp.CSharpStateDStream",
                new object[] { (jdstream as DStreamIpcProxy).jvmDStreamReference, func, deserializer });

            var javaDStreamReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDStreamReference, "asJavaDStream"));
            return new DStreamIpcProxy(javaDStreamReference, jvmDStreamReference);
        }
    }
}
