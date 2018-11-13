// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    /// <summary>
    /// calling SparkCLR jvm side API
    /// </summary>
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class SparkCLRIpcProxy : ISparkCLRProxy
    {
        private SparkContextIpcProxy sparkContextProxy;
        private StreamingContextIpcProxy streamingContextIpcProxy;

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

        public IStreamingContextProxy StreamingContextProxy { get { return streamingContextIpcProxy; } }
        
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

        public bool CheckpointExists(string checkpointPath)
        {
            if (checkpointPath == null)
                return false;

            var path = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", checkpointPath);
            var conf = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.conf.Configuration");
            var fs = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(path, "getFileSystem", conf));

            return (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(fs, "exists", path) &&
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(fs, "listStatus", path) != null;
        }

        public IStreamingContextProxy CreateStreamingContext(SparkContext sparkContext, long durationMs)
        {
            streamingContextIpcProxy = new StreamingContextIpcProxy(sparkContext, durationMs);
            return streamingContextIpcProxy;
        }

        public IStreamingContextProxy CreateStreamingContext(string checkpointPath)
        {
            streamingContextIpcProxy = new StreamingContextIpcProxy(checkpointPath);
            return streamingContextIpcProxy;
        }
    }
}
