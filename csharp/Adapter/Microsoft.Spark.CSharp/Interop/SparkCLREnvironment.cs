// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Runtime.CompilerServices;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;

[assembly: InternalsVisibleTo("AdapterTest")]
namespace Microsoft.Spark.CSharp.Interop
{
    /// <summary>
    /// Contains everything needed to setup an environment for using C# with Spark
    /// </summary>
    public class SparkCLREnvironment : IDisposable
    {
        internal IJvmBridge jvmBridge;

        internal static IJvmBridge JvmBridge
        {
            get
            {
                return Environment.jvmBridge;
            }
        }

        internal ISparkConfProxy sparkConfProxy;
        internal static ISparkConfProxy SparkConfProxy
        {
            get
            {
                return Environment.sparkConfProxy;
            }
        }

        internal ISparkContextProxy sparkContextProxy;
        internal static ISparkContextProxy SparkContextProxy
        {
            get
            {
                return Environment.sparkContextProxy;
            }
        }

        //internal IStreamingContextProxy streamingContextProxy;
        //internal static IStreamingContextProxy StreamingContextProxy
        //{
        //    get
        //    {
        //        return Environment.streamingContextProxy;
        //    }
        //}

        internal ISqlContextProxy sqlContextProxy;
        internal static ISqlContextProxy SqlContextProxy
        {
            get
            {
                return Environment.sqlContextProxy;
            }
        }

        internal IConfigurationService configurationService;

        internal static IConfigurationService ConfigurationService
        {
            get
            {
                return Environment.configurationService;
            }
            set
            {
                Environment.configurationService = value;
            }
        }

        protected static SparkCLREnvironment Environment = new SparkCLREnvironment();

        protected SparkCLREnvironment() { }

        /// <summary>
        /// Initializes and returns the environment for SparkCLR execution
        /// </summary>
        /// <returns></returns>
        public static SparkCLREnvironment Initialize()
        {
            Environment.InitializeEnvironment();
            return Environment;
        }

        /// <summary>
        /// Disposes the socket used in the JVM-CLR bridge
        /// </summary>
        public void Dispose()
        {
            jvmBridge.Dispose();
        }

        protected virtual void InitializeEnvironment()
        {
            var proxyFactory = new ProxyFactory();
            configurationService = new ConfigurationService();
            sparkConfProxy = proxyFactory.GetSparkConfProxy();
            sparkContextProxy = proxyFactory.GetSparkContextProxy();
            //streamingContextProxy = new StreamingContextIpcProxy();
            sqlContextProxy = proxyFactory.GetSqlContextProxy();
            jvmBridge = new JvmBridge();
            InitializeJvmBridge();
        }
       
        private void InitializeJvmBridge()
        {
            int portNo = ConfigurationService.BackendPortNumber;

            if (portNo == 0) //fail early
            {
                throw new Exception("Port number is not set");
            }

            Console.WriteLine("CSharpBackend port number to be used in JvMBridge is " + portNo);//TODO - send to logger
            jvmBridge.Initialize(portNo);
        }

        private class ProxyFactory
        {
            private readonly InteropType interopType;
            internal ProxyFactory(InteropType interopType = InteropType.IPC)
            {
                this.interopType = interopType;
            }

            internal ISparkConfProxy GetSparkConfProxy()
            {
                switch (interopType)
                {
                     case InteropType.IPC:
                        return new SparkConfIpcProxy();

                    default:
                        throw new NotImplementedException();
                }
            }

            internal ISparkContextProxy GetSparkContextProxy()
            {
                switch (interopType)
                {
                    case InteropType.IPC:
                        return new SparkContextIpcProxy();

                    default:
                        throw new NotImplementedException();
                }
            }

            internal ISqlContextProxy GetSqlContextProxy()
            {
                switch (interopType)
                {
                    case InteropType.IPC:
                        return new SqlContextIpcProxy();

                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public enum InteropType
        {
            IPC
        }
    }
}
