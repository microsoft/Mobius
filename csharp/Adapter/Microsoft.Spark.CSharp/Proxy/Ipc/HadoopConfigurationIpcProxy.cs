// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class HadoopConfigurationIpcProxy : IHadoopConfigurationProxy
    {
        private readonly JvmObjectReference jvmHadoopConfigurationReference;
        public HadoopConfigurationIpcProxy(JvmObjectReference jHadoopConf)
        {
            jvmHadoopConfigurationReference = jHadoopConf;
        }

        public void Set(string name, string value)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHadoopConfigurationReference, "set", new object[] { name, value });
        }

        public string Get(string name, string defaultvalue)
        {
            return SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHadoopConfigurationReference, "get", new object[] { name, defaultvalue }).ToString();
        }
    }
}
