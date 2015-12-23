// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;


namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class SparkConfIpcProxy : ISparkConfProxy
    {
        private JvmObjectReference jvmSparkConfReference;

        internal JvmObjectReference JvmSparkConfReference
        {
            get { return jvmSparkConfReference; }
        }

        public SparkConfIpcProxy(JvmObjectReference jvmSparkConfReference)
        {
            this.jvmSparkConfReference = jvmSparkConfReference;
        }
       
        public void SetMaster(string master)
        {
            jvmSparkConfReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkConfReference, "setMaster", new object[] { master }).ToString());
        }

        public void SetAppName(string appName)
        {
            jvmSparkConfReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkConfReference, "setAppName", new object[] { appName }).ToString());
        }

        public void SetSparkHome(string sparkHome)
        {
            jvmSparkConfReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkConfReference, "setSparkHome", new object[] { sparkHome }).ToString());
        }

        public void Set(string key, string value)
        {
            jvmSparkConfReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkConfReference, "set", new object[] { key, value }).ToString());
        }

        public int GetInt(string key, int defaultValue)
        {
            return int.Parse(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkConfReference, "getInt", new object[] { key, defaultValue }).ToString());
        }

        public string Get(string key, string defaultValue)
        {
            return SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkConfReference, "get", new object[] { key, defaultValue }).ToString();
        }
    }
}
