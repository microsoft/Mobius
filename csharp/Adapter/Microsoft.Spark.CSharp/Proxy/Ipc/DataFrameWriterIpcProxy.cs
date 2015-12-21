// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class DataFrameWriterIpcProxy : IDataFrameWriterProxy
    {
        private readonly JvmObjectReference jvmDataFrameWriterReference;

        internal DataFrameWriterIpcProxy(JvmObjectReference jvmDataFrameWriterReference)
        {
            this.jvmDataFrameWriterReference = jvmDataFrameWriterReference;
        }

        public void Mode(string saveMode)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameWriterReference, "mode", new object[] { saveMode });
        }

        public void Format(string source)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameWriterReference, "format", new object[] { source });
        }

        public void Options(Dictionary<string, string> options)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameWriterReference, "options", new object[] { options });
        }

        public void PartitionBy(params string[] colNames)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameWriterReference, "partitionBy", new object[] { colNames });
        }

        public void Save()
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameWriterReference, "save");
        }

        public void InsertInto(string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameWriterReference, "insertInto", new object[] { tableName });
        }

        public void SaveAsTable(string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameWriterReference, "saveAsTable", new object[] { tableName });
        }

        public void Jdbc(string url, string table, Dictionary<string, string> properties)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameWriterReference, "jdbc", new object[] { url, table, properties });
        }
    }
}
