// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class DataFrameNaFunctionsIpcProxy : IDataFrameNaFunctionsProxy
    {
        private readonly JvmObjectReference jvmDataFrameNaFunctionsReference;
        private readonly ISqlContextProxy sqlContextProxy;

        internal DataFrameNaFunctionsIpcProxy(JvmObjectReference jvmDataFrameNaFunctionsReference, ISqlContextProxy sqlContextProxy)
        {
            this.jvmDataFrameNaFunctionsReference = jvmDataFrameNaFunctionsReference;
            this.sqlContextProxy = sqlContextProxy;
        }

        public IDataFrameProxy Drop(int minNonNulls, string[] cols)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameNaFunctionsReference, "drop", minNonNulls, cols).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Fill(double value, string[] cols)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameNaFunctionsReference, "fill", value, cols).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Fill(string value, string[] cols)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameNaFunctionsReference, "fill", value, cols).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Fill(Dictionary<string, object> valueMap)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameNaFunctionsReference, "fill", valueMap).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Replace<T>(string col, Dictionary<T, T> replacement)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameNaFunctionsReference, "replace", col, replacement).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Replace<T>(string[] cols, Dictionary<T, T> replacement)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                   SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                       jvmDataFrameNaFunctionsReference, "replace", cols, replacement).ToString()), sqlContextProxy);
        }
    }
}
