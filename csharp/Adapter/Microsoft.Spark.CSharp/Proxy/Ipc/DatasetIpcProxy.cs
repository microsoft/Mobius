// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class DatasetIpcProxy : IDatasetProxy
    {
        private readonly JvmObjectReference jvmDatasetReference;
        private readonly ISqlContextProxy sqlContextProxy;

        internal DatasetIpcProxy(JvmObjectReference jvmDatasetReference, ISqlContextProxy sqlContextProxy)
        {
            this.jvmDatasetReference = jvmDatasetReference;
            this.sqlContextProxy = sqlContextProxy;
        }

        public IDataFrameProxy ToDF()
        {
            return new DataFrameIpcProxy(
                    new JvmObjectReference(
                        (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDatasetReference, "toDF")), 
                    sqlContextProxy
                );
        }
    }
}
