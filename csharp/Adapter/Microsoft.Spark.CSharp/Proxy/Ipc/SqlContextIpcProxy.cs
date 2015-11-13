// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Sql;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    internal class SqlContextIpcProxy : ISqlContextProxy
    {
        private JvmObjectReference jvmSqlContextReference;

        public SqlContextIpcProxy(JvmObjectReference jvmSqlContextReference)
        {
            this.jvmSqlContextReference = jvmSqlContextReference;
        }

        public IDataFrameProxy ReadDataFrame(string path, StructType schema, Dictionary<string, string> options)
        {
            //TODO parameter Dictionary<string, string> options is not used right now - it is meant to be passed on to data sources
            return new DataFrameIpcProxy(
                        new JvmObjectReference(
                               SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "loadDF", new object[] { jvmSqlContextReference, path, (schema.StructTypeProxy as StructTypeIpcProxy).JvmStructTypeReference }).ToString()
                            ), this
                    );
        }

        public IDataFrameProxy JsonFile(string path)
        {
            var javaDataFrameReference = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "jsonFile", new object[] {path});
            var javaObjectReferenceForDataFrame = new JvmObjectReference(javaDataFrameReference.ToString());
            return new DataFrameIpcProxy(javaObjectReferenceForDataFrame, this);
        }

        public IDataFrameProxy TextFile(string path, StructType schema, string delimiter)
        {
            return new DataFrameIpcProxy(
                    new JvmObjectReference(
                        SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod(
                            "org.apache.spark.sql.api.csharp.SQLUtils", "loadTextFile",
                            new object[] {jvmSqlContextReference, path, delimiter, (schema.StructTypeProxy as StructTypeIpcProxy).JvmStructTypeReference}).ToString()
                        ), this
                    );
        }

        public IDataFrameProxy TextFile(string path, string delimiter, bool hasHeader, bool inferSchema)
        {
            return new DataFrameIpcProxy(
                    new JvmObjectReference(
                        SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod(
                            "org.apache.spark.sql.api.csharp.SQLUtils", "loadTextFile",
                            new object[] {jvmSqlContextReference, path, hasHeader, inferSchema}).ToString()
                        ), this
                    );
        }

        public IDataFrameProxy Sql(string sqlQuery)
        {
            var javaDataFrameReference = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "sql", new object[] { sqlQuery });
            var javaObjectReferenceForDataFrame = new JvmObjectReference(javaDataFrameReference.ToString());
            return new DataFrameIpcProxy(javaObjectReferenceForDataFrame, this);
        }

        public void RegisterFunction(string name, byte[] command, string returnType)
        {
            var judf = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "udf"));

            var hashTableReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.Hashtable", new object[] { });
            var arrayListReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.ArrayList", new object[] { });

            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(judf, "registerPython",
                new object[]
                {
                    name, command, hashTableReference, arrayListReference, 
                    SparkCLREnvironment.ConfigurationService.GetCSharpWorkerExePath(),
                    "1.0",
                    arrayListReference, null, "\"" + returnType + "\""
                });
        }
    }
}
