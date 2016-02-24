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
using Microsoft.Spark.CSharp.Sql;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class SqlContextIpcProxy : ISqlContextProxy
    {
        private readonly JvmObjectReference jvmSqlContextReference;

        public SqlContextIpcProxy(JvmObjectReference jvmSqlContextReference)
        {
            this.jvmSqlContextReference = jvmSqlContextReference;
        }

        public IDataFrameReaderProxy Read()
        {
            var javaDataFrameReaderReference = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "read");
            return new DataFrameReaderIpcProxy(new JvmObjectReference(javaDataFrameReaderReference.ToString()), this);
        }
        
        public IDataFrameProxy CreateDataFrame(IRDDProxy rddProxy, IStructTypeProxy structTypeProxy)
        {
            var rdd = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "byteArrayRDDToAnyArrayRDD",
                    new object[] { (rddProxy as RDDIpcProxy).JvmRddReference }).ToString());

            return new DataFrameIpcProxy(
                new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "applySchemaToPythonRDD",
                    new object[] { rdd, (structTypeProxy as StructTypeIpcProxy).JvmStructTypeReference }).ToString()), this);
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
                            new object[] { jvmSqlContextReference, path, delimiter, schema.Json}).ToString()
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

            var dt =  new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.types.DataType", "fromJson", new object[] { "\"" + returnType + "\"" }));

            var udf = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.sql.UserDefinedPythonFunction", new object[]
                {
                    name, command, hashTableReference, arrayListReference, 
                    SparkCLREnvironment.ConfigurationService.GetCSharpWorkerExePath(),
                    "1.0",
                    arrayListReference, null, dt
                });

            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(judf, "registerPython", new object[] {name, udf});
        }

        public ISqlContextProxy NewSession()
        {
            return new SqlContextIpcProxy(
                new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "newSession")));
        }

        public string GetConf(string key, string defaultValue)
        {
            return (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "getConf", new object[] { key, defaultValue });
        }

        public void SetConf(string key, string value)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "setConf", new object[] { key, value });
        }

        public void RegisterDataFrameAsTable(IDataFrameProxy dataFrameProxy, string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmSqlContextReference, "registerDataFrameAsTable", 
                new object[] { (dataFrameProxy as DataFrameIpcProxy).JvmDataFrameReference, tableName });
        }

        public void DropTempTable(string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmSqlContextReference, "dropTempTable", new object[] { tableName });
        }

        public IDataFrameProxy Table(string tableName)
        {
            return new DataFrameIpcProxy(
                new JvmObjectReference(
                    (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "table",
                        new object[] { tableName })), this);
        }

        public IDataFrameProxy Tables()
        {
            return new DataFrameIpcProxy(
                new JvmObjectReference(
                    (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "tables")), this);
        }

        public IDataFrameProxy Tables(string databaseName)
        {
            return new DataFrameIpcProxy(
                new JvmObjectReference(
                    (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "tables",
                        new object[] { databaseName })), this);
        }

        public IEnumerable<string> TableNames()
        {
            var tableNames = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "tableNames");
            return (List<string>) tableNames;
        }

        public IEnumerable<string> TableNames(string databaseName)
        {
            return (List<string>)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "tableNames",
                new object[] { databaseName });
        }

        public void CacheTable(string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "cacheTable",
                new object[] { tableName });
        }

        public void UncacheTable(string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "uncacheTable",
                new object[] { tableName });
        }

        public void ClearCache()
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "clearCache");
        }

        public bool IsCached(string tableName)
        {
            return (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "isCached",
                new object[] { tableName });
        }
    }
}
