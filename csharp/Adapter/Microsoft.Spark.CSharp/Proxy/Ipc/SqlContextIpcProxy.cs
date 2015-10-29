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
        private ISparkContextProxy sparkContextProxy;

        public void CreateSqlContext(ISparkContextProxy scProxy)
        {
            sparkContextProxy = scProxy;
            jvmSqlContextReference = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "createSQLContext", new object[] { (sparkContextProxy as SparkContextIpcProxy).JvmSparkContextReference }).ToString());
        }

        public StructField CreateStructField(string name, string dataType, bool isNullable)
        {
            return new StructField(
                new StructFieldIpcProxy(
                    new JvmObjectReference(
                        SparkCLREnvironment.JvmBridge.CallStaticJavaMethod(
                            "org.apache.spark.sql.api.csharp.SQLUtils", "createStructField",
                            new object[] {name, dataType, isNullable}).ToString()
                        )
                    )
                );
        }

        public StructType CreateStructType(List<StructField> fields)
        {
            var fieldsReference = fields.Select(s => (s.StructFieldProxy as StructFieldIpcProxy).JvmStructFieldReference).ToList().Cast<JvmObjectReference>();
            //var javaObjectReferenceList = objectList.Cast<JvmObjectReference>().ToList();
            var seq = 
                new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils",
                        "toSeq", new object[] { fieldsReference }).ToString());

            return new StructType(
                new StructTypeIpcProxy(
                    new JvmObjectReference(
                        SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "createStructType", new object[] { seq }).ToString()
                        )
                    )
                );
        }

        public IDataFrameProxy ReaDataFrame(string path, StructType schema, Dictionary<string, string> options)
        {
            //parameter Dictionary<string, string> options is not used right now - it is meant to be passed on to data sources
            return new DataFrameIpcProxy(
                        new JvmObjectReference(
                               SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "loadDF", new object[] { jvmSqlContextReference, path, (schema.StructTypeProxy as StructTypeIpcProxy).JvmStructTypeReference }).ToString()
                            ), this
                    );
        }

        public IDataFrameProxy JsonFile(string path)
        {
            var javaDataFrameReference = SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "jsonFile", new object[] {path});
            var javaObjectReferenceForDataFrame = new JvmObjectReference(javaDataFrameReference.ToString());
            return new DataFrameIpcProxy(javaObjectReferenceForDataFrame, this);
        }

        public IDataFrameProxy TextFile(string path, StructType schema, string delimiter)
        {
            return new DataFrameIpcProxy(
                    new JvmObjectReference(
                        SparkCLREnvironment.JvmBridge.CallStaticJavaMethod(
                            "org.apache.spark.sql.api.csharp.SQLUtils", "loadTextFile",
                            new object[] {jvmSqlContextReference, path, delimiter, (schema.StructTypeProxy as StructTypeIpcProxy).JvmStructTypeReference}).ToString()
                        ), this
                    );
        }

        public IDataFrameProxy TextFile(string path, string delimiter, bool hasHeader, bool inferSchema)
        {
            return new DataFrameIpcProxy(
                    new JvmObjectReference(
                        SparkCLREnvironment.JvmBridge.CallStaticJavaMethod(
                            "org.apache.spark.sql.api.csharp.SQLUtils", "loadTextFile",
                            new object[] {jvmSqlContextReference, path, hasHeader, inferSchema}).ToString()
                        ), this
                    );
        }

        public IDataFrameProxy Sql(string sqlQuery)
        {
            var javaDataFrameReference = SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmSqlContextReference, "sql", new object[] { sqlQuery });
            var javaObjectReferenceForDataFrame = new JvmObjectReference(javaDataFrameReference.ToString());
            return new DataFrameIpcProxy(javaObjectReferenceForDataFrame, this);
        }
    }
}
