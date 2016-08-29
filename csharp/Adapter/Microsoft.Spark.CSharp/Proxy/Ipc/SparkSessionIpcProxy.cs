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
    internal class SparkSessionIpcProxy : ISparkSessionProxy
    {
        private readonly JvmObjectReference jvmSparkSessionReference;
        private readonly ISqlContextProxy sqlContextProxy;

        private readonly IUdfRegistration udfRegistration;

        public IUdfRegistration Udf
        {
            get
            {
                if (udfRegistration == null)
                {

                }

                return udfRegistration;
            }
        }

        public ISqlContextProxy SqlContextProxy
        {
            get { return sqlContextProxy; }
        }

        public ICatalogProxy GetCatalog()
        {
            return new CatalogIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkSessionReference, "catalog")), sqlContextProxy);
        }

        internal SparkSessionIpcProxy(JvmObjectReference jvmSparkSessionReference)
        {
            this.jvmSparkSessionReference = jvmSparkSessionReference;
            sqlContextProxy = new SqlContextIpcProxy(GetSqlContextReference());
        }

        private JvmObjectReference GetSqlContextReference()
        {
            return
                new JvmObjectReference(
                    (string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "getSqlContext", new object[] { jvmSparkSessionReference }));
        }

        public ISparkSessionProxy NewSession()
        {
            return new SparkSessionIpcProxy(
                new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkSessionReference, "newSession")));
        }

        public IDataFrameReaderProxy Read()
        {
            var javaDataFrameReaderReference = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkSessionReference, "read");
            return new DataFrameReaderIpcProxy(new JvmObjectReference(javaDataFrameReaderReference.ToString()), sqlContextProxy);
        }

        public IDataFrameProxy CreateDataFrame(IRDDProxy rddProxy, IStructTypeProxy structTypeProxy)
        {
            var rdd = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "byteArrayRDDToAnyArrayRDD",
                    new object[] { (rddProxy as RDDIpcProxy).JvmRddReference }).ToString());

            return new DataFrameIpcProxy(
                new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkSessionReference, "applySchemaToPythonRDD",
                    new object[] { rdd, (structTypeProxy as StructTypeIpcProxy).JvmStructTypeReference }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Sql(string sqlQuery)
        {
            var javaDataFrameReference = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkSessionReference, "sql", new object[] { sqlQuery });
            var javaObjectReferenceForDataFrame = new JvmObjectReference(javaDataFrameReference.ToString());
            return new DataFrameIpcProxy(javaObjectReferenceForDataFrame, sqlContextProxy);
        }

        public IDataFrameProxy Table(string tableName)
        {
            return new DataFrameIpcProxy(
                new JvmObjectReference(
                    (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkSessionReference, "table",
                        new object[] { tableName })), sqlContextProxy);
        }

        public void Stop()
        {
            throw new NotImplementedException();
        }
    }
}
