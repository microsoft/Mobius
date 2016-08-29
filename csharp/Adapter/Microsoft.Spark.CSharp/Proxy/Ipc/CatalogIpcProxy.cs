// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Sql.Catalog;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    public class CatalogIpcProxy : ICatalogProxy
    {
        private readonly JvmObjectReference jvmCatalogReference;
        private readonly ISqlContextProxy sqlContextProxy;

        internal CatalogIpcProxy(JvmObjectReference jvmCatalogReference, ISqlContextProxy sqlContextProxy)
        {
            this.jvmCatalogReference = jvmCatalogReference;
            this.sqlContextProxy = sqlContextProxy;
        }

        public string CurrentDatabase
        {
            get
            {
                return SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmCatalogReference, "currentDatabase").ToString();
            }
        }

        public void CacheTable(string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmCatalogReference, "cacheTable", new object[] { tableName });
        }

        public void ClearCache()
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmCatalogReference, "clearCache");
        }

        public DataFrame CreateExternalTable(string tableName, string path)
        {
            throw new NotImplementedException();
        }

        public DataFrame CreateExternalTable(string tableName, string source, Dictionary<string, string> options)
        {
            throw new NotImplementedException();
        }

        public DataFrame CreateExternalTable(string tableName, string path, string source)
        {
            throw new NotImplementedException();
        }

        public DataFrame CreateExternalTable(string tableName, string source, StructType schema, Dictionary<string, string> options)
        {
            throw new NotImplementedException();
        }

        public void DropTempTable(string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmCatalogReference, "dropTempView", new object[] { tableName });
        }

        public bool IsCached(string tableName)
        {
            throw new NotImplementedException();
        }

        public Dataset<Sql.Catalog.Column> ListColumns(string tableName)
        {
            throw new NotImplementedException();
        }

        public Dataset<Sql.Catalog.Column> ListColumns(string dbName, string tableName)
        {
            throw new NotImplementedException();
        }

        public Dataset<Database> ListDatabases()
        {
            return new Dataset<Database>(
                        new DatasetIpcProxy(
                            new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmCatalogReference, "listDatabases")), sqlContextProxy));
        }

        public Dataset<Function> ListFunctions(string dbName)
        {
            return new Dataset<Function>(
            new DatasetIpcProxy(
                new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmCatalogReference, "listFunctions", new object[] { dbName })), sqlContextProxy));
        }

        public Dataset<Table> ListTables(string dbName)
        {
            return new Dataset<Table>(
            new DatasetIpcProxy(
                new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmCatalogReference, "listTables", new object[] {dbName})), sqlContextProxy));
        }

        public void SetCurrentDatabase(string dbName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmCatalogReference, "setCurrentDatabase", new object[] { dbName });
        }

        public void UnCacheTable(string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmCatalogReference, "uncacheTable", new object[] { tableName });
        }

        public void RefreshTable(string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmCatalogReference, "refreshTable", new object[] { tableName });
        }

        Dataset<Sql.Catalog.Column> ICatalogProxy.ListColumns(string tableName)
        {
            throw new NotImplementedException();
        }

        Dataset<Sql.Catalog.Column> ICatalogProxy.ListColumns(string dbName, string tableName)
        {
            throw new NotImplementedException();
        }
    }
}
