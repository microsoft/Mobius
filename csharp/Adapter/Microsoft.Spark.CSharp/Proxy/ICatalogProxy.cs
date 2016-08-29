// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Sql.Catalog;
using Column = Microsoft.Spark.CSharp.Sql.Catalog.Column;

namespace Microsoft.Spark.CSharp.Proxy
{
    interface ICatalogProxy
    {
        string CurrentDatabase { get; }

        void SetCurrentDatabase(string dbName);

        Dataset<Database> ListDatabases();

        Dataset<Table> ListTables(string dbName);

        Dataset<Function> ListFunctions(string dbName);

        Dataset<Column> ListColumns(string tableName);

        Dataset<Column> ListColumns(string dbName, string tableName);

        void DropTempTable(string tableName);

        bool IsCached(string tableName);

        void CacheTable(string tableName);

        void UnCacheTable(string tableName);

        void RefreshTable(string tableName);

        void ClearCache();

        DataFrame CreateExternalTable(string tableName, string path);

        DataFrame CreateExternalTable(string tableName, string path, string source);

        DataFrame CreateExternalTable(string tableName, string source, Dictionary<string, string> options);

        DataFrame CreateExternalTable(string tableName, string source, StructType schema,
            Dictionary<string, string> options);
    }
}
