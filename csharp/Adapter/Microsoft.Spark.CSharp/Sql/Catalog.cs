// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Sql.Catalog
{
    public class Catalog
    {
        ICatalogProxy catalogProxy;

        internal Catalog(ICatalogProxy catalogProxy)
        {
            this.catalogProxy = catalogProxy;
        }

        public string CurrentDatabase
        {
            get { return catalogProxy.CurrentDatabase; }
        }

        public List<Database> GetDatabasesList()
        {
            var rows = ListDatabases().Collect();
            var list = new List<Database>();
            foreach (var row in rows)
            {
                list.Add(new Database
                {
                    Name = row.Get("name"),
                    Description = row.Get("description"),
                    LocationUri = row.Get("locationUri")
                });
            }

            return list;
        }

        public DataFrame ListDatabases()
        {
            return catalogProxy.ListDatabases().ToDF();
        }

        public List<Table> GetTablesList(string dbName = null)
        {
            var tables = ListTables(dbName).Collect();
            //iterate and construct Table
            throw new NotImplementedException();
        }

        public DataFrame ListTables(string dbName = null)
        {
            return catalogProxy.ListTables(dbName ?? CurrentDatabase).ToDF();
        }

        public List<Table> GetColumnsList(string tableName, string dbName = null)
        {
            var tables = ListColumns(tableName, dbName).Collect();
            //iterate and construct Column 
            throw new NotImplementedException();
        }

        public DataFrame ListColumns(string tableName, string dbName = null)
        {
            return catalogProxy.ListColumns(tableName, dbName ?? CurrentDatabase).ToDF();
        }

        public List<Table> GetFunctionsList(string dbName = null)
        {
            var tables = ListFunctions(dbName).Collect();
            //iterate and construct Table
            throw new NotImplementedException();
        }

        public DataFrame ListFunctions(string dbName = null)
        {
            return catalogProxy.ListFunctions(dbName ?? CurrentDatabase).ToDF();
        }

        public void SetCurrentDatabase(string dbName)
        {
            catalogProxy.SetCurrentDatabase(dbName);
        }

        public void DropTempTable(string tableName)
        {
            catalogProxy.DropTempTable(tableName);
        }

        public bool IsCached(string tableName)
        {
            return catalogProxy.IsCached(tableName);
        }

        public void CacheTable(string tableName)
        {
            catalogProxy.CacheTable(tableName);
        }

        public void UnCacheTable(string tableName)
        {
            catalogProxy.UnCacheTable(tableName);
        }

        public void RefreshTable(string tableName)
        {
            catalogProxy.RefreshTable(tableName);
        }

        public void ClearCache()
        {
            catalogProxy.ClearCache();
        }

        public DataFrame CreateExternalTable(string tableName, string path)
        {
            return catalogProxy.CreateExternalTable(tableName, path);
        }

        public DataFrame CreateExternalTable(string tableName, string path, string source)
        {
            return catalogProxy.CreateExternalTable(tableName, path, source);
        }

        public DataFrame CreateExternalTable(string tableName, string source, Dictionary<string, string> options)
        {
            return catalogProxy.CreateExternalTable(tableName, source, options);
        }

        public DataFrame CreateExternalTable(string tableName, string source, StructType schema, Dictionary<string, string> options)
        {
            return catalogProxy.CreateExternalTable(tableName, source, schema, options);
        }
    }

    public class Database
    {
        public string Name { get; internal set; }
        public string Description { get; internal set; }
        public string LocationUri { get; internal set; }
    }

    public class Table
    {
        public string Name { get; internal set; }
        public string Database { get; internal set; }
        public string Description { get; internal set; }
        public string TableType { get; internal set; }
        public bool IsTemporary { get; internal set; }
    }

    public class Column
    {
        public string Name { get; internal set; }
        public string DataType { get; internal set; }
        public string Description { get; internal set; }
        public bool IsNullable { get; internal set; }
        public bool IsPartition { get; internal set; }
        public bool IsBucket { get; internal set; }
    }

    public class Function
    {
        public string Name { get; internal set; }
        public string Database { get; internal set; }
        public string Description { get; internal set; }
        public string ClassName { get; internal set; }
        public bool IsTemporary { get; internal set; }
    }
}
