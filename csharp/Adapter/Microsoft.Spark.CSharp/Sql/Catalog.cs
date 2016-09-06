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
    /// <summary>
    /// Catalog interface for Spark.
    /// </summary>
    public class Catalog
    {
        ICatalogProxy catalogProxy;

        internal Catalog(ICatalogProxy catalogProxy)
        {
            this.catalogProxy = catalogProxy;
        }

        /// <summary>
        /// Returns the current default database in this session.
        /// </summary>
        public string CurrentDatabase
        {
            get { return catalogProxy.CurrentDatabase; }
        }

        // TODO Enable these convenience functions if needed
        /*
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

        public List<Table> GetTablesList(string dbName = null)
        {
            var tables = ListTables(dbName).Collect();
            //iterate and construct Table
            throw new NotImplementedException();
        }

        public List<Table> GetColumnsList(string tableName, string dbName = null)
        {
            var tables = ListColumns(tableName, dbName).Collect();
            //iterate and construct Column 
            throw new NotImplementedException();
        }

        public List<Table> GetFunctionsList(string dbName = null)
        {
            var tables = ListFunctions(dbName).Collect();
            //iterate and construct Table
            throw new NotImplementedException();
        }
        */

        /// <summary>
        /// Returns a list of databases available across all sessions.
        /// </summary>
        /// <returns></returns>
        public DataFrame ListDatabases()
        {
            return catalogProxy.ListDatabases().ToDF();
        }

        /// <summary>
        /// Returns a list of tables in the current database or given database
        /// This includes all temporary tables.
        /// </summary>
        /// <param name="dbName">Optional database name. If not provided, current database is used</param>
        public DataFrame ListTables(string dbName = null)
        {
            return catalogProxy.ListTables(dbName ?? CurrentDatabase).ToDF();
        }

        /// <summary>
        /// Returns a list of columns for the given table in the current database or
        /// the given temporary table.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="dbName">Name of the database. If database is not provided, current database is used</param>
        public DataFrame ListColumns(string tableName, string dbName = null)
        {
            return catalogProxy.ListColumns(tableName, dbName ?? CurrentDatabase).ToDF();
        }

        /// <summary>
        /// Returns a list of functions registered in the specified database.
        /// This includes all temporary functions
        /// </summary>
        /// <param name="dbName">Name of the database. If database is not provided, current database is used</param>
        public DataFrame ListFunctions(string dbName = null)
        {
            return catalogProxy.ListFunctions(dbName ?? CurrentDatabase).ToDF();
        }

        /// <summary>
        /// Sets the current default database in this session.
        /// </summary>
        /// <param name="dbName">Name of database</param>
        public void SetCurrentDatabase(string dbName)
        {
            catalogProxy.SetCurrentDatabase(dbName);
        }

        /// <summary>
        /// Drops the temporary view with the given view name in the catalog.
        /// If the view has been cached before, then it will also be uncached.
        /// </summary>
        /// <param name="tempViewName">Name of the table</param>
        public void DropTempView(string tempViewName)
        {
            catalogProxy.DropTempTable(tempViewName);
        }

        /// <summary>
        /// Returns true if the table is currently cached in-memory.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        public bool IsCached(string tableName)
        {
            return catalogProxy.IsCached(tableName);
        }

        /// <summary>
        /// Caches the specified table in-memory.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        public void CacheTable(string tableName)
        {
            catalogProxy.CacheTable(tableName);
        }

        /// <summary>
        /// Removes the specified table from the in-memory cache.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        public void UnCacheTable(string tableName)
        {
            catalogProxy.UnCacheTable(tableName);
        }

        /// <summary>
        /// Invalidate and refresh all the cached metadata of the given table. For performance reasons,
        /// Spark SQL or the external data source library it uses might cache certain metadata about a
        /// table, such as the location of blocks.When those change outside of Spark SQL, users should
        /// call this function to invalidate the cache.
        /// If this table is cached as an InMemoryRelation, drop the original cached version and make the
        /// new version cached lazily.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        public void RefreshTable(string tableName)
        {
            catalogProxy.RefreshTable(tableName);
        }

        /// <summary>
        /// Removes all cached tables from the in-memory cache.
        /// </summary>
        public void ClearCache()
        {
            catalogProxy.ClearCache();
        }

        /// <summary>
        /// Creates an external table from the given path and returns the corresponding DataFrame.
        /// It will use the default data source configured by spark.sql.sources.default.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="path">Path to table</param>
        public DataFrame CreateExternalTable(string tableName, string path)
        {
            return catalogProxy.CreateExternalTable(tableName, path);
        }

        /// <summary>
        /// Creates an external table from the given path on a data source and returns DataFrame
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="path">Path to table</param>
        /// <param name="source">Data source</param>
        public DataFrame CreateExternalTable(string tableName, string path, string source)
        {
            return catalogProxy.CreateExternalTable(tableName, path, source);
        }

        /// <summary>
        /// Creates an external table from the given path based on a data source and a set of options.
        /// Then, returns the corresponding DataFrame.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="source">Data source</param>
        /// <param name="options">Options to create table</param>
        /// <returns></returns>
        public DataFrame CreateExternalTable(string tableName, string source, Dictionary<string, string> options)
        {
            return catalogProxy.CreateExternalTable(tableName, source, options);
        }

        /// <summary>
        /// Create an external table from the given path based on a data source, a schema and
        /// a set of options.Then, returns the corresponding DataFrame.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        /// <param name="source">Data source</param>
        /// <param name="schema">Schema of the table</param>
        /// <param name="options">Options to create table</param>
        /// <returns></returns>
        public DataFrame CreateExternalTable(string tableName, string source, StructType schema, Dictionary<string, string> options)
        {
            return catalogProxy.CreateExternalTable(tableName, source, schema, options);
        }
    }

    /// <summary>
    /// A database in Spark
    /// </summary>
    public class Database
    {
        /// <summary>
        /// Name of the database
        /// </summary>
        public string Name { get; internal set; }

        /// <summary>
        /// Desciption for the database
        /// </summary>
        public string Description { get; internal set; }

        /// <summary>
        /// Location of the database
        /// </summary>
        public string LocationUri { get; internal set; }
    }

    /// <summary>
    /// A table in Spark
    /// </summary>
    public class Table
    {
        /// <summary>
        /// Name of the table
        /// </summary>
        public string Name { get; internal set; }

        /// <summary>
        /// Name of the database Table belongs to
        /// </summary>
        public string Database { get; internal set; }

        /// <summary>
        /// Description of the table
        /// </summary>
        public string Description { get; internal set; }

        /// <summary>
        /// Type of the table (table, view)
        /// </summary>
        public string TableType { get; internal set; }

        /// <summary>
        /// Whether the table is a temporary table
        /// </summary>
        public bool IsTemporary { get; internal set; }
    }

    /// <summary>
    /// A column in Spark
    /// </summary>
    public class Column
    {
        /// <summary>
        /// Name of the column
        /// </summary>
        public string Name { get; internal set; }

        /// <summary>
        /// Datatype of the column
        /// </summary>
        public string DataType { get; internal set; }

        /// <summary>
        /// Description of the column
        /// </summary>
        public string Description { get; internal set; }

        /// <summary>
        /// Whether the column value can be null
        /// </summary>
        public bool IsNullable { get; internal set; }

        /// <summary>
        /// Whether the column is a partition column.
        /// </summary>
        public bool IsPartition { get; internal set; }

        /// <summary>
        /// Whether the column is a bucket column.
        /// </summary>
        public bool IsBucket { get; internal set; }
    }

    /// <summary>
    /// A user-defined function in Spark
    /// </summary>
    public class Function
    {
        /// <summary>
        /// Name of the column
        /// </summary>
        public string Name { get; internal set; }

        /// <summary>
        /// Name of the database
        /// </summary>
        public string Database { get; internal set; }

        /// <summary>
        /// Description of the function
        /// </summary>
        public string Description { get; internal set; }

        /// <summary>
        /// Fully qualified class name of the function
        /// </summary>
        public string ClassName { get; internal set; }

        /// <summary>
        /// Whether the function is a temporary function or not.
        /// </summary>
        public bool IsTemporary { get; internal set; }
    }
}
