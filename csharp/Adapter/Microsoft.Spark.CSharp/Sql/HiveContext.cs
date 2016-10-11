// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// HiveContext is deprecated. Use SparkSession.Builder().EnableHiveSupport()
	/// HiveContext is a variant of Spark SQL that integrates with data stored in Hive. 
    /// Configuration for Hive is read from hive-site.xml on the classpath.
    /// It supports running both SQL and HiveQL commands.
    /// </summary>
    public class HiveContext : SqlContext
    {
        /// <summary>
        /// Creates a HiveContext
        /// </summary>
        /// <param name="sparkContext"></param>
        public HiveContext(SparkContext sparkContext)
            : base(sparkContext, sparkContext.SparkContextProxy.CreateHiveContext())
        { }

        internal HiveContext(SparkContext sparkContext, ISqlContextProxy sqlContextProxy)
            : base(sparkContext, sqlContextProxy)
        { }

        /// <summary>
        /// Executes a SQL query using Spark, returning the result as a DataFrame. The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <returns></returns>
        public new DataFrame Sql(string sqlQuery)
        {
            return new DataFrame(SqlContextProxy.Sql(sqlQuery), sparkContext);
        }

        /// <summary>
        /// Invalidate and refresh all the cached the metadata of the given table.
        /// For performance reasons, Spark SQL or the external data source library it uses
        /// might cache certain metadata about a table, such as the location of blocks.
        /// When those change outside of Spark SQL, users should call this function to invalidate the cache.
        /// </summary>
        /// <param name="tableName"></param>
        public void RefreshTable(string tableName)
        {
            SqlContextProxy.RefreshTable(tableName);
        }
    }
}
