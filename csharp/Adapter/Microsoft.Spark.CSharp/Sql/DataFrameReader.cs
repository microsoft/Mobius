// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// Interface used to load a DataFrame from external storage systems (e.g. file systems,
    /// key-value stores, etc). Use SQLContext.read() to access this.
    /// </summary>
    public class DataFrameReader
    {
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(DataFrameReader));

        private readonly IDataFrameReaderProxy dataFrameReaderProxy;
        private readonly SparkContext sparkContext;

        internal DataFrameReader(IDataFrameReaderProxy dataFrameReaderProxy, SparkContext sparkContext)
        {
            this.dataFrameReaderProxy = dataFrameReaderProxy;
            this.sparkContext = sparkContext;
        }
        /// <summary>
        /// Specifies the input data source format.
        /// </summary>
        public DataFrameReader Format(string source)
        {
            logger.LogInfo("Input data source format for the reader is '{0}'", source);
            dataFrameReaderProxy.Format(source);
            return this;
        }

        /// <summary>
        /// Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
        /// automatically from data. By specifying the schema here, the underlying data source can
        /// skip the schema inference step, and thus speed up data loading.
        /// </summary>
        public DataFrameReader Schema(StructType schema)
        {
            dataFrameReaderProxy.Schema(schema);
            return this;
        }

        /// <summary>
        /// Adds an input option for the underlying data source.
        /// </summary>
        public DataFrameReader Option(string key, string value)
        {
            dataFrameReaderProxy.Options(new Dictionary<string, string>(){{key, value}});
            logger.LogInfo("Input key-vaue option for the data source is {0}={1}", key, value);
            return this;
        }

        /// <summary>
        /// Adds input options for the underlying data source.
        /// </summary>
        public DataFrameReader Options(Dictionary<string,string> options)
        {
            dataFrameReaderProxy.Options(options);
            return this;
        }

        /// <summary>
        /// Loads input in as a [[DataFrame]], for data sources that require a path (e.g. data backed by
        /// a local or distributed file system).
        /// </summary>
        public DataFrame Load(string path)
        {
            return Option("path", path).Load();
        }

        /// <summary>
        /// Loads input in as a DataFrame, for data sources that don't require a path (e.g. external
        /// key-value stores).
        /// </summary>
        public DataFrame Load()
        {
            logger.LogInfo("Loading DataFrame using the reader");
            return new DataFrame(dataFrameReaderProxy.Load(), sparkContext);
        }

        /// <summary>
        /// Construct a [[DataFrame]] representing the database table accessible via JDBC URL,
        /// url named table and connection properties.
        /// </summary>
        public DataFrame Jdbc(string url, string table, Dictionary<String, String> properties)
        {
            logger.LogInfo("Constructing DataFrame using JDBC source. Url={0}, tableName={1}", url, table);
            return new DataFrame(dataFrameReaderProxy.Jdbc(url, table, properties), sparkContext);
        }

        /// <summary>
        /// Construct a DataFrame representing the database table accessible via JDBC URL
        /// url named table. Partitions of the table will be retrieved in parallel based on the parameters
        /// passed to this function.
        /// 
        /// Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
        /// your external database systems.
        /// </summary>
        /// <param name="url">JDBC database url of the form `jdbc:subprotocol:subname`</param>
        /// <param name="table">Name of the table in the external database.</param>
        /// <param name="columnName">the name of a column of integral type that will be used for partitioning.</param>
        /// <param name="lowerBound">the minimum value of `columnName` used to decide partition stride</param>
        /// <param name="upperBound">the maximum value of `columnName` used to decide partition stride</param>
        /// <param name="numPartitions">the number of partitions.  the range `minValue`-`maxValue` will be split evenly into this many partitions</param>
        /// <param name="connectionProperties">JDBC database connection arguments, a list of arbitrary string tag/value. 
        /// Normally at least a "user" and "password" property should be included.</param>
        public DataFrame Jdbc(string url, string table, string columnName, string lowerBound, string upperBound, 
            int numPartitions, Dictionary<String, String> connectionProperties)
        {
            logger.LogInfo("Constructing DataFrame using JDBC source. Url={0}, tableName={1}, columnName={2}", url, table, columnName);
            return new DataFrame(dataFrameReaderProxy.Jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties), sparkContext);
        }

        /// <summary>
        /// Construct a DataFrame representing the database table accessible via JDBC URL
        /// url named table using connection properties. The `predicates` parameter gives a list
        /// expressions suitable for inclusion in WHERE clauses; each one defines one partition
        /// of the DataFrame.
        /// 
        /// Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
        /// your external database systems.
        /// </summary>
        /// <param name="url">JDBC database url of the form `jdbc:subprotocol:subname`</param>
        /// <param name="table">Name of the table in the external database.</param>
        /// <param name="predicates">Condition in the where clause for each partition.</param>
        /// <param name="connectionProperties">JDBC database connection arguments, a list of arbitrary string tag/value. 
        /// Normally at least a "user" and "password" property should be included.</param>
        public DataFrame Jdbc(string url, string table, string[] predicates, Dictionary<String, String> connectionProperties)
        {
            logger.LogInfo("Constructing DataFrame using JDBC source. Url={0}, table={1}", url, table);
            return new DataFrame(dataFrameReaderProxy.Jdbc(url, table, predicates, connectionProperties), sparkContext);
        }

        /// <summary>
        /// Loads a JSON file (one object per line) and returns the result as a DataFrame.
        /// 
        /// This function goes through the input once to determine the input schema. If you know the
        /// schema in advance, use the version that specifies the schema to avoid the extra scan.
        /// </summary>
        /// <param name="path">input path</param>
        public DataFrame Json(string path)
        {
            logger.LogInfo("Constructing DataFrame using JSON source {0}", path);
            return Format("json").Load(path);
        }

        /// <summary>
        /// Loads a Parquet file, returning the result as a [[DataFrame]]. This function returns an empty
        /// DataFrame if no paths are passed in.
        /// </summary>
        public DataFrame Parquet(params string[] path)
        {
            logger.LogInfo("Constructing DataFrame using Parquet source {0}", string.Join(";", path));
            return new DataFrame(dataFrameReaderProxy.Parquet(path), sparkContext);
        }
    }
}
