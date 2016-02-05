// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// Interface used to write a DataFrame to external storage systems (e.g. file systems,
    /// key-value stores, etc). Use DataFrame.Write to access this.
    /// 
    /// See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter
    /// </summary>
    public class DataFrameWriter
    {
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(DataFrameWriter));
        internal IDataFrameWriterProxy DataFrameWriterProxy
        {
            get { return dataFrameWriterProxy; }
        }

        private readonly IDataFrameWriterProxy dataFrameWriterProxy;

        internal DataFrameWriter(IDataFrameWriterProxy dataFrameWriterProxy)
        {
            this.dataFrameWriterProxy = dataFrameWriterProxy;
        }

        /// <summary>
        /// Specifies the behavior when data or table already exists. Options include:
        ///   - `SaveMode.Overwrite`: overwrite the existing data.
        ///   - `SaveMode.Append`: append the data.
        ///   - `SaveMode.Ignore`: ignore the operation (i.e. no-op).
        ///   - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.
        /// </summary>
        public DataFrameWriter Mode(SaveMode saveMode)
        {
            return Mode(saveMode.GetStringValue());
        }

        /// <summary>
        /// Specifies the behavior when data or table already exists. Options include:
        ///   - `SaveMode.Overwrite`: overwrite the existing data.
        ///   - `SaveMode.Append`: append the data.
        ///   - `SaveMode.Ignore`: ignore the operation (i.e. no-op).
        ///   - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.
        /// </summary>
        public DataFrameWriter Mode(string saveMode)
        {
            dataFrameWriterProxy.Mode(saveMode);
            return this;
        }

        /// <summary>
        /// Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
        /// </summary>
        public DataFrameWriter Format(string source)
        {
            logger.LogInfo("Output data storage format for the writer is '{0}'", source);
            dataFrameWriterProxy.Format(source);
            return this;
        }

        /// <summary>
        /// Adds an output option for the underlying data source.
        /// </summary>
        public DataFrameWriter Option(string key, string value)
        {
            var options = new Dictionary<string, string>() { { key, value } };
            logger.LogInfo("Output key-vaue option for the external data stroage is {0}={1}", key, value);
            return Options(options);
        }

        /// <summary>
        /// Adds output options for the underlying data source.
        /// </summary>
        public DataFrameWriter Options(Dictionary<string,string> options)
        {
            dataFrameWriterProxy.Options(options);
            return this;
        }

        /// <summary>
        /// Partitions the output by the given columns on the file system. If specified, the output is
        /// laid out on the file system similar to Hive's partitioning scheme.
        /// 
        /// This is only applicable for Parquet at the moment.
        /// </summary>
        public DataFrameWriter PartitionBy(params string[] colNames)
        {
            dataFrameWriterProxy.PartitionBy(colNames);
            return this;
        }

        /// <summary>
        /// Saves the content of the DataFrame at the specified path.
        /// </summary>
        public void Save(string path)
        {
            Option("path", path).Save();
        }

        /// <summary>
        /// Saves the content of the DataFrame as the specified table.
        /// </summary>
        public void Save()
        {
            dataFrameWriterProxy.Save();
        }

        /// <summary>
        /// Inserts the content of the DataFrame to the specified table. It requires that
        /// the schema of the DataFrame is the same as the schema of the table.
        /// Because it inserts data to an existing table, format or options will be ignored.
        /// </summary>
        public void InsertInto(string tableName)
        {
            dataFrameWriterProxy.InsertInto(tableName);
        }

        /// <summary>
        /// Saves the content of the DataFrame as the specified table.
        /// In the case the table already exists, behavior of this function depends on the
        /// save mode, specified by the `mode` function (default to throwing an exception).
        /// When `mode` is `Overwrite`, the schema of the DataFrame does not need to be
        /// the same as that of the existing table.
        /// When `mode` is `Append`, the schema of the DataFrame need to be
        /// the same as that of the existing table, and format or options will be ignored.
        /// </summary>
        public void SaveAsTable(string tableName)
        {
            dataFrameWriterProxy.SaveAsTable(tableName);
        }

        /// <summary>
        /// Saves the content of the DataFrame to a external database table via JDBC. In the case the
        /// table already exists in the external database, behavior of this function depends on the
        /// save mode, specified by the `mode` function (default to throwing an exception).
        /// 
        /// Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
        /// your external database systems.
        /// </summary>
        /// <param name="url">JDBC database url of the form `jdbc:subprotocol:subname`</param>
        /// <param name="table">Name of the table in the external database.</param>
        /// <param name="properties">JDBC database connection arguments, a list of arbitrary string tag/value. 
        /// Normally at least a "user" and "password" property should be included.</param>
        public void Jdbc(string url, string table, Dictionary<string, string> properties)
        {
            dataFrameWriterProxy.Jdbc(url, table, properties);
        }

        /// <summary>
        /// Saves the content of the DataFrame in JSON format at the specified path.
        /// This is equivalent to:
        ///    Format("json").Save(path)
        /// </summary>
        public void Json(string path)
        {
            Format("json").Save(path);
        }

        /// <summary>
        /// Saves the content of the DataFrame in JSON format at the specified path.
        /// This is equivalent to:
        ///    Format("parquet").Save(path)
        /// </summary>
        public void Parquet(string path)
        {
            Format("parquet").Save(path);
        }
    }
}
