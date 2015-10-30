// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// The entry point for working with structured data (rows and columns) in Spark.  
    /// Allows the creation of [[DataFrame]] objects as well as the execution of SQL queries.
    /// </summary>
    public class SqlContext
    {
        private ISqlContextProxy sqlContextProxy;
        private SparkContext sparkContext;
        internal ISqlContextProxy SqlContextProxy { get { return sqlContextProxy; } }
        public SqlContext(SparkContext sparkContext)
        {
            this.sparkContext = sparkContext;
            sqlContextProxy = sparkContext.SparkContextProxy.CreateSqlContext();  
        }

        /// <summary>
        /// Loads a dataframe the source path using the given schema and options
        /// </summary>
        /// <param name="path"></param>
        /// <param name="schema"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public DataFrame ReadDataFrame(string path, StructType schema, Dictionary<string, string> options)
        {
            return new DataFrame(sqlContextProxy.ReaDataFrame(path, schema, options), sparkContext);
        }

        public DataFrame CreateDataFrame(RDD<byte[]> rdd, StructType schema)
        {
            throw new NotImplementedException();    
        }

        /// <summary>
        /// Executes a SQL query using Spark, returning the result as a DataFrame. The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <returns></returns>
        public DataFrame Sql(string sqlQuery)
        {
            return new DataFrame(sqlContextProxy.Sql(sqlQuery), sparkContext);
        }

        /// <summary>
        /// Loads a JSON file (one object per line), returning the result as a DataFrame
        /// It goes through the entire dataset once to determine the schema.
        /// </summary>
        /// <param name="path">path to JSON file</param>
        /// <returns></returns>
        public DataFrame JsonFile(string path)
        {
            return new DataFrame(sqlContextProxy.JsonFile(path), sparkContext);
        }
        
        /// <summary>
        /// Loads a JSON file (one object per line) and applies the given schema
        /// </summary>
        /// <param name="path">path to JSON file</param>
        /// <param name="schema">schema to use</param>
        /// <returns></returns>
        public DataFrame JsonFile(string path, StructType schema)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Loads text file with the specific column delimited using the given schema
        /// </summary>
        /// <param name="path">path to text file</param>
        /// <param name="schema">schema to use</param>
        /// <param name="delimiter">delimiter to use</param>
        /// <returns></returns>
        public DataFrame TextFile(string path, StructType schema, string delimiter =",")
        {
            return new DataFrame(sqlContextProxy.TextFile(path, schema, delimiter), sparkContext);
        }

        /// <summary>
        /// Loads a text file (one object per line), returning the result as a DataFrame
        /// </summary>
        /// <param name="path">path to text file</param>
        /// <param name="delimiter">delimited to use</param>
        /// <param name="hasHeader">indicates if the text file has a header row</param>
        /// <param name="inferSchema">indicates if every row has to be read to infer the schema; if false, columns will be strings</param>
        /// <returns></returns>
        public DataFrame TextFile(string path, string delimiter = ",", bool hasHeader = false, bool inferSchema = false)
        {
            return new DataFrame(sqlContextProxy.TextFile(path, delimiter, hasHeader, inferSchema), sparkContext);
        }
    }
}
