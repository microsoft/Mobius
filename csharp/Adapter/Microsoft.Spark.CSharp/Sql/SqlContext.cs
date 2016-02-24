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
    /// The entry point for working with structured data (rows and columns) in Spark.  
    /// Allows the creation of [[DataFrame]] objects as well as the execution of SQL queries.
    /// </summary>
    public class SqlContext
    {
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SqlContext));

        private readonly ISqlContextProxy sqlContextProxy;
        private readonly SparkContext sparkContext;
        internal ISqlContextProxy SqlContextProxy { get { return sqlContextProxy; } }

        private static SqlContext instance;

        /// <summary>
        /// Creates a SqlContext
        /// </summary>
        /// <param name="sparkContext"></param>
        public SqlContext(SparkContext sparkContext)
        {
            this.sparkContext = sparkContext;
            sqlContextProxy = sparkContext.SparkContextProxy.CreateSqlContext();
            if (instance == null) instance = this;
        }

        internal SqlContext(SparkContext sparkContext, ISqlContextProxy sqlContextProxy)
        {
            this.sparkContext = sparkContext;
            this.sqlContextProxy = sqlContextProxy;
            if (instance == null) instance = this;
        }

        /// <summary>
        /// Get the existing SQLContext or create a new one with given SparkContext.
        /// </summary>
        /// <param name="sparkContext"></param>
        /// <returns></returns>
        public static SqlContext GetOrCreate(SparkContext sparkContext)
        {
            if (instance == null)
            {
                return new SqlContext(sparkContext);
            }
            return instance;
        }

        /// <summary>
        /// Returns a new SQLContext as new session, that has separate SQLConf, 
        /// registered temporary tables and UDFs, but shared SparkContext and table cache.
        /// </summary>
        /// <returns></returns>
        public SqlContext NewSession()
        {
            var newSessionProxy = sqlContextProxy.NewSession();
            return new SqlContext(this.sparkContext, newSessionProxy);
        }

        /// <summary>
        /// Returns the value of Spark SQL configuration property for the given key.
        /// If the key is not set, returns defaultValue.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="defaultValue"></param>
        /// <returns></returns>
        public string GetConf(string key, string defaultValue)
        {
            return sqlContextProxy.GetConf(key, defaultValue);
        }

        /// <summary>
        /// Sets the given Spark SQL configuration property.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void SetConf(string key, string value)
        {
            sqlContextProxy.SetConf(key, value);
        }

        /// <summary>
        /// Returns a DataFrameReader that can be used to read data in as a DataFrame.
        /// </summary>
        public DataFrameReader Read()
        {
            logger.LogInfo("Using DataFrameReader to read input data from external data source");
            return new DataFrameReader(sqlContextProxy.Read(), sparkContext);
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
            logger.LogInfo("Reading DataFrame from file {0}", path);
            return new DataFrame(sqlContextProxy.ReadDataFrame(path, schema, options), sparkContext);
        }

        /// <summary>
        /// Creates a <see cref="DataFrame"/> from a RDD containing array of object using the given schema.
        /// </summary>
        /// <param name="rdd">RDD containing array of object. The array acts as a row and items within the array act as columns which the schema is specified in <paramref name="schema"/>. </param>
        /// <param name="schema">The schema of DataFrame.</param>
        /// <returns></returns>
        public DataFrame CreateDataFrame(RDD<object[]> rdd, StructType schema)
        {
            // Note: This is for pickling RDD, convert to RDD<byte[]> which happens in CSharpWorker. 
            // The below sqlContextProxy.CreateDataFrame() will call byteArrayRDDToAnyArrayRDD() of SQLUtils.scala which only accept RDD of type RDD[Array[Byte]].
            // In byteArrayRDDToAnyArrayRDD() of SQLUtils.scala, the SerDeUtil.pythonToJava() will be called which is a mapPartitions inside. 
            // It will be executed until the CSharpWorker finishes Pickling to RDD[Array[Byte]].
            var rddRow = rdd.Map(r => r);
            rddRow.serializedMode = SerializedMode.Row;

            return new DataFrame(sqlContextProxy.CreateDataFrame(rddRow.RddProxy, schema.StructTypeProxy), sparkContext); 
        }

        /// <summary>
        /// Registers the given <see cref="DataFrame"/> as a temporary table in the catalog.
        /// Temporary tables exist only during the lifetime of this instance of SqlContext.
        /// </summary>
        /// <param name="dataFrame"></param>
        /// <param name="tableName"></param>
        public void RegisterDataFrameAsTable(DataFrame dataFrame, string tableName)
        {
            sqlContextProxy.RegisterDataFrameAsTable(dataFrame.DataFrameProxy, tableName);
        }

        /// <summary>
        /// Remove the temp table from catalog.
        /// </summary>
        /// <param name="tableName"></param>
        public void DropTempTable(string tableName)
        {
            sqlContextProxy.DropTempTable(tableName);
        }

        /// <summary>
        /// Returns the specified table as a <see cref="DataFrame"/>
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public DataFrame Table(string tableName)
        {
            return new DataFrame(sqlContextProxy.Table(tableName), sparkContext);
        }

        /// <summary>
        /// Returns a <see cref="DataFrame"/> containing names of tables in the given database.
        /// If <paramref name="databaseName"/> is not specified, the current database will be used.
        /// The returned DataFrame has two columns: 'tableName' and 'isTemporary' (a column with bool 
        /// type indicating if a table is a temporary one or not).
        /// </summary>
        /// <param name="databaseName">Name of the database to use. Default to the current database. 
        /// Note: This is only applicable to HiveContext.</param>
        /// <returns></returns>
        public DataFrame Tables(string databaseName = null)
        {
            return databaseName == null ?
                new DataFrame(sqlContextProxy.Tables(), sparkContext) :
                new DataFrame(sqlContextProxy.Tables(databaseName), sparkContext);
        }

        /// <summary>
        /// Returns a list of names of tables in the database <paramref name="databaseName"/>
        /// </summary>
        /// <param name="databaseName">Name of the database to use. Default to the current database.
        /// Note: This is only applicable to HiveContext.</param>
        /// <returns></returns>
        public IEnumerable<string> TableNames(string databaseName = null)
        {
            return databaseName == null ?
                sqlContextProxy.TableNames() : SqlContextProxy.TableNames(databaseName);
        }

        /// <summary>
        /// Caches the specified table in-memory.
        /// </summary>
        /// <param name="tableName"></param>
        public void CacheTable(string tableName)
        {
            sqlContextProxy.CacheTable(tableName);
        }

        /// <summary>
        /// Removes the specified table from the in-memory cache.
        /// </summary>
        /// <param name="tableName"></param>
        public void UncacheTable(string tableName)
        {
            sqlContextProxy.UncacheTable(tableName);
        }

        /// <summary>
        /// Removes all cached tables from the in-memory cache.
        /// </summary>
        public void ClearCache()
        {
            sqlContextProxy.ClearCache();
        }

        /// <summary>
        /// Returns true if the table is currently cached in-memory.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public bool IsCached(string tableName)
        {
            return sqlContextProxy.IsCached(tableName);
        }

        /// <summary>
        /// Executes a SQL query using Spark, returning the result as a DataFrame. The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <returns></returns>
        public DataFrame Sql(string sqlQuery)
        {
            logger.LogInfo("SQL query to execute on the dataframe is {0}", sqlQuery);
            return new DataFrame(sqlContextProxy.Sql(sqlQuery), sparkContext);
        }

        /// <summary>
        /// Loads a JSON file (one object per line), returning the result as a DataFrame
        /// It goes through the entire dataset once to determine the schema.
        /// </summary>
        /// <param name="path">path to JSON file</param>
        /// <returns></returns>
        [Obsolete("Deprecated. As of 1.4.0, replaced by read().json()")]
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
        [Obsolete("Deprecated. As of 1.4.0, replaced by read().json()")]
        public DataFrame JsonFile(string path, StructType schema)
        {
            return Read().Schema(schema).Json(path);
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

        #region UDF Registration
        /// <summary>
        /// Register UDF with no input argument, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool}"/>("MyFilter", () => true);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter()");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT>(string name, Func<RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 1 input argument, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool, string}"/>("MyFilter", (arg1) => arg1 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1>(string name, Func<A1, RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 2 input arguments, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool, string, string}"/>("MyFilter", (arg1, arg2) => arg1 != null && arg2 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2>(string name, Func<A1, A2, RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 3 input arguments, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool, string, string, string}"/>("MyFilter", (arg1, arg2, arg3) => arg1 != null && arg2 != null && arg3 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, columnName3)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3>(string name, Func<A1, A2, A3, RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 4 input arguments, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool, string, string, ..., string}"/>("MyFilter", (arg1, arg2, ..., arg4) => arg1 != null && arg2 != null && ... && arg3 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName4)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4>(string name, Func<A1, A2, A3, A4, RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 5 input arguments, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool, string, string, ..., string}"/>("MyFilter", (arg1, arg2, ..., arg5) => arg1 != null && arg2 != null && ... && arg5 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName5)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5>(string name, Func<A1, A2, A3, A4, A5, RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 6 input arguments, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool, string, string, ..., string}"/>("MyFilter", (arg1, arg2, ..., arg6) => arg1 != null && arg2 != null && ... && arg6 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName6)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <typeparam name="A6"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5, A6>(string name, Func<A1, A2, A3, A4, A5, A6, RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5, A6>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 7 input arguments, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool, string, string, ..., string}"/>("MyFilter", (arg1, arg2, ..., arg7) => arg1 != null && arg2 != null && ... && arg7 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName7)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <typeparam name="A6"></typeparam>
        /// <typeparam name="A7"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5, A6, A7>(string name, Func<A1, A2, A3, A4, A5, A6, A7, RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 8 input arguments, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool, string, string, ..., string}"/>("MyFilter", (arg1, arg2, ..., arg8) => arg1 != null && arg2 != null && ... && arg8 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName8)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <typeparam name="A6"></typeparam>
        /// <typeparam name="A7"></typeparam>
        /// <typeparam name="A8"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5, A6, A7, A8>(string name, Func<A1, A2, A3, A4, A5, A6, A7, A8, RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 9 input arguments, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool, string, string, ..., string}"/>("MyFilter", (arg1, arg2, ..., arg9) => arg1 != null && arg2 != null && ... && arg9 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName9)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <typeparam name="A6"></typeparam>
        /// <typeparam name="A7"></typeparam>
        /// <typeparam name="A8"></typeparam>
        /// <typeparam name="A9"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9>(string name, Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 10 input arguments, e.g:
        ///     <see cref="SqlContext.RegisterFunction{bool, string, string, ..., string}"/>("MyFilter", (arg1, arg2, ..., arg10) => arg1 != null && arg2 != null && ... && arg10 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName10)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <typeparam name="A6"></typeparam>
        /// <typeparam name="A7"></typeparam>
        /// <typeparam name="A8"></typeparam>
        /// <typeparam name="A9"></typeparam>
        /// <typeparam name="A10"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10>(string name, Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT> f)
        {
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10>(f).Execute;
            sqlContextProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }
        #endregion
    }
}
