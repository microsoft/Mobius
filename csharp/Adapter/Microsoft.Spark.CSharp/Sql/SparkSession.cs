// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Remoting.Contexts;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql.Catalog;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// The entry point to programming Spark with the Dataset and DataFrame API.
    /// </summary>
    public class SparkSession
    {
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkSession));

        private ISparkSessionProxy sparkSessionProxy;

        internal ISparkSessionProxy SparkSessionProxy
        {
            get { return sparkSessionProxy; } 
            //setter is used only for testing...//TODO - refactor
            set { sparkSessionProxy = value; }
        }

        private Catalog.Catalog catalog;

        /// <summary>
        /// Interface through which the user may create, drop, alter or query underlying
        /// databases, tables, functions etc.
        /// </summary>
        public Catalog.Catalog Catalog
        {
            get { return catalog ?? (catalog = new Catalog.Catalog(SparkSessionProxy.GetCatalog())); }
        }

		internal JvmObjectReference JvmReference => (sparkSessionProxy as SparkSessionIpcProxy)?.JvmReference;

		/// <summary>
		/// Interface through which the user may access the underlying SparkContext.
		/// </summary>
		public SparkContext SparkContext { get; private set; }

        public UdfRegistration Udf
        {
            get { return new UdfRegistration(sparkSessionProxy.Udf); }
        }

        /// <summary>
        /// Builder for SparkSession
        /// </summary>
        public static Builder Builder()
        {
            return new Builder();
        }

        internal SparkSession(SparkContext sparkContext)
        {
            sparkSessionProxy = sparkContext.SparkContextProxy.CreateSparkSession();
            SparkContext = sparkContext;
        }

        internal SparkSession(ISparkSessionProxy sparkSessionProxy)
        {
            this.sparkSessionProxy = sparkSessionProxy;
        }

        /// <summary>
        /// Start a new session with isolated SQL configurations, temporary tables, registered
        /// functions are isolated, but sharing the underlying [[SparkContext]] and cached data.
        /// Note: Other than the [[SparkContext]], all shared state is initialized lazily.
        /// This method will force the initialization of the shared state to ensure that parent
        /// and child sessions are set up with the same shared state. If the underlying catalog
        /// implementation is Hive, this will initialize the metastore, which may take some time.
        /// </summary>
        public SparkSession NewSession()
        {
            return new SparkSession(sparkSessionProxy.NewSession());
        }

        /// <summary>
        /// Stop underlying SparkContext
        /// </summary>
        public void Stop()
        {
            sparkSessionProxy.Stop();
        }

        /// <summary>
        /// Returns a DataFrameReader that can be used to read non-streaming data in as a DataFrame
        /// </summary>
        /// <returns></returns>
        public DataFrameReader Read()
        {
            logger.LogInfo("Using DataFrameReader to read input data from external data source");
            return new DataFrameReader(sparkSessionProxy.Read(), SparkContext);
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
	        var rddRow = rdd.MapPartitions(r => r.Select(rr => rr));
            rddRow.serializedMode = SerializedMode.Row;

            return new DataFrame(sparkSessionProxy.CreateDataFrame(rddRow.RddProxy, schema.StructTypeProxy), SparkContext);
        }

		public DataFrame CreateDataFrame(RDD<Row> rdd, StructType schema)
		{
			// Note: This is for pickling RDD, convert to RDD<byte[]> which happens in CSharpWorker. 
			// The below sqlContextProxy.CreateDataFrame() will call byteArrayRDDToAnyArrayRDD() of SQLUtils.scala which only accept RDD of type RDD[Array[Byte]].
			// In byteArrayRDDToAnyArrayRDD() of SQLUtils.scala, the SerDeUtil.pythonToJava() will be called which is a mapPartitions inside. 
			// It will be executed until the CSharpWorker finishes Pickling to RDD[Array[Byte]].
			var rddRow = rdd.MapPartitions(rows => rows.Select(r => r.Values));
			rddRow.serializedMode = SerializedMode.Row;

			return new DataFrame(sparkSessionProxy.CreateDataFrame(rddRow.RddProxy, schema.StructTypeProxy), SparkContext);
		}

		/// <summary>
		/// Returns the specified table as a <see cref="DataFrame"/>
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns></returns>
		public DataFrame Table(string tableName)
        {
            return new DataFrame(sparkSessionProxy.Table(tableName), SparkContext);
        }

        /// <summary>
        /// Executes a SQL query using Spark, returning the result as a DataFrame. The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'
        /// </summary>
        /// <param name="sqlQuery"></param>
        /// <returns></returns>
        public DataFrame Sql(string sqlQuery)
        {
            logger.LogInfo("SQL query to execute on the dataframe is {0}", sqlQuery);
            return new DataFrame(sparkSessionProxy.Sql(sqlQuery), SparkContext);
        }
    }
}
