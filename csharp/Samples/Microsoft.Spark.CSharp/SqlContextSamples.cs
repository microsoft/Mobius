// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using Microsoft.Spark.CSharp.Sql;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Microsoft.Spark.CSharp.Samples
{
    class SqlContextSamples
    {
        private const string PeopleJson = @"people.json";
        private const string RequestsLog = @"requestslog.txt";

        private static SqlContext sqlContext;

        private static SqlContext GetSqlContext()
        {
            return sqlContext ?? (sqlContext = new SqlContext(SparkCLRSamples.SparkContext));
        }

        /// <summary>
        /// Sample to GetOrCreate a sql context
        /// </summary>
        [Sample]
        internal static void SqlContextGetOrCreateSample()
        {
            // create a new SqlContext
            var sqlContext = SqlContext.GetOrCreate(SparkCLRSamples.SparkContext);
            // get existing SqlContext
            var getOrCreatedSqlContext = SqlContext.GetOrCreate(SparkCLRSamples.SparkContext);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(sqlContext, getOrCreatedSqlContext);
            }
        }

        /// <summary>
        /// Sample to create DataFrame. The RDD is generated from SparkContext Parallelize; the schema is created via object creating.
        /// </summary>
        [Sample]
        internal static void SqlContextCreateDataFrameSample()
        {
            var schemaPeople = new StructType(new List<StructField>
                                        {
                                            new StructField("id", new StringType()),
                                            new StructField("name", new StringType()),
                                            new StructField("age", new IntegerType()),
                                            new StructField("address", new StructType(new List<StructField>
                                                                                      {
                                                                                          new StructField("city", new StringType()),
                                                                                          new StructField("state", new StringType())
                                                                                      })),
                                            new StructField("phone numbers", new ArrayType(new StringType()))
                                        });

            var rddPeople = SparkCLRSamples.SparkContext.Parallelize(
                                    new List<object[]>
                                    {
                                        new object[] { "123", "Bill", 43, new object[]{ "Columbus", "Ohio" }, new string[]{ "Tel1", "Tel2" } },
                                        new object[] { "456", "Steve", 34,  new object[]{ "Seattle", "Washington" }, new string[]{ "Tel3", "Tel4" } }
                                    });

            var dataFramePeople = GetSqlContext().CreateDataFrame(rddPeople, schemaPeople);
            Console.WriteLine("------ Schema of People Data Frame:\r\n");
            dataFramePeople.ShowSchema();
            Console.WriteLine();
            var collected = dataFramePeople.Collect().ToArray();
            foreach (var people in collected)
            {
                string id = people.Get("id");
                string name = people.Get("name");
                int age = people.Get("age");
                Row address = people.Get("address");
                string city = address.Get("city");
                string state = address.Get("state");
                object[] phoneNumbers = people.Get("phone numbers");
                Console.WriteLine("id:{0}, name:{1}, age:{2}, address:(city:{3},state:{4}), phoneNumbers:[{5},{6}]\r\n", id, name, age, city, state, phoneNumbers[0], phoneNumbers[1]);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, dataFramePeople.Rdd.Count());
                Assert.AreEqual(schemaPeople.Json, dataFramePeople.Schema.Json);
            }
        }

        /// <summary>
        /// Sample to create DataFrame. The RDD is generated from SparkContext TextFile; the schema is created from Json.
        /// </summary>
        [Sample]
        internal static void SqlContextCreateDataFrameSample2()
        {
            var rddRequestsLog = SparkCLRSamples.SparkContext.TextFile(SparkCLRSamples.Configuration.GetInputDataPath(RequestsLog), 1).Map(r => r.Split(',').Select(s => (object)s).ToArray());

            const string schemaRequestsLogJson = @"{
	                                            ""fields"": [{
		                                            ""metadata"": {},
		                                            ""name"": ""guid"",
		                                            ""nullable"": false,
		                                            ""type"": ""string""
	                                            },
	                                            {
		                                            ""metadata"": {},
		                                            ""name"": ""datacenter"",
		                                            ""nullable"": false,
		                                            ""type"": ""string""
	                                            },
	                                            {
		                                            ""metadata"": {},
		                                            ""name"": ""abtestid"",
		                                            ""nullable"": false,
		                                            ""type"": ""string""
	                                            },
	                                            {
		                                            ""metadata"": {},
		                                            ""name"": ""traffictype"",
		                                            ""nullable"": false,
		                                            ""type"": ""string""
	                                            }],
	                                            ""type"": ""struct""
                                              }";

            // create schema from parsing Json
            StructType requestsLogSchema = DataType.ParseDataTypeFromJson(schemaRequestsLogJson) as StructType;
            var dataFrameRequestsLog = GetSqlContext().CreateDataFrame(rddRequestsLog, requestsLogSchema);

            Console.WriteLine("------ Schema of RequestsLog Data Frame:");
            dataFrameRequestsLog.ShowSchema();
            Console.WriteLine();
            var collected = dataFrameRequestsLog.Collect().ToArray();
            foreach (var request in collected)
            {
                string guid = request.Get("guid");
                string datacenter = request.Get("datacenter");
                string abtestid = request.Get("abtestid");
                string traffictype = request.Get("traffictype");
                Console.WriteLine("guid:{0}, datacenter:{1}, abtestid:{2}, traffictype:{3}\r\n", guid, datacenter, abtestid, traffictype);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(10, collected.Length);
                Assert.AreEqual(Regex.Replace(schemaRequestsLogJson, @"\s", string.Empty), Regex.Replace(dataFrameRequestsLog.Schema.Json, @"\s", string.Empty));
            }
        }

        /// <summary>
        /// Sample to create new session from a SqlContext
        /// </summary>
        [Sample]
        internal static void SqlContextNewSessionSample()
        {
            var originalSqlContext = GetSqlContext();
            var newSession = originalSqlContext.NewSession();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                originalSqlContext.SetConf("key", "value_1");
                newSession.SetConf("key", "value_2");
                Assert.AreEqual("value_1", originalSqlContext.GetConf("key", "defaultValue"));
                Assert.AreEqual("value_2", newSession.GetConf("key", "defaultValue"));
            }
        }

        /// <summary>
        /// Sample to register DataFrame as temp table/show tableNames/drop table
        /// </summary>
        [Sample]
        internal static void SqlContextTempTableSample()
        {
            var sqlContext = GetSqlContext();
            var peopleDataFrame = sqlContext.Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            // register a DataFrame as temp table
            sqlContext.RegisterDataFrameAsTable(peopleDataFrame, "people");

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var tableNames = sqlContext.TableNames().ToArray();
                Assert.AreEqual(1, tableNames.Length);
                Assert.IsTrue(tableNames.Contains("people"));
            }

            // Drop a temp table
            sqlContext.DropTempTable("people");

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.False(sqlContext.TableNames().Contains("people"));
            }
        }

        /// <summary>
        /// Sample to show tables
        /// </summary>
        [Sample]
        internal static void SqlContextTablesSample()
        {
            var sqlContext = GetSqlContext();
            var peopleDataFrame = sqlContext.Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            sqlContext.RegisterDataFrameAsTable(peopleDataFrame, "people1");
            sqlContext.RegisterDataFrameAsTable(peopleDataFrame, "people2");

            var tablesDataFrame = sqlContext.Tables();
            tablesDataFrame.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var collected = tablesDataFrame.Collect().ToArray();
                Assert.AreEqual(2, collected.Length);
                Assert.IsTrue(collected.Any(row => "people1" == row.GetAs<string>("tableName") && row.GetAs<bool>("isTemporary")));
                Assert.IsTrue(collected.Any(row => "people2" == row.GetAs<string>("tableName") && row.GetAs<bool>("isTemporary")));
            }
        }

        /// <summary>
        /// Sample to uncache table
        /// </summary>
        [Sample]
        internal static void SqlContextCacheAndUncacheTableSample()
        {
            var sqlContext = GetSqlContext();
            var peopleDataFrame = sqlContext.Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            // register a DataFrame as temp table
            sqlContext.RegisterDataFrameAsTable(peopleDataFrame, "people");

            // cache table
            sqlContext.CacheTable("people");

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(sqlContext.IsCached("people"));
            }

            // uncache table
            sqlContext.UncacheTable("people");

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsFalse(sqlContext.IsCached("people"));
            }
        }

        /// <summary>
        /// Sample to clear cache
        /// </summary>
        [Sample]
        internal static void SqlContextClearCacheTableSample()
        {
            var sqlContext = GetSqlContext();
            var peopleDataFrame = sqlContext.Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            // register a DataFrame as temp table
            sqlContext.RegisterDataFrameAsTable(peopleDataFrame, "people");
            // cache table
            sqlContext.CacheTable("people");

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(sqlContext.IsCached("people"));
            }

            // clear cache
            sqlContext.ClearCache();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsFalse(sqlContext.IsCached("people"));
            }
        }

        /// <summary>
        /// DataFrameReader read json sample
        /// </summary>
        [Sample]
        internal static void SqlContextReadJsonSample()
        {
            // load json
            var peopleDataFrame = GetSqlContext().Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.ShowSchema();
            peopleDataFrame.Show();

            var count1 = peopleDataFrame.Count();
            Console.WriteLine("Line count in people.json: {0}", count1);

            // load json with schema sample
            var peopleDataFrameWitSchema = GetSqlContext().Read().Schema(peopleDataFrame.Schema).Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrameWitSchema.ShowSchema();
            peopleDataFrameWitSchema.Show();

            var count2 = peopleDataFrameWitSchema.Count();
            Console.WriteLine("Line count in people.json: {0}", count2);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(count1, count2);
                Assert.AreEqual(4, count1);
            }
        }

        /// <summary>
        /// DataFrameReader read json with option sample
        /// </summary>
        [Sample]
        internal static void SqlContextReadJsonWithOptionSample()
        {
            // load json with sampling ration option sample
            // set sampling ration to a very little value so that schema probably won't be infered.
            var peopleDataFrame = GetSqlContext().Read().Option("samplingRatio", "0.001").Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.ShowSchema();
            peopleDataFrame.Show();

            var count1 = peopleDataFrame.Count();
            Console.WriteLine("Line count in people.json: {0}", count1);
        }

        /// <summary>
        /// DataFrameReader read parquet sample
        /// </summary>
        [Sample]
        internal static void SqlContextReadParquetSample()
        {
            // save json file to parquet file first
            var peopleDataFrame = GetSqlContext().Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var expectedCount = peopleDataFrame.Count();

            var parquetPath = Path.GetTempPath() + "DF_Parquet_Samples_" + (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalSeconds;
            peopleDataFrame.Coalesce(1).Write().Parquet(parquetPath);

            Console.WriteLine("Save dataframe to parquet: {0}", parquetPath);

            var peopleDataFrame2 = GetSqlContext().Read().Parquet(parquetPath);
            Console.WriteLine("Schema:");
            peopleDataFrame2.ShowSchema();
            peopleDataFrame2.Show();

            var count = peopleDataFrame2.Count();
            Console.WriteLine("Rows in peopleDataFrame2: {0}", count);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(expectedCount, count);
            }

            Directory.Delete(parquetPath, true);
            Console.WriteLine("Remove parquet directory: {0}", parquetPath);
        }
    }
}
