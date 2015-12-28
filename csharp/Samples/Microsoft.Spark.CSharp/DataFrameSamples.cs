// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using NUnit.Framework;

namespace Microsoft.Spark.CSharp.Samples
{
    class DataFrameSamples
    {
        private const string PeopleJson = @"people.json";
        private const string OrderJson = @"order.json";
        private const string RequestsLog = @"requestslog.txt";
        private const string MetricsLog = @"metricslog.txt";
        private const string CSVTestLog = @"csvtestlog.txt";

        private static SqlContext sqlContext;

        private static SqlContext GetSqlContext()
        {
            return sqlContext ?? (sqlContext = new SqlContext(SparkCLRSamples.SparkContext));
        }

        /// <summary>
        /// Sample to create DataFrame. The RDD is generated from SparkContext Parallelize; the schema is created via object creating.
        /// </summary>
        [Sample]
        internal static void DFCreateDataFrameSample()
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
        internal static void DFCreateDataFrameSample2()
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
        /// Sample to show schema of DataFrame
        /// </summary>
        [Sample]
        internal static void DFShowSchemaSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.Explain(true);
            peopleDataFrame.ShowSchema();
        }

        /// <summary>
        /// Sample to get schema of DataFrame in json format
        /// </summary>
        [Sample]
        internal static void DFGetSchemaJsonSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            string json = peopleDataFrame.Schema.Json;
            Console.WriteLine("schema in json format: {0}", json);
        }

        /// <summary>
        /// Sample to run collect for DataFrame
        /// </summary>
        [Sample]
        internal static void DFCollectSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var rows = peopleDataFrame.Collect().ToArray();
            Console.WriteLine("peopleDataFrame:");
            int i = 0;

            foreach (var row in rows)
            {
                if (i == 0)
                {
                    Console.WriteLine("schema: {0}", row.GetSchema());
                }

                // output the whole row as a string
                Console.WriteLine(row);

                // output each field of the row, including fields in subrow
                string id = row.GetAs<string>("id");
                string name = row.GetAs<string>("name");
                int age = row.GetAs<int>("age");
                Console.Write("id: {0}, name: {1}, age: {2}", id, name, age);

                Row address = row.GetAs<Row>("address");
                if (address != null)
                {
                    string city = address.GetAs<string>("city");
                    string state = address.GetAs<string>("state");
                    Console.WriteLine(", state: {0}, city: {1}", state, city);
                }
                else
                {
                    Console.WriteLine();
                }

                i++;
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(4, rows.Length);
            }
        }

        /// <summary>
        /// Sample to register a DataFrame as temptable and run queries
        /// </summary>
        [Sample]
        internal static void DFRegisterTableSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.RegisterTempTable("people");
            var nameFilteredDataFrame = GetSqlContext().Sql("SELECT name, address.city, address.state FROM people where name='Bill'");
            var countDataFrame = GetSqlContext().Sql("SELECT count(name) FROM people where name='Bill'");
            var maxAgeDataFrame = GetSqlContext().Sql("SELECT max(age) FROM people where name='Bill'");
            long maxAgeDataFrameRowsCount = maxAgeDataFrame.Count();
            long nameFilteredDataFrameRowsCount = nameFilteredDataFrame.Count();
            long countDataFrameRowsCount = countDataFrame.Count();
            Console.WriteLine("nameFilteredDataFrameRowsCount={0}, maxAgeDataFrameRowsCount={1}, countDataFrameRowsCount={2}", nameFilteredDataFrameRowsCount, maxAgeDataFrameRowsCount, countDataFrameRowsCount);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(1, maxAgeDataFrameRowsCount);
                Assert.AreEqual(2, nameFilteredDataFrameRowsCount);
                Assert.AreEqual(1, countDataFrameRowsCount);
            }
        }

        /// <summary>
        /// Sample to load a text file as dataframe
        /// </summary>
        [Sample]
        internal static void DFTextFileLoadDataFrameSample()
        {
            var requestsSchema = new StructType(new List<StructField>
                                                {
                                                    new StructField("guid", new StringType(), false), 
                                                    new StructField("datacenter", new StringType(), false), 
                                                    new StructField("abtestid", new StringType(), false), 
                                                    new StructField("traffictype", new StringType(), false), 
                                                });

            var requestsDateFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(RequestsLog), requestsSchema);
            requestsDateFrame.RegisterTempTable("requests");
            var guidFilteredDataFrame = GetSqlContext().Sql("SELECT guid, datacenter FROM requests where guid = '4628deca-139d-4121-b540-8341b9c05c2a'");
            guidFilteredDataFrame.Show();

            requestsDateFrame.ShowSchema();
            requestsDateFrame.Show();
            var count = requestsDateFrame.Count();

            guidFilteredDataFrame.ShowSchema();
            guidFilteredDataFrame.Show();
            var filteredCount = guidFilteredDataFrame.Count();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(10, count);
                Assert.AreEqual(1, filteredCount);
            }
        }
        
        /// <summary>
        /// Sample to load two text files and join them using temptable constructs
        /// </summary>
        [Sample]
        internal static void DFTextFileJoinTempTableSample()
        {
            var requestsDataFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(RequestsLog));
            var metricsDateFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(MetricsLog));
            metricsDateFrame.ShowSchema();

            requestsDataFrame.RegisterTempTable("requests");
            metricsDateFrame.RegisterTempTable("metrics");

            //C0 - guid in requests DF, C3 - guid in metrics DF
            var join = GetSqlContext().Sql(
                        "SELECT joinedtable.datacenter, max(joinedtable.latency) maxlatency, avg(joinedtable.latency) avglatency " +
                        "FROM (SELECT a.C1 as datacenter, b.C6 as latency from requests a JOIN metrics b ON a.C0  = b.C3) joinedtable " +
                        "GROUP BY datacenter");
            
            join.ShowSchema();
            join.Show();
            var count = join.Count();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(4, count);
            }
        }

        /// <summary>
        /// Sample to load two text files and join them using DataFrame DSL
        /// </summary>
        [Sample]
        internal static void DFTextFileJoinTableDSLSample()
        {
            //C0 - guid, C1 - datacenter
            var requestsDataFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(RequestsLog)).Select("C0", "C1");
            //C3 - guid, C6 - latency
            var metricsDateFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(MetricsLog), ",", false, true).Select("C3", "C6"); //override delimiter, hasHeader & inferSchema

            var joinDataFrame = requestsDataFrame.Join(metricsDateFrame, requestsDataFrame["C0"] == metricsDateFrame["C3"]).GroupBy("C1");
            var maxLatencyByDcDataFrame = joinDataFrame.Agg(new Dictionary<string, string> { { "C6", "max" } });
            var avgLatencyByDcDataFrame = joinDataFrame.Agg(new Dictionary<string, string> { { "C6", "avg" } });

            maxLatencyByDcDataFrame.ShowSchema();
            maxLatencyByDcDataFrame.Show();
            
            avgLatencyByDcDataFrame.ShowSchema();
            avgLatencyByDcDataFrame.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEquivalent(new[] { "C1", "max(C6)" }, maxLatencyByDcDataFrame.Schema.Fields.Select(f => f.Name).ToArray());
                CollectionAssert.AreEquivalent(new[] { "iowa", "texas", "singapore", "ireland" }, maxLatencyByDcDataFrame.Collect().Select(row => row.Get("C1")).ToArray());

                CollectionAssert.AreEquivalent(new[] { "C1", "avg(C6)" }, avgLatencyByDcDataFrame.Schema.Fields.Select(f => f.Name).ToArray());
                CollectionAssert.AreEquivalent(new[] { "iowa", "texas", "singapore", "ireland" }, avgLatencyByDcDataFrame.Collect().Select(row => row.Get("C1")).ToArray());
            }
        }

        /// <summary>
        /// Sample to iterate on the schema of DataFrame
        /// </summary>
        [Sample]
        internal static void DFSchemaSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var peopleDataFrameSchema = peopleDataFrame.Schema;
            var peopleDataFrameSchemaFields = peopleDataFrameSchema.Fields;
            foreach (var peopleDataFrameSchemaField in peopleDataFrameSchemaFields)
            {
                var name = peopleDataFrameSchemaField.Name;
                var dataType = peopleDataFrameSchemaField.DataType;
                var stringVal = dataType.TypeName;
                var simpleStringVal = dataType.SimpleString;
                var isNullable = peopleDataFrameSchemaField.IsNullable;
                Console.WriteLine("Name={0}, DT.string={1}, DT.simplestring={2}, DT.isNullable={3}", name, stringVal, simpleStringVal, isNullable);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual("address:struct<city:string,state:string>", peopleDataFrameSchemaFields[0].SimpleString);
                Assert.AreEqual("age:long", peopleDataFrameSchemaFields[1].SimpleString);
                Assert.AreEqual("id:string", peopleDataFrameSchemaFields[2].SimpleString);
                Assert.AreEqual("name:string", peopleDataFrameSchemaFields[3].SimpleString);
            }
        }

        /// <summary>
        /// Sample to convert DataFrames to RDD
        /// </summary>
        [Sample]
        internal static void DFConversionSample()
        {
            // repartitioning below so that batching in pickling does not impact count on Rdd created from the dataframe
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson)).Repartition(4);
            var stringRddCreatedFromDataFrame = peopleDataFrame.ToJSON();
            var stringRddCreatedFromDataFrameRowCount = stringRddCreatedFromDataFrame.Count();

            var byteArrayRddCreatedFromDataFrame = peopleDataFrame.ToRDD();
            var byteArrayRddCreatedFromDataFrameRowCount = byteArrayRddCreatedFromDataFrame.Count();

            Console.WriteLine("stringRddCreatedFromDataFrameRowCount={0}, byteArrayRddCreatedFromDataFrameRowCount={1}", stringRddCreatedFromDataFrameRowCount, byteArrayRddCreatedFromDataFrameRowCount);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(4, stringRddCreatedFromDataFrameRowCount);
                Assert.AreEqual(4, byteArrayRddCreatedFromDataFrameRowCount);
            }
        }

        /// <summary>
        /// Sample to perform simple select and filter on DataFrame using DSL
        /// </summary>
        [Sample]
        internal static void DFProjectionFilterDSLSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson)); 
            var projectedFilteredDataFrame = peopleDataFrame.Select("name", "address.state")
                                                    .Where("name = 'Bill' or state = 'California'");
            projectedFilteredDataFrame.ShowSchema();
            projectedFilteredDataFrame.Show();
            var projectedFilteredDataFrameCount = projectedFilteredDataFrame.Count();
            projectedFilteredDataFrame.RegisterTempTable("selectedcolumns");
            var sqlCountDataFrame = GetSqlContext().Sql("SELECT count(name) FROM selectedcolumns where name='Bill'");
            var sqlCountDataFrameRowCount = sqlCountDataFrame.Count();
            Console.WriteLine("projectedFilteredDataFrameCount={0}, sqlCountDataFrameRowCount={1}", projectedFilteredDataFrameCount, sqlCountDataFrameRowCount);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEqual(new[] { "name", "state" }, projectedFilteredDataFrame.Schema.Fields.Select(f => f.Name).ToArray());
                Assert.IsTrue(projectedFilteredDataFrame.Collect().All(row => row.Get("name") == "Bill" || row.Get("state") == "California"));
                Assert.AreEqual(2, sqlCountDataFrame.Collect().ToArray()[0].Get(0));
            }
        }

        /// <summary>
        /// Sample to join DataFrames using DSL
        /// </summary>
        [Sample]
        internal static void DFJoinSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var orderDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(OrderJson));

            var expressionJoin = peopleDataFrame.Join(orderDataFrame, peopleDataFrame["id"] == orderDataFrame["personid"]);
            expressionJoin.ShowSchema();
            expressionJoin.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var collected = expressionJoin.Collect().ToArray();
                Assert.AreEqual(2, collected.Length);
                Assert.IsTrue(collected.Any(row => "123" == row.Get("id")));
                Assert.IsTrue(collected.Any(row => "456" == row.Get("id")));
            }
        }

        /// <summary>
        /// Sample to join DataFrames using DSL
        /// </summary>
        [Sample]
        internal static void DFJoinMultiColSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var multiColumnJoin = peopleDataFrame.Join(peopleDataFrame, new string[] { "name", "id" });
            multiColumnJoin.ShowSchema();
            multiColumnJoin.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var schema = multiColumnJoin.Schema;
                CollectionAssert.AreEquivalent(new[] { "name", "id", "age", "address", "age", "address" }, schema.Fields.Select(f => f.Name.ToLower()).ToArray());
            }
        }

        /// <summary>
        /// Sample to intersect DataFrames using DSL.
        /// </summary>
        [Sample]
        internal static void DFIntersectSample()
        {
            var peopleDataFrame1 = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var peopleDataFrame2 = peopleDataFrame1.Filter("name = 'Bill'");

            var intersected = peopleDataFrame1.Intersect(peopleDataFrame2);
            intersected.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                // intersected should have the same items as peopleDataFrame2
                AreRowListEquivalent(peopleDataFrame2.Rdd.Collect().ToList(), intersected.Rdd.Collect().ToList());
            }
        }

        /// <summary>
        /// Sample to unionAll DataFrames using DSL.
        /// </summary>
        [Sample]
        internal static void DFUnionAllSample()
        {
            var peopleDataFrame1 = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var peopleDataFrame2 = peopleDataFrame1.Filter("name = 'Bill'");

            var unioned = peopleDataFrame1.UnionAll(peopleDataFrame2);
            unioned.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                // unioned should have the same items as peopleDataFrame1
                AreRowListEquivalent(peopleDataFrame1.Rdd.Collect().ToList(), unioned.Rdd.Collect().ToList());
            }
        }

        /// <summary>
        /// Sample to subtract a DataFrame from another DataFrame using DSL.
        /// </summary>
        [Sample]
        internal static void DFSubtractSample()
        {
            var peopleDataFrame1 = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var peopleDataFrame2 = peopleDataFrame1.Filter("name = 'Bill'");

            var subtracted = peopleDataFrame1.Subtract(peopleDataFrame2);
            subtracted.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                // subtracted should have items peopleDataFrame1 have but peopleDataFrame2 not have
                AreRowListEquivalent(peopleDataFrame1.Rdd.Collect().Where(row => row.Get("name") != "Bill").ToList(), subtracted.Rdd.Collect().ToList());
            }
        }

        /// <summary>
        /// Sample to drop a column from DataFrame using DSL.
        /// </summary>
        [Sample]
        internal static void DFDropSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            var dropped = peopleDataFrame.Drop("name");
            dropped.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEquivalent(new[] { "id", "age", "address" }, dropped.Schema.Fields.Select(f => f.Name).ToArray());
            }
        }

        /// <summary>
        /// Sample to drop a DataFrame with omitting rows with null values using DSL.
        /// </summary>
        [Sample]
        internal static void DFDropNaSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            var dropped = peopleDataFrame.DropNa(thresh: 2, subset: new[] { "name", "address" });
            dropped.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var expected = peopleDataFrame.Collect().Where(row => row.Get("name") != null && row.Get("address") != null).ToList();
                Assert.IsTrue(AreRowListEquivalent(expected, dropped.Collect().ToList()));
            }
        }

        /// <summary>
        /// Sample to fill value to Na fields of a dataframe.
        /// </summary>
        [Sample]
        internal static void DFFillNaSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var count = peopleDataFrame.Count();
            Console.WriteLine("Records in people dataframe: {0}", count);

            peopleDataFrame = peopleDataFrame.WithColumn("mail", Functions.Lit(null).Cast("string"))
                .WithColumn("height", Functions.Lit(null).Cast("int"));

            const string unknownMail = "unknown mail";

            var filledMailDataFrame = peopleDataFrame.FillNa(unknownMail, new[] { "mail" });
            Console.WriteLine("Filled null [mail] field with '{0}'", unknownMail);
            filledMailDataFrame.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var expected = filledMailDataFrame.Collect().Where(row => (string)row.Get("mail") == unknownMail).Select(row => row.Get("mail")).Count();
                Assert.AreEqual(count, expected);
            }

            const int unknownHeight = -1;
            var filledHeightDataFrame = peopleDataFrame.FillNa(unknownHeight, new[] { "height" });
            Console.WriteLine("Filled null [mail] and [height] fields with '{0}'", unknownHeight);
            filledHeightDataFrame.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var expected = filledHeightDataFrame.Collect().Where(row => (int)row.Get("height") == unknownHeight)
                    .Select(row => row.Get("height")).Count();
                Assert.AreEqual(count, expected);
            }
        }

        /// <summary>
        /// Sample to fill value to Na fields of a dataframe using dict
        /// </summary>
        [Sample]
        internal static void DFFillNaSampleWithDict()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var count = peopleDataFrame.Count();
            Console.WriteLine("Records in people dataframe: {0}", count);

            peopleDataFrame = peopleDataFrame.WithColumn("mail", Functions.Lit(null).Cast("string"))
                .WithColumn("height", Functions.Lit(null).Cast("int"));

            const string unknownMail = "unknown mail";
            const int unknownHeight = -1;

            var filledDataFrame = peopleDataFrame.FillNa(new Dictionary<string, object>() { { "mail", unknownMail }, { "height", unknownHeight } });
            Console.WriteLine("Filled null [mail] and [height] fields with '{0}' and '{1}'", unknownMail, unknownHeight);
            filledDataFrame.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var expected = filledDataFrame.Collect().Count(row => (int)row.Get("height") == unknownHeight && (string)row.Get("mail") == unknownMail);
                Assert.AreEqual(count, expected);
            }
        }

        /// <summary>
        /// Sample to drop rows containing any null values.
        /// </summary>
        [Sample]
        internal static void DFNaDropSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var dfCount = peopleDataFrame.Count();
            Console.WriteLine("Before drop operation, # of rows: {0}", dfCount);
            peopleDataFrame.Show();

            var peopleDataFrameWithoutNull = peopleDataFrame.Na().Drop();
            var dfCount2 = peopleDataFrameWithoutNull.Count();
            Console.WriteLine("After drop operation, # of rows: {0}", dfCount2);
            peopleDataFrameWithoutNull.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(dfCount > dfCount2);
            }
        }

        /// <summary>
        /// Sample to fill rows containing null values.
        /// </summary>
        [Sample]
        internal static void DFNaFillSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson))
                .WithColumn("mail", Functions.Lit(null).Cast("string"));
            Console.WriteLine("Before fill operation:");
            peopleDataFrame.Show();

            var filledDataFrame = peopleDataFrame.Na().Fill("unknown mail");
            Console.WriteLine("After fill operation:");
            filledDataFrame.Show();
        }

        /// <summary>
        /// Sample to replace rows containing null values.
        /// </summary>
        [Sample]
        internal static void DFNaReplaceSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            Console.WriteLine("Before replace operation:");
            peopleDataFrame.Show();

            var replacement = new Dictionary<string, string>()
            {
                {"Bill", "BILL"},
                {"123", "I II III"}
            };
            var filledDataFrame = peopleDataFrame.Na().Replace(new[] { "name", "id" }, replacement);
            Console.WriteLine("After replace operation:");
            filledDataFrame.Show();
        }

        /// <summary>
        /// Sample to drop a duplicated rows in a DataFrame.
        /// </summary>
        [Sample]
        internal static void DFDropDuplicatesSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            var dropped = peopleDataFrame.DropDuplicates(new[] { "name" });
            dropped.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(1, dropped.Collect().Count(row => row.Get("name") == "Bill"));
            }
        }

        /// <summary>
        /// Sample to replace a value in a DataFrame.
        /// </summary>
        [Sample]
        internal static void DFReplaceSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            var singleValueReplaced = peopleDataFrame.Replace("Bill", "Bill.G");
            singleValueReplaced.Show();

            var multiValueReplaced = peopleDataFrame.ReplaceAll(new List<int> { 14, 34 }, new List<int> { 44, 54 });
            multiValueReplaced.Show();

            var multiValueReplaced2 = peopleDataFrame.ReplaceAll(new List<string> { "Bill", "Steve" }, "former CEO");
            multiValueReplaced2.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(AreRowListEquivalent(peopleDataFrame.Collect().ToList(), singleValueReplaced.Collect().ToList(),
                                new Dictionary<string, Func<object, object, bool>>
                            {
                                {"name", (x, y) => x.ToString() == "Bill" ? 
                                    y.ToString() == "Bill.G" : x.ToString() == y.ToString()}
                            }));


                Assert.IsTrue(AreRowListEquivalent(peopleDataFrame.Collect().ToList(), multiValueReplaced.Collect().ToList(),
                            new Dictionary<string, Func<object, object, bool>>
                            {
                                {"age", (x, y) =>
                                        {
                                            if ((int)x == 14) return (int)y == 44;
                                            if ((int)x == 34) return (int)y == 54;
                                            return (int)x == (int)y;
                                        }
                                }
                            }));

                Assert.IsTrue(AreRowListEquivalent(peopleDataFrame.Collect().ToList(), multiValueReplaced2.Collect().ToList(),
                            new Dictionary<string, Func<object, object, bool>>
                            {
                                {"name", (x, y) => x.ToString() == "Bill" || x.ToString() == "Steve"? 
                                    y.ToString() == "former CEO" : x.ToString() == y.ToString()}
                            }));
            }
        }

        /// <summary>
        /// Sample to perform randomSplit on DataFrame
        /// </summary>
        [Sample]
        internal static void DFRandomSplitSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var randomSplitted = peopleDataFrame.RandomSplit(new[] { 1.0, 2.0 }).ToArray();

            randomSplitted[0].Show();
            randomSplitted[1].Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(peopleDataFrame.Count(), randomSplitted[0].Count() + randomSplitted[1].Count());
            }
        }

        /// <summary>
        /// Sample to perform Columns on DataFrame
        /// </summary>
        [Sample]
        internal static void DFColumnsSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var columnNames = peopleDataFrame.Columns().ToArray();

            Console.WriteLine("Columns:");
            Console.WriteLine(string.Join(", ", columnNames));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(4, columnNames.Length);
                Assert.IsTrue(columnNames.Contains("id"));
                Assert.IsTrue(columnNames.Contains("name"));
                Assert.IsTrue(columnNames.Contains("age"));
                Assert.IsTrue(columnNames.Contains("address"));
            }
        }

        /// <summary>
        /// Sample to perform DTypes on DataFrame
        /// </summary>
        [Sample]
        internal static void DFDTypesSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var columnsNameAndType = peopleDataFrame.DTypes().ToArray();

            Console.WriteLine("Columns name and type:");
            Console.WriteLine(string.Join(", ", columnsNameAndType.Select(c => string.Format("{0}: {1}", c.Item1, c.Item2))));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(4, columnsNameAndType.Length);
                Assert.IsTrue(columnsNameAndType.Any(c => c.Item1 == "id" && c.Item2 == "string"));
                Assert.IsTrue(columnsNameAndType.Any(c => c.Item1 == "name" && c.Item2 == "string"));
                Assert.IsTrue(columnsNameAndType.Any(c => c.Item1 == "age" && c.Item2 == "bigint"));
                Assert.IsTrue(columnsNameAndType.Any(c => c.Item1 == "address" && c.Item2 == "struct<city:string,state:string>"));
            }
        }

        /// <summary>
        /// Sample to perform Sort on DataFrame
        /// </summary>
        [Sample]
        internal static void DFSortSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var sorted = peopleDataFrame.Sort(new string[] { "name", "age" }, new bool[] { true, false });

            sorted.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var sortedDF = sorted.Collect().ToArray();
                Assert.AreEqual("789", sortedDF[0].GetAs<string>("id"));
                Assert.AreEqual("123", sortedDF[1].GetAs<string>("id"));
                Assert.AreEqual("531", sortedDF[2].GetAs<string>("id"));
                Assert.AreEqual("456", sortedDF[3].GetAs<string>("id"));
            }

            var sorted2 = peopleDataFrame.Sort(new Column[] { peopleDataFrame["name"].Asc(), peopleDataFrame["age"].Desc() });

            sorted2.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var sortedDF2 = sorted2.Collect().ToArray();
                Assert.AreEqual("789", sortedDF2[0].GetAs<string>("id"));
                Assert.AreEqual("123", sortedDF2[1].GetAs<string>("id"));
                Assert.AreEqual("531", sortedDF2[2].GetAs<string>("id"));
                Assert.AreEqual("456", sortedDF2[3].GetAs<string>("id"));
            }
        }

        /// <summary>
        /// Sample to perform Alias on DataFrame
        /// </summary>
        [Sample]
        internal static void DFAliasSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var dfAs1 = peopleDataFrame.Alias("df_as1");
            var dfAs2 = peopleDataFrame.Alias("df_as2");
            var joined = dfAs1.Join(dfAs2, dfAs1["df_as1.name"] == dfAs2["df_as2.name"], JoinType.Inner);
            var joined2 = joined.Select("df_as1.name", "df_as2.age");

            joined2.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var sortedDF = joined2.Collect().ToArray();
                Assert.AreEqual(6, sortedDF.Length);
                Assert.IsTrue(sortedDF.Count(row => row.GetAs<string>("name") == "Bill" && row.GetAs<int>("age") == 34) == 2);
                Assert.IsTrue(sortedDF.Count(row => row.GetAs<string>("name") == "Bill" && row.GetAs<int>("age") == 43) == 2);
                Assert.IsTrue(sortedDF.Count(row => row.GetAs<string>("name") == "Steve" && row.GetAs<int>("age") == 14) == 1);
                Assert.IsTrue(sortedDF.Count(row => row.GetAs<string>("name") == "Satya" && row.GetAs<int>("age") == 46) == 1);
            }
        }

        /// <summary>
        /// Sample to perform Select on DataFrame
        /// </summary>
        [Sample]
        internal static void DFSelectSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var projected = peopleDataFrame.Select(peopleDataFrame["name"], "age");

            projected.Show();
            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var columns = projected.Columns().ToArray();
                Assert.AreEqual(2, columns.Length);
                Assert.IsTrue(columns.Contains("name"));
                Assert.IsTrue(columns.Contains("age"));
            }
        }

        /// <summary>
        /// Sample to perform WithColumn on DataFrame
        /// </summary>
        [Sample]
        internal static void DFWithColumnSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var dfWithNewColumn = peopleDataFrame.WithColumn("age2", peopleDataFrame["age"] + 10);

            dfWithNewColumn.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                foreach (var row in dfWithNewColumn.Collect())
                {
                    Assert.AreEqual(row.GetAs<int>("age") + 10, row.GetAs<int>("age2"));
                }
            }
        }

        /// <summary>
        /// Sample to perform WithColumnRenamed on DataFrame
        /// </summary>
        [Sample]
        internal static void DFWithColumnRenamedSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var dfWithNewColumnName = peopleDataFrame.WithColumnRenamed("age", "age2");

            dfWithNewColumnName.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var columns = dfWithNewColumnName.Columns().ToArray();
                Assert.IsTrue(columns.Contains("age2"));
            }
        }

        /// <summary>
        /// Sample to perform Corr on DataFrame
        /// </summary>
        [Sample]
        internal static void DFCorrSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            double corr = peopleDataFrame.Corr("age", "age");
            Console.WriteLine("Corr of column age and age is " + corr);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(1.0, corr);
            }
        }

        /// <summary>
        /// Sample to perform Cov on DataFrame
        /// </summary>
        [Sample]
        internal static void DFCovSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            double cov = peopleDataFrame.Cov("age", "age");
            Console.WriteLine("Cov of column age and age is " + cov);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(208.25, cov);
            }
        }

        /// <summary>
        /// Sample to perform FreqItems on DataFrame
        /// </summary>
        [Sample]
        internal static void DFFreqItemsSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            DataFrame freqItems = peopleDataFrame.FreqItems(new []{"name"}, 0.4);
            freqItems.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var collected = freqItems.Collect().ToArray();
                Assert.AreEqual(1, collected.Length);
                Assert.AreEqual("Bill", collected[0].Get(0)[0].ToString());
            }
        }

        /// <summary>
        /// Sample to perform Describe on DataFrame
        /// </summary>
        [Sample]
        internal static void DFDescribeSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            DataFrame descDF = peopleDataFrame.Describe();
            descDF.Show();

            DataFrame descDF2 = peopleDataFrame.Describe("age", "name");
            descDF2.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var collectedDesc = descDF.Collect().ToArray();
                Assert.AreEqual("4", collectedDesc.First(row => row.GetAs<string>("summary") == "count").GetAs<string>("age"));
                Assert.AreEqual("34.25", collectedDesc.First(row => row.GetAs<string>("summary") == "mean").GetAs<string>("age"));

                var collectedDesc2 = descDF2.Collect().ToArray();
                Assert.AreEqual("4", collectedDesc2.First(row => row.GetAs<string>("summary") == "count").GetAs<string>("age"));
                Assert.AreEqual("Bill", collectedDesc2.First(row => row.GetAs<string>("summary") == "min").GetAs<string>("name"));
            }
        }

        /// <summary>
        /// Sample to perform Rollup on DataFrame
        /// </summary>
        [Sample]
        internal static void DFRollupSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            DataFrame rollupDF = peopleDataFrame.Rollup("name", "age").Count();
            rollupDF.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var collectedRollup = rollupDF.Collect().ToArray();
                Assert.AreEqual(8, collectedRollup.Length);
            }
        }

        /// <summary>
        /// Sample to perform Cube on DataFrame
        /// </summary>
        [Sample]
        internal static void DFCubeSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            DataFrame cubeDF = peopleDataFrame.Cube("name", "age").Count();
            cubeDF.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var collectedCube = cubeDF.Collect().ToArray();
                Assert.AreEqual(12, collectedCube.Length);
            }
        }

        /// <summary>
        /// Sample to perform Cube on DataFrame
        /// </summary>
        [Sample]
        internal static void DFGroupDataOperationSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            GroupedData gd = peopleDataFrame.GroupBy("name");
            var countDF = gd.Count();
            countDF.Show();
            var maxDF = gd.Max();
            maxDF.Show();
            var minDF = gd.Min();
            minDF.Show();
            var meanDF = gd.Mean();
            meanDF.Show();
            var avgDF = gd.Avg();
            avgDF.Show();


            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, (int)(countDF.Collect().First(col => ((string)col.Get(0) == "Bill")).Get(1)));
                Assert.AreEqual(43, (int)(maxDF.Collect().First(col => ((string)col.Get(0) == "Bill")).Get(1)));
                Assert.AreEqual(34, (int)(minDF.Collect().First(col => ((string)col.Get(0) == "Bill")).Get(1)));
                Assert.AreEqual(38.5, (double)(meanDF.Collect().First(col => ((string)col.Get(0) == "Bill")).Get(1)));
                Assert.AreEqual(38.5, (double)(avgDF.Collect().First(col => ((string)col.Get(0) == "Bill")).Get(1)));
            }
        }

        /// <summary>
        /// Sample to perform Crosstab on DataFrame
        /// </summary>
        [Sample]
        internal static void DFCrosstabSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            DataFrame crosstab = peopleDataFrame.Crosstab("name", "age");
            crosstab.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var collectedCrosstab = crosstab.Collect();
                CollectionAssert.AreEquivalent(new[] { "name_age", "14", "46", "34", "43" }, crosstab.Columns().ToArray());
                CollectionAssert.AreEquivalent(new[] { "Bill", "Satya", "Steve" }, collectedCrosstab.Select(row => row.GetAs<string>("name_age")).ToArray());
            }
        }

        /// <summary>
        /// Sample to perform aggregation on DataFrame using DSL
        /// </summary>
        [Sample]
        internal static void DFAggregateDSLSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var countAggDataFrame = peopleDataFrame.Where("name = 'Bill'").Agg(new Dictionary<string, string> {{"name", "count"}});
            var countAggDataFrameCount = countAggDataFrame.Count();
            var maxAggDataFrame = peopleDataFrame.GroupBy("name").Agg(new Dictionary<string, string> {{"age", "max"}});
            var maxAggDataFrameCount = maxAggDataFrame.Count();
            Console.WriteLine("countAggDataFrameCount: {0}, maxAggDataFrameCount: {1}.", countAggDataFrameCount, maxAggDataFrameCount);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(1, countAggDataFrameCount);
                Assert.AreEqual(2, countAggDataFrame.Collect().ToArray()[0].Get("count(name)"));
                Assert.AreEqual(3, maxAggDataFrameCount);
                Assert.AreEqual(43, maxAggDataFrame.Collect().ToArray().First(row => row.Get("name") == "Bill").Get("max(age)"));
            }
        }

        /// <summary>
        /// Sample to perform simple select and filter on DataFrame using UDF
        /// </summary>
        [Sample]
        internal static void DFProjectionFilterUDFSample()
        {
            GetSqlContext().RegisterFunction<string, string, string>("FullAddress", (city, state) => city + " " + state);
            GetSqlContext().RegisterFunction<bool, string, int>("PeopleFilter", (name, age) => name == "Bill" && age > 80);

            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            // DataFrame query
            var functionAppliedDF = peopleDataFrame.SelectExpr("name", "age * 2 as age", "FullAddress(address.city, address.state) as address")
                .Where("PeopleFilter(name, age)");

            functionAppliedDF.ShowSchema();
            functionAppliedDF.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var collected = functionAppliedDF.Collect().ToArray();
                CollectionAssert.AreEquivalent(new[] { "name", "age", "address" }, functionAppliedDF.Schema.Fields.Select(f => f.Name).ToArray());
                Assert.AreEqual(1, collected.Length);
                Assert.AreEqual("Bill", collected[0].Get("name"));
                Assert.AreEqual(86, collected[0].Get("age"));
                Assert.AreEqual("Seattle Washington", collected[0].Get("address"));
            }
        }

        /// <summary>
        /// Sample to perform limit on DataFrame using DSL.
        /// </summary>
        [Sample]
        internal static void DFLimitSample()
        {
            const int num = 2;
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var peopleDataFrame2 = peopleDataFrame.Sort(new string[] { "id" }, new bool[] { true }).Limit(num);

            PrintAndVerifyPeopleDataFrameRows(peopleDataFrame2.Head(num), num);
        }

        /// <summary>
        /// Sample to run head for DataFrame
        /// </summary>
        [Sample]
        internal static void DFHeadSample()
        {
            const int num = 3;
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var rows = peopleDataFrame.Sort(new string[] { "id" }, new bool[] { true }).Head(num);

            PrintAndVerifyPeopleDataFrameRows(rows, num);
        }

        [Sample]
        internal static void DFTakeSample()
        {
            const int num = 2;
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var rows = peopleDataFrame.Sort(new string[] { "id" }, new bool[] { true }).Take(num);

            PrintAndVerifyPeopleDataFrameRows(rows, num);
        }

        /// <summary>
        /// Print given rows from people dataframe, and verify schema and contents if validation is enabled.
        /// </summary>
        /// <param name="rows"> Rows from people DataFrame </param>
        /// <param name="num"> Expected number of rows from people DataFrame </param>
        internal static void PrintAndVerifyPeopleDataFrameRows(IEnumerable<Row> rows, int num)
        {
            Console.WriteLine("peopleDataFrame:");

            var count = 0;
            StructType schema = null;
            Row firstRow = null;
            foreach (var row in rows)
            {
                if (count == 0)
                {
                    firstRow = row;

                    schema = row.GetSchema();
                    Console.WriteLine("schema: {0}", schema);
                }

                // output each row
                Console.WriteLine(row);
                Console.Write("id: {0}, name: {1}, age: {2}", row.GetAs<string>("id"), row.GetAs<string>("name"), row.GetAs<int>("age"));

                var address = row.GetAs<Row>("address");
                if (address != null)
                {
                    Console.WriteLine(", state: {0}, city: {1}", address.GetAs<string>("state"), address.GetAs<string>("city"));
                }
                else
                {
                    Console.WriteLine();
                }

                count++;
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(count, num);
                VerifySchemaOfPeopleDataFrame(schema);
                VerifyFirstRowOfPeopleDataFrame(firstRow);
            }
        }

        /// <summary>
        /// Sample to run first() method on DataFrame
        /// </summary>
        [Sample]
        internal static void DFFirstSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var row = peopleDataFrame.Sort(new string[] { "id" }, new bool[] { true }).First();
            var schema = row.GetSchema();

            Console.WriteLine("peopleDataFrame:");
            Console.WriteLine("schema: {0}", schema);

            // output 1st row
            Console.WriteLine(row);
            Console.Write("id: {0}, name: {1}, age: {2}", row.GetAs<string>("id"), row.GetAs<string>("name"), row.GetAs<int>("age"));

            var address = row.GetAs<Row>("address");
            if (address != null)
            {
                Console.WriteLine(", state: {0}, city: {1}", address.GetAs<string>("state"), address.GetAs<string>("city"));
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                VerifySchemaOfPeopleDataFrame(schema);
                VerifyFirstRowOfPeopleDataFrame(row);
            }

            try
            {
                peopleDataFrame.Filter("age < -1").First();

                if (SparkCLRSamples.Configuration.IsValidationEnabled)
                {
                    Assert.Fail("Invoking First() on a empty DataFrame should throw an InvalidOperationException.");
                }
            }
            catch (InvalidOperationException)
            {
                Console.WriteLine("Invoking First() methold on an empty DataFrame causes InvalidOperationException, which is expected.");
                // do nothing
            }
        }

        /// <summary>
        /// Verify the schema of people dataframe.
        /// </summary>
        /// <param name="schema"> RowSchema of people DataFrame </param>
        internal static void VerifySchemaOfPeopleDataFrame(StructType schema)
        {
            Assert.IsNotNull(schema);
            Assert.AreEqual("struct", schema.TypeName);
            Assert.IsNotNull(schema.Fields);
            Assert.AreEqual(4, schema.Fields.Count);

            // name
            var nameColSchema = schema.Fields.Find(c => c.Name.Equals("name"));
            Assert.IsNotNull(nameColSchema);
            Assert.AreEqual("name", nameColSchema.Name);
            Assert.IsTrue(nameColSchema.IsNullable);
            Assert.AreEqual("string", nameColSchema.DataType.TypeName);

            // id
            var idColSchema = schema.Fields.Find(c => c.Name.Equals("id"));
            Assert.IsNotNull(idColSchema);
            Assert.AreEqual("id", idColSchema.Name);
            Assert.IsTrue(idColSchema.IsNullable);
            Assert.AreEqual("string", nameColSchema.DataType.TypeName);

            // age
            var ageColSchema = schema.Fields.Find(c => c.Name.Equals("age"));
            Assert.IsNotNull(ageColSchema);
            Assert.AreEqual("age", ageColSchema.Name);
            Assert.IsTrue(ageColSchema.IsNullable);
            Assert.AreEqual("long", ageColSchema.DataType.TypeName);

            // address
            var addressColSchema = schema.Fields.Find(c => c.Name.Equals("address"));
            Assert.IsNotNull(addressColSchema);
            Assert.AreEqual("address", addressColSchema.Name);
            Assert.IsTrue(addressColSchema.IsNullable);
            Assert.IsNotNull(addressColSchema.DataType);
            Assert.AreEqual("struct", addressColSchema.DataType.TypeName);
            Assert.IsNotNull(((StructType)addressColSchema.DataType).Fields.Find(c => c.Name.Equals("state")));
            Assert.IsNotNull(((StructType)addressColSchema.DataType).Fields.Find(c => c.Name.Equals("city")));
        }

        /// <summary>
        /// Verify the contents of 1st row of people dataframe.
        /// </summary>
        /// <param name="firstRow"> First row of people DataFrame </param>
        internal static void VerifyFirstRowOfPeopleDataFrame(Row firstRow)
        {
            Assert.IsNotNull(firstRow);
            Assert.AreEqual(4, firstRow.Size());
            Assert.AreEqual("123", firstRow.GetAs<string>("id"));
            Assert.AreEqual("Bill", firstRow.GetAs<string>("name"));
            Assert.AreEqual(34, firstRow.GetAs<int>("age"));

            var addressCol = firstRow.GetAs<Row>("address");
            Assert.IsNotNull(addressCol);
            Assert.AreEqual("Columbus", addressCol.GetAs<string>("city"));
            Assert.AreEqual("Ohio", addressCol.GetAs<string>("state"));
        }

        /// <summary>
        /// Sample to run distinct() operation on DataFrame
        /// </summary>
        [Sample]
        internal static void DFDistinctSample()
        {
            var csvTestLogDataFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(CSVTestLog));

            var row = csvTestLogDataFrame.First();
            var schema = row.GetSchema();

            Console.WriteLine("csvTestLogDataFrame:");
            Console.WriteLine("schema: {0}", schema);

            // output 1st row
            Console.WriteLine(row);

            var count = csvTestLogDataFrame.Count();
            var distinctCount = csvTestLogDataFrame.Distinct().Count();

            Console.WriteLine("Count:{0}, distinct count:{1}", count, distinctCount);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(count > 0);
                Assert.IsTrue(distinctCount > 0);
                Assert.IsTrue(count > distinctCount);
            }
        }

        /// <summary>
        /// Sample to get Rdd from DataFrame.
        /// </summary>
        [Sample]
        internal static void DFRddSample()
        {
            // repartitioning below so that batching in pickling does not impact count on Rdd created from the dataframe
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson)).Repartition(4);
            peopleDataFrame.Show();

            var dfCount = peopleDataFrame.Count();
            var peopleRdd = peopleDataFrame.Rdd;
            var peopleRddCount = peopleRdd.Count();
            Console.WriteLine("Count of people DataFrame: {0}, count of peole RDD: {1}", dfCount, peopleRddCount);

            foreach (var people in peopleRdd.Collect())
            {
                Console.WriteLine(people);
            }

            var stringRdd = peopleRdd.Map(x => string.Format("{0}, {1}", x.Size(), x.ToString()));
            var stringRddCount = stringRdd.Count();
            Console.WriteLine("count of stringrdd is " + stringRddCount);
            var stringrddvalues = stringRdd.Collect();
            int i = 1;
            foreach (var stringrddvalue in stringrddvalues)
            {
                Console.WriteLine("{0}: {1}", i++, stringrddvalue);
            }

            var intRdd = peopleRdd.Map(x => 1);
            var intRddCount = intRdd.Count();
            Console.WriteLine("count of rdd is " + intRddCount);
            var sum = intRdd.Fold(0, (x, y) => x + y);
            Console.WriteLine("Count of rows is " + sum);

            var orderDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(OrderJson));
            var orderRdd = orderDataFrame.Rdd;

            foreach (var order in orderRdd.Collect())
            {
                Console.WriteLine(order);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(dfCount == peopleRddCount); 
                Assert.AreEqual(4, dfCount);
                Assert.AreEqual(4, intRddCount);
                Assert.AreEqual(4, sum);
            }

            var nameRdd = peopleDataFrame.Rdd.Map(item => (string) item.Get("name")).Filter(name => name.Equals("steve", StringComparison.OrdinalIgnoreCase));
            var nameRddCount = nameRdd.Count();
            Console.WriteLine("There are {0} people whose name is steve.", nameRddCount);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(nameRddCount == 1);
            }

            var peopleDataFrame2 = peopleDataFrame.Filter("age <= 34 ");
            var peopleDataFrame2Count = peopleDataFrame2.Count();
            var peopleRdd2Count = peopleDataFrame2.Rdd.Count();
            Console.WriteLine("Count of peopleRdd2 is: {0}", peopleRdd2Count);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(peopleRdd2Count, 2);
                Assert.IsTrue(peopleDataFrame2Count == peopleRdd2Count);
            }
        }

        /// <summary>
        /// Verifies that the two List of Rows are equivalent. 
        /// Two List are equivalent if they have the same Rows in the same quantity, but in any order.
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="columnsComparer">Specify custom comparer for certain columns. The key of dictionary is column name, the value is comparer.</param>
        /// <returns>True if two lists are equivalent.</returns>
        private static bool AreRowListEquivalent(List<Row> x, List<Row> y, Dictionary<string, Func<object, object, bool>> columnsComparer = null)
        {
            if ((x == null && y != null) || (x != null && y == null)) return false;

            if (x == null && y == null) return true;

            return x.Count == y.Count && x.All(xRow => y.Any(yRow => IsRowEqual(xRow, yRow, columnsComparer)));
        }

        /// <summary>
        /// Verifies that the two Rows are equal. 
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="columnsComparer">Specify custom comparer for certain columns. The key of dictionary is column name, the value is comparer.</param>
        /// <returns>True if two Rows are equal.</returns>
        private static bool IsRowEqual(Row x, Row y, Dictionary<string, Func<object, object, bool>> columnsComparer = null)
        {
            if (x == null && y == null) return true;
            if (x == null && y != null || x != null && y == null) return false;

            foreach (var col in x.GetSchema().Fields)
            {
                if (!y.GetSchema().Fields.Any(c => c.Name == col.Name)) return false;

                if (col.DataType is StructType)
                {
                    if (!IsRowEqual(x.GetAs<Row>(col.Name), y.GetAs<Row>(col.Name), columnsComparer)) return false;
                }
                else if (x.Get(col.Name) == null && y.Get(col.Name) != null || x.Get(col.Name) != null && y.Get(col.Name) == null) return false;
                else if (x.Get(col.Name) != null && y.Get(col.Name) != null)
                {
                    Func<object, object, bool> colComparer;
                    if (columnsComparer != null && columnsComparer.TryGetValue(col.Name, out colComparer))
                    {
                        if (!colComparer(x.Get(col.Name), y.Get(col.Name))) return false;
                    }
                    else
                    {
                        if (x.Get(col.Name).ToString() != y.Get(col.Name).ToString()) return false;
                    }
                }
            }
            return true;
        }

        /// <summary>
        /// Sample to check IsLocal from DataFrame.
        /// </summary>
        [Sample]
        internal static void DFIsLocalSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            bool isLocal = peopleDataFrame.IsLocal;

            Console.WriteLine("IsLocal: {0}", isLocal);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(false, isLocal);
            }
        }

        /// <summary>
        /// Sample to check IsLocal from DataFrame.
        /// </summary>
        [Sample]
        internal static void DFCoalesceSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson)).Repartition(4);

            var numPartitions = peopleDataFrame.MapPartitions(iters => new int[] { iters.Count() }).Count();
            Console.WriteLine("Before coalesce, numPartitions: {0}", numPartitions);

            var expectedNumPartitions = numPartitions / 2;
            var newDataFrame = peopleDataFrame.Coalesce((int)expectedNumPartitions);
            Console.WriteLine("Force coalesce, dataframe count: {0}", newDataFrame.Count());

            var newNumPartitions = newDataFrame.MapPartitions(iters => new int[] { iters.Count() }).Count();
            Console.WriteLine("After coalesce, numPartitions: {0}", newNumPartitions);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(numPartitions == newNumPartitions * 2);
            }
        }

        /// <summary>
        /// Sample to persist DataFrame.
        /// </summary>
        [Sample]
        internal static void DFPersistSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.Persist().Show();
        }

        /// <summary>
        /// Sample to unpersist DataFrame.
        /// </summary>
        [Sample]
        internal static void DFUnpersistSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.Persist().Unpersist().Show();
        }

        /// <summary>
        /// Sample to cache DataFrame.
        /// </summary>
        [Sample]
        internal static void DFCacheSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.Cache().Show();
        }

        /// <summary>
        /// Sample to repartition DataFrame.
        /// </summary>
        [Sample]
        internal static void DFRepartitionSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var numPartitions = peopleDataFrame.MapPartitions(iters => new int[] { iters.Count() }).Count();
            Console.WriteLine("Before repartition, numPartitions: {0}", numPartitions);

            var expectedNumPartitions = numPartitions + 1;
            var newDataFrame = peopleDataFrame.Repartition((int)expectedNumPartitions);
            Console.WriteLine("Force repartition, dataframe count: {0}", newDataFrame.Count());

            var newNumPartitions = newDataFrame.MapPartitions(iters => new int[] { iters.Count() }).Count();
            Console.WriteLine("After repartition, numPartitions: {0}", newNumPartitions);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(numPartitions + 1, newNumPartitions);
            }
        }

        /// <summary>
        /// Sample to sample a DataFrame.
        /// </summary>
        [Sample]
        internal static void DFSampleSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            Console.WriteLine("With a seed:");
            var sampleDFWithSeed = peopleDataFrame.Sample(false, 0.25, new Random().Next());
            sampleDFWithSeed.Show();

            Console.WriteLine("Without a seed:");
            var sampleDFWithoutSeed = peopleDataFrame.Sample(false, 0.25, null);
            sampleDFWithoutSeed.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var originalDFCollected = peopleDataFrame.Collect().ToArray();
                var sampleDFWithSeedCollected = sampleDFWithSeed.Collect().ToArray();
                var sampleDFWithoutSeedCollected = sampleDFWithoutSeed.Collect().ToArray();
                if (sampleDFWithSeedCollected.Length > 0)
                {
                    // sample DataFrame should be a subset of original DataFrame
                    Assert.IsTrue(sampleDFWithSeedCollected.All(sample => originalDFCollected.Any(original => IsRowEqual(original, sample))));
                }
                if (sampleDFWithoutSeedCollected.Length > 0)
                {
                    // sample DataFrame should be a subset of original DataFrame
                    Assert.IsTrue(sampleDFWithoutSeedCollected.All(sample => originalDFCollected.Any(original => IsRowEqual(original, sample))));
                }
            }
        }

        /// <summary>
        /// FlatMap sample
        /// </summary>
        [Sample]
        internal static void DFFlatMapSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var dfCount = peopleDataFrame.Count();

            RDD<string> rdd = peopleDataFrame.FlatMap(row => new String[] { (string)row.Get("name"), (string)row.Get("id") });
            var rddCount = rdd.Count();

            Console.WriteLine("DataFrame count: {0}, RDD count: {1}", dfCount, rddCount);

            string[] rows = rdd.Collect();
            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsNotNull(rows);
                Assert.AreEqual(rows.Count(), 2 * dfCount);
            }

            for (var i = 0; i < rows.Length; i++)
            {
                Console.WriteLine("[{0}] {1}", i, rows[i].ToString());
            }
        }

        /// <summary>
        /// Map sample
        /// </summary>
        [Sample]
        internal static void DFMapSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var dfCount = peopleDataFrame.Count();

            RDD<string> rdd = peopleDataFrame.Map(row => (string)row.Get("name"));
            var rddCount = rdd.Count();

            Console.WriteLine("DataFrame count: {0}, RDD count: {1}", dfCount, rddCount);

            string[] rows = rdd.Collect();
            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsNotNull(rows);
                Assert.AreEqual(rows.Count(), dfCount);
            }

            for (var i = 0; i < rows.Length; i++)
            {
                Console.WriteLine("[{0}] {1}", i, rows[i]);
            }
        }
        /// <summary>
        /// MapPartitions sample
        /// </summary>
        [Sample]
        internal static void DFMapPartitionsSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var dfCount = peopleDataFrame.Count();

            RDD<int> rdd = peopleDataFrame.MapPartitions(iter => new int[] { iter.Count() });
            var rddSum = rdd.Collect().Sum();

            Console.WriteLine("DataFrame count: {0}, RDD sum: {1}", dfCount, rddSum);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(dfCount, rddSum);
            }
        }

        /// <summary>
        /// ForeachPartition sample
        /// </summary>
        [Sample]
        internal static void DFForeachPartitionSample()
        {
            const int partitionNumber = 3;
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson)).Repartition(partitionNumber);
            Console.WriteLine("Partition number: " + partitionNumber);
            var accumulator = SparkCLRSamples.SparkContext.Accumulator(0);
            peopleDataFrame.ForeachPartition(new PartitionCountHelper(accumulator).Execute);
            Console.WriteLine("Partition number counted by ForeachPartition :" + accumulator.Value);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(partitionNumber, accumulator.Value);
            }
        }

        [Serializable]
        internal class PartitionCountHelper
        {
            private Accumulator<int> accumulator;
            internal PartitionCountHelper(Accumulator<int> accumulator)
            {
                this.accumulator = accumulator;
            }

            internal void Execute(IEnumerable<Row> iter)
            {
                // Count only once for a partition iter
                accumulator = accumulator + 1;
                int i = 0;
                foreach (var row in iter)
                {
                    i++;
                }
            }
        }

        [Serializable]
        internal class ForeachRowHelper
        {
            private Accumulator<int> accumulator;
            internal ForeachRowHelper(Accumulator<int> accumulator)
            {
                this.accumulator = accumulator;
            }

            internal void Execute(Row row)
            {
                accumulator = accumulator + 1;
            }
        }

        /// <summary>
        /// Foreach sample
        /// </summary>
        [Sample]
        internal static void DFForeachSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var accumulator = SparkCLRSamples.SparkContext.Accumulator(0);
            peopleDataFrame.Foreach(new ForeachRowHelper(accumulator).Execute);

            Console.WriteLine("DataFrame Row count: " + accumulator.Value);
            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(4, accumulator.Value);
            }
        }

        [Serializable]
        internal class ActionHelper
        {
            private Accumulator<int> accumulator;
            internal ActionHelper(Accumulator<int> accumulator)
            {
                this.accumulator = accumulator;
            }

            internal void Execute(IEnumerable<Row> iter)
            {
                foreach (var row in iter)
                {
                    accumulator = accumulator + 1;
                }
            }
        }

        /// <summary>
        /// ForeachPartition sample with accumulator
        /// </summary>
        [Sample]
        internal static void DFForeachnSampleWithAccumulator()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var accumulator = SparkCLRSamples.SparkContext.Accumulator(0);
            peopleDataFrame.ForeachPartition(new ActionHelper(accumulator).Execute);

            Console.WriteLine("Total count of rows: {0}", accumulator.Value);
        }

        /// <summary>
        /// Write to parquet sample using DataFrameWriter
        /// </summary>
        [Sample]
        internal static void DFWriteToParquetSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var parquetPath = Path.GetTempPath() + "DF_Parquet_Samples_" + (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalSeconds;
            peopleDataFrame.Coalesce(1).Write().Parquet(parquetPath);

            Console.WriteLine("Save dataframe to parquet: {0}", parquetPath);
            Console.WriteLine("Files:");

            foreach (var f in Directory.EnumerateFiles(parquetPath))
            {
                Console.WriteLine(f);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(Directory.Exists(parquetPath));
            }

            Directory.Delete(parquetPath, true);
            Console.WriteLine("Remove parquet directory: {0}", parquetPath);
        }

        /// <summary>
        /// Write to parquet sample with 'append' mode
        /// </summary>
        [Sample]
        internal static void DFWriteToParquetSampleWithAppendMode()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var parquetPath = Path.GetTempPath() + "DF_Parquet_Samples_" + (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalSeconds;
            peopleDataFrame.Coalesce(1).Write().Mode(SaveMode.ErrorIfExists).Parquet(parquetPath);

            Console.WriteLine("Save dataframe to parquet: {0}", parquetPath);
            Console.WriteLine("Files:");

            foreach (var f in Directory.EnumerateFiles(parquetPath))
            {
                Console.WriteLine(f);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(Directory.Exists(parquetPath));
            }

            peopleDataFrame.Write().Mode(SaveMode.Append).Parquet(parquetPath);

            Console.WriteLine("Append dataframe to parquet: {0}", parquetPath);
            Console.WriteLine("Files:");

            foreach (var f in Directory.EnumerateFiles(parquetPath))
            {
                Console.WriteLine(f);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(Directory.Exists(parquetPath));
            }

            Directory.Delete(parquetPath, true);
            Console.WriteLine("Remove parquet directory: {0}", parquetPath);
        }

        /// <summary>
        /// Write to json sample
        /// </summary>
        [Sample]
        internal static void DFWriteToJsonSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var jsonPath = Path.GetTempPath() + "DF_Json_Samples_" + (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalSeconds;
            peopleDataFrame.Write().Mode(SaveMode.Overwrite).Json(jsonPath);

            Console.WriteLine("Save dataframe to: {0}", jsonPath);
            Console.WriteLine("Files:");

            foreach (var f in Directory.EnumerateFiles(jsonPath))
            {
                Console.WriteLine(f);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(Directory.Exists(jsonPath));
            }

            Directory.Delete(jsonPath, true);
            Console.WriteLine("Remove parquet directory: {0}", jsonPath);
        }

        /// <summary>
        /// Write to parquet sample
        /// </summary>
        [Sample]
        internal static void DFSaveAsParquetFileSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var parquetPath = Path.GetTempPath() + "DF_Parquet_Samples_" + (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalSeconds;
            peopleDataFrame.Coalesce(1).SaveAsParquetFile(parquetPath);

            Console.WriteLine("Save dataframe to parquet: {0}", parquetPath);
            Console.WriteLine("Files:");

            foreach (var f in Directory.EnumerateFiles(parquetPath))
            {
                Console.WriteLine(f);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(Directory.Exists(parquetPath));
            }

            Directory.Delete(parquetPath, true);
            Console.WriteLine("Remove parquet directory: {0}", parquetPath);
        }

        /// <summary>
        /// DataFrame `Save` sample
        /// </summary>
        [Sample]
        internal static void DFSaveSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var path = Path.GetTempPath() + "DF_Samples_" + (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalSeconds;
            peopleDataFrame.Save(path, "json", SaveMode.ErrorIfExists, "option1", "option_value1");

            Console.WriteLine("Save dataframe to: {0}", path);
            Console.WriteLine("Files:");

            foreach (var f in Directory.EnumerateFiles(path))
            {
                Console.WriteLine(f);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(Directory.Exists(path));
            }

            Directory.Delete(path, true);
            Console.WriteLine("Remove directory: {0}", path);
        }
    }
}
