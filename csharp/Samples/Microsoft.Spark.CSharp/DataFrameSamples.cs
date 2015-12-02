// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
        internal static void DFGetSchemaToJsonSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            string json = peopleDataFrame.Schema.ToJson();
            Console.WriteLine("schema in json format: {0}", json);
        }

        /// <summary>
        /// Sample to run collect for DataFrame
        /// </summary>
        [Sample]
        internal static void DFCollectSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            IEnumerable<Row> rows = peopleDataFrame.Collect();
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
            var requestsSchema = StructType.CreateStructType(
                new List<StructField>
                {
                    StructField.CreateStructField("guid", "string", false),
                    StructField.CreateStructField("datacenter", "string", false),
                    StructField.CreateStructField("abtestid", "string", false),
                    StructField.CreateStructField("traffictype", "string", false),
                }
                );

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

        private static DataFrame GetMetricsDataFrame()
        {
            var metricsSchema = StructType.CreateStructType(
                new List<StructField>
                {
                    StructField.CreateStructField("unknown", "string", false),
                    StructField.CreateStructField("date", "string", false),
                    StructField.CreateStructField("time", "string", false),
                    StructField.CreateStructField("guid", "string", false),
                    StructField.CreateStructField("lang", "string", false),
                    StructField.CreateStructField("country", "string", false),
                    StructField.CreateStructField("latency", "integer", false)
                }
                );

            return
                GetSqlContext()
                    .TextFile(SparkCLRSamples.Configuration.GetInputDataPath(MetricsLog), metricsSchema);
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
                var stringVal = dataType.ToString();
                var simpleStringVal = dataType.SimpleString();
                var isNullable = peopleDataFrameSchemaField.IsNullable;
                Console.WriteLine("Name={0}, DT.string={1}, DT.simplestring={2}, DT.isNullable={3}", name, stringVal, simpleStringVal, isNullable);
            }
        }

        /// <summary>
        /// Sample to convert DataFrames to RDD
        /// </summary>
        [Sample]
        internal static void DFConversionSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var stringRddCreatedFromDataFrame = peopleDataFrame.ToJSON();
            var stringRddCreatedFromDataFrameRowCount = stringRddCreatedFromDataFrame.Count();

            var byteArrayRddCreatedFromDataFrame = peopleDataFrame.ToRDD();
            var byteArrayRddCreatedFromDataFrameRowCount = byteArrayRddCreatedFromDataFrame.Count();

            Console.WriteLine("stringRddCreatedFromDataFrameRowCount={0}, byteArrayRddCreatedFromDataFrameRowCount={1}", stringRddCreatedFromDataFrameRowCount, byteArrayRddCreatedFromDataFrameRowCount);
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
        }

        /// <summary>
        /// Sample to join DataFrames using DSL
        /// </summary>
        [Sample]
        internal static void DFJoinSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var orderDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(OrderJson));
            
            //Following example does not work in Spark 1.4.1 - uncomment in 1.5
            //var peopleDataFrame2 = GetSqlContext().JsonFile(Samples.GetInputDataPath(PeopleJson));
            //var columnNameJoin = peopleDataFrame.Join(peopleDataFrame2, new string[] {"id"});
            //columnNameJoin.Show();
            //columnNameJoin.ShowDF();

            var expressionJoin = peopleDataFrame.Join(orderDataFrame, peopleDataFrame["id"] == orderDataFrame["personid"]);
            expressionJoin.ShowSchema();
            expressionJoin.Show();
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
        }

        /// <summary>
        /// Sample to drop a DataFrame from another DataFrame using DSL.
        /// </summary>
        [Sample]
        internal static void DFDropSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            var dropped = peopleDataFrame.Drop("name");
            dropped.Show();
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
        /// Sample to drop a duplicated rows in a DataFrame.
        /// </summary>
        [Sample]
        internal static void DFDropDuplicatesSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            var dropped = peopleDataFrame.DropDuplicates(new[] { "name" });
            dropped.Show();
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
        }

        /// <summary>
        /// Sample to perform simple select and filter on DataFrame using UDF
        /// </summary>
        [Sample]
        internal static void DFProjectionFilterUDFSample()
        {
            GetSqlContext().RegisterFunction<string, string, string>("FullAddress", (city, state) => city + " " + state);
            GetSqlContext().RegisterFunction<bool, string, int>("PeopleFilter", (name, age) => name == "Bill" && age > 30);

            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            // DataFrame query
            peopleDataFrame.SelectExpr("name", "age * 2 as age", "FullAddress(address.city, address.state) as address")
                .Where("name='Bill' and age > 40 and PeopleFilter(name, age)")
                .Show();

            // equivalent sql script
            peopleDataFrame.RegisterTempTable("people");
            GetSqlContext().Sql("SELECT name, age*2 as age, FullAddress(address.city, address.state) as address FROM people where name='Bill' and age > 40 and PeopleFilter(name, age)").Show();
        }

        /// <summary>
        /// Sample to perform limit on DataFrame using DSL.
        /// </summary>
        [Sample]
        internal static void DFLimitSample()
        {
            const int num = 2;
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var peopleDataFrame2 = peopleDataFrame.Limit(num);

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
            var rows = peopleDataFrame.Head(num);

            PrintAndVerifyPeopleDataFrameRows(rows, num);
        }

        [Sample]
        internal static void DFTakeSample()
        {
            const int num = 2;
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var rows = peopleDataFrame.Take(num);

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
            RowSchema schema = null;
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
            var row = peopleDataFrame.First();
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
        internal static void VerifySchemaOfPeopleDataFrame(RowSchema schema)
        {
            Assert.IsNotNull(schema);
            Assert.AreEqual("struct", schema.type);
            Assert.IsNotNull(schema.columns);
            Assert.AreEqual(4, schema.columns.Count);

            // name
            var nameColSchema = schema.columns.Find(c => c.name.Equals("name"));
            Assert.IsNotNull(nameColSchema);
            Assert.AreEqual("name", nameColSchema.name);
            Assert.IsTrue(nameColSchema.nullable);
            Assert.AreEqual("string", nameColSchema.type.ToString());

            // id
            var idColSchema = schema.columns.Find(c => c.name.Equals("id"));
            Assert.IsNotNull(idColSchema);
            Assert.AreEqual("id", idColSchema.name);
            Assert.IsTrue(idColSchema.nullable);
            Assert.AreEqual("string", nameColSchema.type.ToString());

            // age
            var ageColSchema = schema.columns.Find(c => c.name.Equals("age"));
            Assert.IsNotNull(ageColSchema);
            Assert.AreEqual("age", ageColSchema.name);
            Assert.IsTrue(ageColSchema.nullable);
            Assert.AreEqual("long", ageColSchema.type.ToString());

            // address
            var addressColSchema = schema.columns.Find(c => c.name.Equals("address"));
            Assert.IsNotNull(addressColSchema);
            Assert.AreEqual("address", addressColSchema.name);
            Assert.IsTrue(addressColSchema.nullable);
            Assert.IsNotNull(addressColSchema.type);
            Assert.AreEqual("struct", addressColSchema.type.type);
            Assert.IsNotNull(addressColSchema.type.columns.Find(c => c.name.Equals("state")));
            Assert.IsNotNull(addressColSchema.type.columns.Find(c => c.name.Equals("city")));
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
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.Show();

            var dfCount = peopleDataFrame.Count();
            var peopleRdd = peopleDataFrame.Rdd;
            var peopleRddCount = peopleRdd.Count();
            Console.WriteLine("Count of people DataFrame: {0}, count of peole RDD: {1}", dfCount, peopleRddCount);

            foreach (var people in peopleRdd.Collect())
            {
                Console.WriteLine(people);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(dfCount > 0);
                Assert.IsTrue(dfCount == peopleRddCount);
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

            foreach (var col in x.GetSchema().columns)
            {
                if (!y.GetSchema().columns.Any(c => c.ToString() == col.ToString())) return false;

                if (col.type.columns.Any())
                {
                    if (!IsRowEqual(x.GetAs<Row>(col.name), y.GetAs<Row>(col.name), columnsComparer)) return false;
                }
                else if (x.Get(col.name) == null && y.Get(col.name) != null || x.Get(col.name) != null && y.Get(col.name) == null) return false;
                else if (x.Get(col.name) != null && y.Get(col.name) != null)
                {
                    Func<object, object, bool> colComparer;
                    if (columnsComparer != null && columnsComparer.TryGetValue(col.name, out colComparer))
                    {
                        if (!colComparer(x.Get(col.name), y.Get(col.name))) return false;
                    }
                    else
                    {
                        if (x.Get(col.name).ToString() != y.Get(col.name).ToString()) return false;
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
                Assert.IsTrue((numPartitions + 1) == newNumPartitions);
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
            peopleDataFrame.Sample(false, 0.25, new Random().Next()).Show();

            Console.WriteLine("Without a seed:");
            peopleDataFrame.Sample(false, 0.25, null).Show();
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
    }
}
