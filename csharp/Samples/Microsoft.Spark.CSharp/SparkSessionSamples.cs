// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using NUnit.Framework;

namespace Microsoft.Spark.CSharp.Samples
{
    class SparkSessionSamples
    {
        private static SparkSession sparkSession;

        internal static SparkSession GetSparkSession()
        {
            return sparkSession ?? (sparkSession = SparkSession.Builder().EnableHiveSupport().GetOrCreate());
        }

        [Sample]
        internal static void SSNewSessionSample()
        {
            RunDataFrameSample(true);
        }

        [Sample]
        internal static void SSDataFrameSample()
        {
            RunDataFrameSample(false);
        }

        private static void RunDataFrameSample(bool createNewSession)
        {
            SparkSession ss = GetSparkSession();

            if (createNewSession)
            {
                ss = sparkSession.NewSession();
            }

            var peopleDataFrame = ss.Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(DataFrameSamples.PeopleJson));
            var count = peopleDataFrame.Count();
            Console.WriteLine("Count of items in DataFrame {0}", count);

            var sortedDataFrame = peopleDataFrame.Sort(new string[] { "name", "age" }, new bool[] { true, false });

            sortedDataFrame.Show();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var sortedDF = sortedDataFrame.Collect().ToArray();
                Assert.AreEqual("789", sortedDF[0].GetAs<string>("id"));
                Assert.AreEqual("123", sortedDF[1].GetAs<string>("id"));
                Assert.AreEqual("531", sortedDF[2].GetAs<string>("id"));
                Assert.AreEqual("456", sortedDF[3].GetAs<string>("id"));
            }
        }

        [Sample]
        internal static void SSShowSchemaSample()
        {
            var peopleDataFrame = GetSparkSession().Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(DataFrameSamples.PeopleJson));
            peopleDataFrame.Explain(true);
            peopleDataFrame.ShowSchema();
        }

        [Sample]
        internal static void SSTableSample()
        {
            var originalPeopleDataFrame = GetSparkSession().Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(DataFrameSamples.PeopleJson));
            originalPeopleDataFrame.RegisterTempTable("people");

            var peopleDataFrame = GetSparkSession().Table("people");

            var projectedFilteredDataFrame = peopleDataFrame.Select("name", "address.state")
                                        .Where("name = 'Bill' or state = 'California'");

            projectedFilteredDataFrame.ShowSchema();
            projectedFilteredDataFrame.Show();
            
            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEqual(new[] { "name", "state" }, projectedFilteredDataFrame.Schema.Fields.Select(f => f.Name).ToArray());
                Assert.IsTrue(projectedFilteredDataFrame.Collect().All(row => row.Get("name") == "Bill" || row.Get("state") == "California"));
            }
        }

        [Sample]
        internal static void SSSqlSample()
        {
            var originalPeopleDataFrame = GetSparkSession().Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(DataFrameSamples.PeopleJson));
            originalPeopleDataFrame.RegisterTempTable("people");

            var nameFilteredDataFrame = GetSparkSession().Sql("SELECT name, address.city, address.state FROM people where name='Bill'");
            var countDataFrame = GetSparkSession().Sql("SELECT count(name) FROM people where name='Bill'");
            var maxAgeDataFrame = GetSparkSession().Sql("SELECT max(age) FROM people where name='Bill'");
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

        [Sample]
        internal static void SSDropTableSample()
        {
            var originalPeopleDataFrame = GetSparkSession().Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(DataFrameSamples.PeopleJson));
            originalPeopleDataFrame.RegisterTempTable("people");

            var nameFilteredDataFrame = GetSparkSession().Sql("SELECT name, address.city, address.state FROM people where name='Bill'");
            long nameFilteredDataFrameRowsCount = nameFilteredDataFrame.Count();

            GetSparkSession().Catalog.DropTempTable("people");
            
            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                bool tableMissing = false;
                try
                {
                    //parsing would fail
                    var nameFilteredDataFrame2 = GetSparkSession().Sql("SELECT name, address.city, address.state FROM people where name='Bill'");
                }
                catch (Exception)
                {
                    tableMissing = true;
                }

                Assert.True(tableMissing);
            }
        }

        [Sample]
        internal static void SSCreateDataFrameSample()
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

            var dataFramePeople = GetSparkSession().CreateDataFrame(rddPeople, schemaPeople);
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
    }
}
