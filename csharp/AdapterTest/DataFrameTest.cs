// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;

using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between DataFrame and its proxies
    /// </summary>
    [TestClass]
    public class DataFrameTest
    {
        //TODO - complete impl

        [TestMethod]
        public void TestDataFrameJoin()
        {
            var sqlContext = new SqlContext(new SparkContext("", ""));
            var dataFrame = sqlContext.JsonFile(@"c:\path\to\input.json"); 
            var dataFrame2 = sqlContext.JsonFile(@"c:\path\to\input2.json"); 
            var joinedDataFrame = dataFrame.Join(dataFrame2, "JoinCol");
            var paramValuesToJoinMethod = (joinedDataFrame.DataFrameProxy as MockDataFrameProxy).mockDataFrameReference as object[];
            var paramValuesToSecondDataFrameJsonFileMethod = ((paramValuesToJoinMethod[0] as MockDataFrameProxy).mockDataFrameReference as object[]);
            Assert.AreEqual(@"c:\path\to\input2.json", paramValuesToSecondDataFrameJsonFileMethod[0]);
            Assert.AreEqual("JoinCol", paramValuesToJoinMethod[1]);
        }

        [TestMethod]
        public void TestDataFrameCollect()
        {
            string jsonSchema = @"
                {
                  ""type"" : ""struct"",
                  ""fields"" : [ {
                    ""name"" : ""address"",
                    ""type"" : {
                      ""type"" : ""struct"",
                      ""fields"" : [ {
                        ""name"" : ""city"",
                        ""type"" : ""string"",
                        ""nullable"" : true,
                        ""metadata"" : { }
                      }, {
                        ""name"" : ""state"",
                        ""type"" : ""string"",
                        ""nullable"" : true,
                        ""metadata"" : { }
                      } ]
                    },
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""age"",
                    ""type"" : ""long"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""id"",
                    ""type"" : ""string"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""name"",
                    ""type"" : ""string"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  } ]
                }";

            int localPort = 4000;
            object row1 = new object[] {
                new object[] {"Columbus", "Ohio"},
                34,
                "123",
                "Bill"
            };

            object row2 = new object[] {
                new object[] {"Seattle", "Washington"},
                43,
                "789",
                "Bill"
            };

            IStructTypeProxy structTypeProxy = new MockStructTypeProxy(jsonSchema);
            IDataFrameProxy dataFrameProxy =
                new MockDataFrameProxy(localPort,
                                       new List<object>() { row1, row2 },
                                       structTypeProxy);
            DataFrame dataFrame = new DataFrame(dataFrameProxy, null);

            List<Row> rows = new List<Row>();
            foreach (var row in dataFrame.Collect())
            {
                rows.Add(row);
                Console.WriteLine("{0}", row);
            }

            Assert.AreEqual(rows.Count, 2);
            Row firstRow = rows[0];

            string id = firstRow.GetAs<string>("id");
            Assert.IsTrue(id.Equals("123"));
            string name = firstRow.GetAs<string>("name");
            Assert.IsTrue(name.Equals("Bill"));
            int age = firstRow.GetAs<int>("age");
            Assert.AreEqual(age, 34);

            Row address = firstRow.GetAs<Row>("address");
            Assert.AreNotEqual(address, null);
            string city = address.GetAs<string>("city");
            Assert.IsTrue(city.Equals("Columbus"));
            string state = address.GetAs<string>("state");
            Assert.IsTrue(state.Equals("Ohio"));
        }
    }

}
