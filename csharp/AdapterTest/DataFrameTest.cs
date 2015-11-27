// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between DataFrame and its proxies
    /// </summary>
    [TestClass]
    public class DataFrameTest
    {
        //TODO - complete impl

        private static Mock<IDataFrameProxy> mockDataFrameProxy;

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            mockDataFrameProxy = new Mock<IDataFrameProxy>();
        }

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

        [TestMethod]
        public void TestIntersect()
        {
            // Arrange
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Intersect(It.IsAny<IDataFrameProxy>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var otherDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualDataFrame = originalDataFrame.Intersect(otherDataFrame);

            // Assert
            mockDataFrameProxy.Verify(m => m.Intersect(otherDataFrame.DataFrameProxy)); // assert Intersect of Proxy was invoked with correct parameters
            Assert.AreEqual(actualDataFrame.DataFrameProxy, expectedResultDataFrameProxy);
        }

        [TestMethod]
        public void TestUnionAll()
        {
            // Arrange
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.UnionAll(It.IsAny<IDataFrameProxy>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var otherDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.UnionAll(otherDataFrame);

            // Assert
            mockDataFrameProxy.Verify(m => m.UnionAll(otherDataFrame.DataFrameProxy)); // assert UnionAll of Proxy was invoked with correct parameters
            Assert.AreEqual(actualResultDataFrame.DataFrameProxy, expectedResultDataFrameProxy);
        }

        [TestMethod]
        public void TestSubtract()
        {
            // Arrange
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Subtract(It.IsAny<IDataFrameProxy>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var otherDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.Subtract(otherDataFrame);

            // Assert
            mockDataFrameProxy.Verify(m => m.Subtract(otherDataFrame.DataFrameProxy)); // assert Subtract of Proxy was invoked with correct parameters
            Assert.AreEqual(actualResultDataFrame.DataFrameProxy, expectedResultDataFrameProxy);
        }

        [TestMethod]
        public void TestDrop()
        {
            // Arrange
            const string columnNameToDrop = "column1";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Drop(It.IsAny<string>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.Drop(columnNameToDrop);

            // Assert
            mockDataFrameProxy.Verify(m => m.Drop(columnNameToDrop)); // assert Drop of Proxy was invoked with correct parameters
            Assert.AreEqual(actualResultDataFrame.DataFrameProxy, expectedResultDataFrameProxy);
        }

        [TestMethod]
        public void TestDropNa()
        {
            // Arrange
            const string columnName = "column1";
            var mockSchemaProxy = new Mock<IStructTypeProxy>();
            var mockFieldProxy = new Mock<IStructFieldProxy>();
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.DropNa(It.IsAny<int?>(), It.IsAny<string[]>())).Returns(expectedResultDataFrameProxy);
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockSchemaProxy.Object);
            mockSchemaProxy.Setup(m => m.GetStructTypeFields()).Returns(new List<IStructFieldProxy> { mockFieldProxy.Object });
            mockFieldProxy.Setup(m => m.GetStructFieldName()).Returns(columnName);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.DropNa();

            // Assert
            mockDataFrameProxy.Verify(m => m.DropNa(1, It.Is<string[]>(subset => subset.Length == 1 && 
                subset.Contains(columnName)))); // assert DropNa of Proxy was invoked with correct parameters
            Assert.AreEqual(actualResultDataFrame.DataFrameProxy, expectedResultDataFrameProxy);
        }

        [TestMethod]
        public void TestDropDuplicates()
        {
            #region subset is null
            // Arrange
            const string columnNameToDropDups = "column1";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.DropDuplicates()).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.DropDuplicates();

            // Assert
            mockDataFrameProxy.Verify(m => m.DropDuplicates()); // assert DropDuplicates of Proxy was invoked with correct parameters
            Assert.AreEqual(actualResultDataFrame.DataFrameProxy, expectedResultDataFrameProxy);
            #endregion

            #region subset is not null
            // Arrange
            mockDataFrameProxy.Setup(m => m.DropDuplicates(It.IsAny<string[]>())).Returns(expectedResultDataFrameProxy);

            // Act
            actualResultDataFrame = originalDataFrame.DropDuplicates(new []{columnNameToDropDups});

            // Assert
            mockDataFrameProxy.Verify(m => m.DropDuplicates(It.Is<string[]>(subset => subset.Length == 1 &&
                subset.Contains(columnNameToDropDups)))); // assert DropDuplicates of Proxy was invoked with correct parameters
            Assert.AreEqual(actualResultDataFrame.DataFrameProxy, expectedResultDataFrameProxy);
            #endregion
        }

        [TestMethod]
        public void TestReplace()
        {
            // Arrange
            const string originalValue = "original";
            const string toReplaceValue = "toReplace";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Replace<string>(It.IsAny<object>(), 
                It.IsAny<Dictionary<string, string>>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.Replace(originalValue, toReplaceValue);

            // Assert
            mockDataFrameProxy.Verify(m => m.Replace("*", It.Is<Dictionary<string, string>>(dict => dict.Count == 1 &&
                dict.ContainsKey(originalValue) && dict[originalValue] == toReplaceValue))); // assert Replace of Proxy was invoked with correct parameters
            Assert.AreEqual(actualResultDataFrame.DataFrameProxy, expectedResultDataFrameProxy);
        }

        [TestMethod]
        public void TestReplaceAll_OneToOne()
        {
            // Arrange
            const string originalValue = "original";
            const string toReplaceValue = "toReplace";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Replace<string>(It.IsAny<object>(), 
                It.IsAny<Dictionary<string, string>>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.ReplaceAll(new []{originalValue}, new []{toReplaceValue});

            // Assert
            mockDataFrameProxy.Verify(m => m.Replace("*", It.Is<Dictionary<string, string>>(dict => dict.Count == 1 &&
                dict.ContainsKey(originalValue) && dict[originalValue] == toReplaceValue))); // assert Replace of Proxy was invoked with correct parameters
            Assert.AreEqual(actualResultDataFrame.DataFrameProxy, expectedResultDataFrameProxy);
        }

        [TestMethod]
        public void TestReplaceAll_ManyToOne()
        {
            // Arrange
            const string originalValue = "original";
            const string toReplaceValue = "toReplace";
            const string columnName = "column1";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Replace<string>(It.IsAny<object>(), 
                It.IsAny<Dictionary<string, string>>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.ReplaceAll(new[] { originalValue }, toReplaceValue, new[] { columnName });

            // Assert
            mockDataFrameProxy.Verify(m => m.Replace(It.Is<string[]>(subset => subset.Length == 1 && subset.Contains(columnName)), 
                It.Is<Dictionary<string, string>>(dict => dict.Count == 1 &&
                dict.ContainsKey(originalValue) && dict[originalValue] == toReplaceValue))); // assert Replace of Proxy was invoked with correct parameters
            Assert.AreEqual(actualResultDataFrame.DataFrameProxy, expectedResultDataFrameProxy);
        }
    }

}
