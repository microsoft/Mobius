// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between DataFrameReader and its proxies
    /// </summary>
    [TestFixture]
    public class DataFrameReaderTest
    {
        private static Mock<IDataFrameReaderProxy> mockDataFrameReaderProxy;
        private static SparkContext sparkContext;

        [OneTimeSetUp]
        public static void ClassInitialize()
        {
            mockDataFrameReaderProxy = new Mock<IDataFrameReaderProxy>();
            sparkContext = new SparkContext("", "");
        }

        [SetUp]
        public void TestInitialize()
        {
            mockDataFrameReaderProxy.Reset();
        }

        [Test]
        public void TestFormat()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Format(It.IsAny<string>()));
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);

            // act
            var reader = dataFrameReader.Format("json");

            // verify
            Assert.IsNotNull(reader);
            Assert.AreSame(reader, dataFrameReader);
            mockDataFrameReaderProxy.Verify(m => m.Format("json"), Times.Once);
        }

        [Test]
        public void TestSchema()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Schema(It.IsAny<StructType>()));
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);
            const string jsonSchema = @"
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
            var mockStructTypeProxy = new MockStructTypeProxy(jsonSchema);
            var schema = new StructType(mockStructTypeProxy);

            // act
            var reader = dataFrameReader.Schema(schema);

            // verify
            Assert.IsNotNull(reader);
            Assert.AreSame(reader, dataFrameReader);
            mockDataFrameReaderProxy.Verify(m => m.Schema(schema), Times.Once);
        }

        [Test]
        public void TestOption()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Options(It.IsAny<Dictionary<string, string>>()));
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);
            const string key = "path";
            const string value = "path_value";

            // Act
            dataFrameReader.Option(key, value);

            // Assert
            mockDataFrameReaderProxy.Verify(m => m.Options(
                It.Is<Dictionary<string, string>>(dict => dict[key] == value && dict.Count == 1)), Times.Once);
        }

        [Test]
        public void TestOptions()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Options(It.IsAny<Dictionary<string, string>>()));
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);
            const string key1 = "key1";
            const string value1 = "value1";
            const string key2 = "key2";
            const string value2 = "value2";

            var opts = new Dictionary<string, string>()
            {
                {key1, value1},
                {key2, value2}
            };

            // Act
            dataFrameReader.Options(opts);

            // Assert
            mockDataFrameReaderProxy.Verify(m => m.Options(It.Is<Dictionary<string, string>>(
                dict =>
                    dict[key1] == value1
                    && dict[key2] == value2
                    && dict.Count == 2)
                ),
                Times.Once
           );
        }

        [Test]
        public void TestLoadWithPath()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Load());
            mockDataFrameReaderProxy.Setup(m => m.Options(It.IsAny<Dictionary<string, string>>()));
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);
            const string key = "path";
            const string path = "path_value";
            // Act
            dataFrameReader.Load(path);

            // Assert
            mockDataFrameReaderProxy.Verify(m => m.Options(
                It.Is<Dictionary<string, string>>(dict => dict[key] == path && dict.Count == 1)), Times.Once);
            mockDataFrameReaderProxy.Verify(m => m.Load(), Times.Once);
        }

        [Test]
        public void TestLoad()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Load());
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);

            // Act
            dataFrameReader.Load();

            // Assert
            mockDataFrameReaderProxy.Verify(m => m.Load(), Times.Once);
        }

        [Test]
        public void TestJdbc1()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Jdbc(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Dictionary<string, string>>()));
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);

            // Act
            const string url = "url";
            const string table = "table_name";
            var properties = new Dictionary<string, string>()
            {
                {"prop1", "value1"},
                {"prop2", "value2"}
            };

            dataFrameReader.Jdbc(url, table, properties);

            // Assert
            mockDataFrameReaderProxy.Verify(m => m.Jdbc(url, table, properties), Times.Once);
        }

        [Test]
        public void TestJdbc2()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Jdbc(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), 
                It.IsAny<string>(), It.IsAny<int>(), It.IsAny<Dictionary<string, string>>()));
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);

            // Act
            const string url = "url";
            const string table = "table_name";
            const string columnName = "col1";
            const string lowerBound = "a";
            const string upperBound = "z";
            const int numPartitions = 5;
            var connectionProperties = new Dictionary<string, string>()
            {
                {"prop1", "value1"},
                {"prop2", "value2"}
            };

            dataFrameReader.Jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties);

            // Assert
            mockDataFrameReaderProxy.Verify(m => m.Jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties), 
                Times.Once);
        }

        [Test]
        public void TestJdbc3()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Jdbc(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string[]>(), It.IsAny<Dictionary<string, string>>()));
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);

            // Act
            const string url = "url";
            const string table = "table_name";
            var predicates = new[] { "predicate1", "predicate2" };
            var connectionProperties = new Dictionary<string, string>()
            {
                {"prop1", "value1"},
                {"prop2", "value2"}
            };

            dataFrameReader.Jdbc(url, table, predicates, connectionProperties);

            // Assert
            mockDataFrameReaderProxy.Verify(m => m.Jdbc(url, table, predicates, connectionProperties), Times.Once);
        }

        [Test]
        public void TestJson()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Format(It.IsAny<string>()));
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);

            // act
            const string path = "path/to/json";
            var reader = dataFrameReader.Json(path);

            // Assert
            Assert.IsNotNull(reader);
            mockDataFrameReaderProxy.Verify(m => m.Format("json"), Times.Once);
            mockDataFrameReaderProxy.Verify(m => m.Options(
                It.Is<Dictionary<string, string>>(dict => dict["path"] == path && dict.Count == 1)), Times.Once);
            mockDataFrameReaderProxy.Verify(m => m.Load(), Times.Once);
        }

        [Test]
        public void TestParquet()
        {
            // arrange
            mockDataFrameReaderProxy.Setup(m => m.Parquet(It.IsAny<string[]>()));
            var dataFrameReader = new DataFrameReader(mockDataFrameReaderProxy.Object, sparkContext);

            // act
            const string path1 = "path/to/json";
            const string path2 = "path/to/json";
            var reader = dataFrameReader.Parquet(path1, path2);

            // Assert
            Assert.IsNotNull(reader);
            mockDataFrameReaderProxy.Verify(m => m.Parquet(
                It.Is<string[]>(strArray => strArray.Length == 2 && strArray[0] == path1 && strArray[1] == path2)), Times.Once);
        }
    }
}
