// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Proxy;
using NUnit.Framework;
using Moq;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between DataFrame and its proxies
    /// </summary>
    [TestFixture]
    public class DataFrameTest
    {
        //TODO - complete impl

        private static Mock<IDataFrameProxy> mockDataFrameProxy;

        [OneTimeSetUp]
        public static void ClassInitialize()
        {
            mockDataFrameProxy = new Mock<IDataFrameProxy>();
        }

        [Test]
        public void TestInitialize()
        {
            mockDataFrameProxy.Reset();
        }

        [Test]
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

        [Test]
        public void TestDataFrameCollect()
        {
            const int localPort = 4000;
            const int size = 2;
            IDataFrameProxy dataFrameProxy = MockDataFrameProxyForCollect(localPort, size);
            DataFrame dataFrame = new DataFrame(dataFrameProxy, null);

            List<Row> rows = new List<Row>();
            foreach (var row in dataFrame.Collect())
            {
                rows.Add(row);
                Console.WriteLine("{0}", row);
            }

            Assert.AreEqual(rows.Count, size);
            const int index = 0;
            AssertRow(rows[index], index);
        }

        [Test]
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
            Assert.AreEqual(expectedResultDataFrameProxy, actualDataFrame.DataFrameProxy );
        }

        [Test]
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
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
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
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
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
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
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
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
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
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
            #endregion
        }

        [Test]
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
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
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
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
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
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
        public void TestRandomSplit()
        {
            // Arrange
            var weights = new double[] { 0.2, 0.8 };
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.RandomSplit(It.IsAny<IEnumerable<double>>(), It.IsAny<long?>())).Returns(new[] { expectedResultDataFrameProxy });
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.RandomSplit(weights);

            // Assert
            mockDataFrameProxy.Verify(m => m.RandomSplit(weights, It.IsAny<long?>())); // assert Drop of Proxy was invoked with correct parameters
            Assert.AreEqual(1, actualResultDataFrame.Count());
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.First().DataFrameProxy);
        }

        [Test]
        public void TestColumns()
        {
            // Arrange
            const string columnName = "column1";
            var mockSchemaProxy = new Mock<IStructTypeProxy>();
            var mockFieldProxy = new Mock<IStructFieldProxy>();
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockSchemaProxy.Object);
            mockSchemaProxy.Setup(m => m.GetStructTypeFields()).Returns(new List<IStructFieldProxy> { mockFieldProxy.Object });
            mockFieldProxy.Setup(m => m.GetStructFieldName()).Returns(columnName);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualColumns = originalDataFrame.Columns();

            // Assert
            CollectionAssert.AreEqual(new[] { columnName }, actualColumns.ToArray());
        }

        [Test]
        public void TestDTypes()
        {
            // Arrange
            const string columnName = "column1";
            const string columnType = "string";
            var mockSchemaProxy = new Mock<IStructTypeProxy>();
            var mockFieldProxy = new Mock<IStructFieldProxy>();
            var mockStructDataTypeProxy = new Mock<IStructDataTypeProxy>();
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.DropNa(It.IsAny<int?>(), It.IsAny<string[]>())).Returns(expectedResultDataFrameProxy);
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockSchemaProxy.Object);
            mockSchemaProxy.Setup(m => m.GetStructTypeFields()).Returns(new List<IStructFieldProxy> { mockFieldProxy.Object });
            mockFieldProxy.Setup(m => m.GetStructFieldName()).Returns(columnName);
            mockFieldProxy.Setup(m => m.GetStructFieldDataType()).Returns(mockStructDataTypeProxy.Object);
            mockStructDataTypeProxy.Setup(m => m.GetDataTypeSimpleString()).Returns(columnType);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualColumnNameAndDataType = originalDataFrame.DTypes().ToArray();

            // Assert
            Assert.AreEqual(1, actualColumnNameAndDataType.Length);
            Assert.AreEqual(columnName, actualColumnNameAndDataType[0].Item1);
            Assert.AreEqual(columnType, actualColumnNameAndDataType[0].Item2);
        }

        [Test]
        public void TestSort_ColumnNames()
        {
            // Arrange
            const string columnName = "column1";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            var mockColumnProxy = new Mock<IColumnProxy>();
            var mockSoretedColumnProxy = new Mock<IColumnProxy>();
            mockColumnProxy.Setup(m => m.UnaryOp(It.IsAny<string>())).Returns(mockSoretedColumnProxy.Object);
            mockDataFrameProxy.Setup(m => m.GetColumn(It.IsAny<string>())).Returns(mockColumnProxy.Object);
            mockDataFrameProxy.Setup(m => m.Sort(It.IsAny<IColumnProxy[]>())).Returns(expectedResultDataFrameProxy);

            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrameProxy = originalDataFrame.Sort(new[] { columnName }, new[] { true });

            // Assert
            mockDataFrameProxy.Verify(m => m.Sort(It.Is<IColumnProxy[]>(cp => cp.Length == 1 && cp[0] == mockSoretedColumnProxy.Object)));
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrameProxy.DataFrameProxy);
        }

        [Test]
        public void TestAlias()
        {
            // Arrange
            const string alias = "alias1";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Alias(It.IsAny<string>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.Alias(alias);

            // Assert
            mockDataFrameProxy.Verify(m => m.Alias(alias)); // assert Drop of Proxy was invoked with correct parameters
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
        public void TestRdd()
        {
            const string jsonSchema = @"
                {
                  ""type"" : ""struct"",
                  ""fields"" : [{
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

            Mock<IStructTypeProxy> mockStructTypeProxy = new Mock<IStructTypeProxy>();
            mockStructTypeProxy.Setup(m => m.ToJson()).Returns(jsonSchema);
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockStructTypeProxy.Object);

            var rows = new object[]
            {
                new RowImpl(new object[]
                {
                    34,
                    "123",
                    "Bill"
                }, 
                RowSchema.ParseRowSchemaFromJson(jsonSchema))
            };

            mockDataFrameProxy.Setup(m => m.JavaToCSharp()).Returns(new MockRddProxy(rows));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            // act
            var rdd = dataFrame.Rdd;

            Assert.IsNotNull(rdd);
            mockDataFrameProxy.Verify(m => m.JavaToCSharp(), Times.Once);
            mockStructTypeProxy.Verify(m => m.ToJson(), Times.Once);

            mockDataFrameProxy.Reset();
            mockStructTypeProxy.Reset();

            rdd = dataFrame.Rdd;
            Assert.IsNotNull(rdd);
            mockDataFrameProxy.Verify(m => m.JavaToCSharp(), Times.Never);
            mockStructTypeProxy.Verify(m => m.ToJson(), Times.Never);
        }

        [Test]
        public void TestIsLocal()
        {
            const bool isLocal = true;
            mockDataFrameProxy.Setup(m => m.IsLocal()).Returns(isLocal);
            var sc = new SparkContext(null);

            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            Assert.IsTrue(dataFrame.IsLocal);
            mockDataFrameProxy.Verify(m => m.IsLocal(), Times.Once()); // assert IsLocal of Proxy was invoked with correct parameters

            // reset
            mockDataFrameProxy.Reset();
            mockDataFrameProxy.Setup(m => m.IsLocal()).Returns(false);
            Assert.IsTrue(dataFrame.IsLocal);
            mockDataFrameProxy.Verify(m => m.IsLocal(), Times.Never());

            // reset
            mockDataFrameProxy.Reset();
            mockDataFrameProxy.Setup(m => m.IsLocal()).Returns(false);
            var dataFrame2 = new DataFrame(mockDataFrameProxy.Object, sc);
            Assert.IsFalse(dataFrame2.IsLocal);
            mockDataFrameProxy.Reset();
            Assert.IsFalse(dataFrame2.IsLocal);
            mockDataFrameProxy.Verify(m => m.IsLocal(), Times.Never());
        }

        [Test]
        public void TestCoalesce()
        {
            // arrange
            mockDataFrameProxy.Reset();
            mockDataFrameProxy.Setup(m => m.Coalesce(It.IsAny<int>()));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            const int numPartitions = 4;
            dataFrame.Coalesce(numPartitions);
            mockDataFrameProxy.Verify(m => m.Coalesce(numPartitions), Times.Once());
        }

        [Test]
        public void TestPersist()
        {
            // arrange
            mockDataFrameProxy.Setup(m => m.Persist(It.IsAny<StorageLevelType>()));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            // assert
            dataFrame.Persist();
            mockDataFrameProxy.Verify(m => m.Persist(StorageLevelType.MEMORY_AND_DISK), Times.Once());
        }

        [Test]
        public void TestUnpersist()
        {
            // arrange
            mockDataFrameProxy.Setup(m => m.Unpersist(It.IsAny<bool>()));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            dataFrame.Unpersist();
            mockDataFrameProxy.Verify(m => m.Unpersist(true), Times.Once());

            dataFrame.Unpersist(false);
            mockDataFrameProxy.Verify(m => m.Unpersist(false), Times.Once());
        }

        [Test]
        public void TestCache()
        {
            // arrange
            mockDataFrameProxy.Setup(m => m.Persist(It.IsAny<StorageLevelType>()));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            dataFrame.Cache();

            // assert
            mockDataFrameProxy.Verify(m => m.Persist(StorageLevelType.MEMORY_AND_DISK), Times.Once());
        }

        [Test]
        public void TestRepartition()
        {
            mockDataFrameProxy.Setup(m => m.Repartition(It.IsAny<int>()));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            const int numPartitions = 5;
            dataFrame.Repartition(numPartitions);

            // assert
            mockDataFrameProxy.Verify(m => m.Repartition(numPartitions), Times.Once());
        }

        [Test]
        public void TestSample()
        {
            // arrange
            mockDataFrameProxy.Setup(m => m.Sample(It.IsAny<bool>(), It.IsAny<double>(), It.IsAny<long>()));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            const bool withReplacement = false;
            const double fraction = 0.5;
            // without a seed
            dataFrame.Sample(withReplacement, fraction, null);
            mockDataFrameProxy.Verify(m => m.Sample(withReplacement, fraction, It.IsAny<long>()), Times.Once());

            mockDataFrameProxy.Reset();
            // specify a seed
            long seed = new Random().Next();
            dataFrame.Sample(withReplacement, fraction, seed);

            // assert
            mockDataFrameProxy.Verify(m => m.Sample(withReplacement, fraction, seed), Times.Once());
        }

        [Test]
        public void TestFlatMap()
        {
            // mock rddProxy
            const int count = 4;
            Mock<IRDDProxy> mockRddProxy = new Mock<IRDDProxy>();
            mockRddProxy.Setup(r => r.Count()).Returns(count);

            // mock sparkContextProxy
            Mock<ISparkContextProxy> mockSparkContextProxy = new Mock<ISparkContextProxy>();
            mockSparkContextProxy.Setup(ctx => ctx.CreateCSharpRdd(It.IsAny<IRDDProxy>(),
                It.IsAny<byte[]>(),
                null, null, It.IsAny<bool>(), null, null)).Returns(mockRddProxy.Object);
            var sc = new SparkContext(null);
            
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            SetPrivatePropertyValue(sc, "SparkContextProxy", mockSparkContextProxy.Object);
            SetPrivateFieldValue(dataFrame, "rdd", new RDD<Row>(mockRddProxy.Object, sc));

            var f = new Func<Row, IEnumerable<int>>(row => new int[] { row.Size() });

            RDD<int> rdd = dataFrame.FlatMap(f);

            // assert
            Assert.IsNotNull(rdd);
            Assert.AreEqual(count, rdd.Count());
        }

        [Test]
        public void TestMapPartitions()
        {
            // mock rddProxy
            const int count = 4;
            Mock<IRDDProxy> mockRddProxy = new Mock<IRDDProxy>();
            mockRddProxy.Setup(r => r.Count()).Returns(count);

            // mock sparkContextProxy
            Mock<ISparkContextProxy> mockSparkContextProxy = new Mock<ISparkContextProxy>();
            mockSparkContextProxy.Setup(ctx => ctx.CreateCSharpRdd(It.IsAny<IRDDProxy>(),
                It.IsAny<byte[]>(),
                null, null, It.IsAny<bool>(), null, null)).Returns(mockRddProxy.Object);
            var sc = new SparkContext(null);

            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            SetPrivatePropertyValue(sc, "SparkContextProxy", mockSparkContextProxy.Object);
            SetPrivateFieldValue(dataFrame, "rdd", new RDD<Row>(mockRddProxy.Object, sc));

            var f = new Func<IEnumerable<Row>, IEnumerable<int>>(iter => Enumerable.Repeat(1, iter.Count()));

            RDD<int> rdd = dataFrame.MapPartitions(f);

            // verify
            Assert.IsNotNull(rdd);
            Assert.AreEqual(count, rdd.Count());
        }

        [Test]
        public void TestLimit()
        {
            // arrange
            IDataFrameProxy limitedDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Limit(It.IsAny<int>())).Returns(limitedDataFrameProxy);

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            // act
            const int size = 2;
            DataFrame limitedDataFrame =dataFrame.Limit(size);

            // assert
            Assert.IsNotNull(limitedDataFrame.DataFrameProxy);
            Assert.AreEqual(limitedDataFrameProxy, limitedDataFrame.DataFrameProxy);
            mockDataFrameProxy.Verify(m => m.Limit(size), Times.Once());
        }

        [Test]
        public void TestHead()
        {
            // arrange
            const int size = 23;
            const int expectedSize = 5;
            const int localPort = 4001;
            IDataFrameProxy limitedDataFrameProxy = MockDataFrameProxyForCollect(localPort, expectedSize);
            mockDataFrameProxy.Setup(m => m.Limit(It.IsAny<int>())).Returns(limitedDataFrameProxy);

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            // act
            IEnumerable<Row> rows = dataFrame.Head(size);

            // assert
            Assert.IsNotNull(rows);
            Assert.AreEqual(expectedSize, rows.Count());
            mockDataFrameProxy.Verify(m => m.Limit(size), Times.Once());
        }

        [Test]
        public void TestFirst()
        {
            // arrange
            const int localPort = 4001;
            IDataFrameProxy limitedDataFrameProxy = MockDataFrameProxyForCollect(localPort, 1);
            mockDataFrameProxy.Setup(m => m.Limit(It.IsAny<int>())).Returns(limitedDataFrameProxy);

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            // act
            Row firstRow = dataFrame.First();

            // assert
            AssertRow(firstRow, 0);
            mockDataFrameProxy.Verify(m => m.Limit(1), Times.Once());
        }

        [Test]
        public void TestTake()
        {
            // arrange
            const int localPort = 4001;
            const int expectedSize = 5;
            IDataFrameProxy limitedDataFrameProxy = MockDataFrameProxyForCollect(localPort, expectedSize);
            mockDataFrameProxy.Setup(m => m.Limit(It.IsAny<int>())).Returns(limitedDataFrameProxy);

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            // act
            IEnumerable<Row> iter = dataFrame.Take(expectedSize);

            // assert
            Assert.IsNotNull(iter);
            Row[] rows = iter.ToArray();
            Assert.AreEqual(expectedSize, rows.Length);
            AssertRow(rows[0], 0);
            AssertRow(rows[2], 2);
            mockDataFrameProxy.Verify(m => m.Limit(expectedSize), Times.Once());
        }

        [Test]
        public void TestDistinct()
        {
            // arrange
            mockDataFrameProxy.Setup(m => m.Distinct()).Returns(new Mock<IDataFrameProxy>().Object);

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            dataFrame.Distinct();

            // assert
            mockDataFrameProxy.Verify(m => m.Distinct(), Times.Once());
        }

        [Test]
        public void TestForeachPartition()
        {
            // mock rddProxy
            const int count = 4;
            Mock<IRDDProxy> mockRddProxy = new Mock<IRDDProxy>();
            mockRddProxy.Setup(rdd => rdd.Count()).Returns(count);

            // mock sparkContextProxy
            Mock<ISparkContextProxy> mockSparkContextProxy = new Mock<ISparkContextProxy>();
            mockSparkContextProxy.Setup(ctx => ctx.CreateCSharpRdd(It.IsAny<IRDDProxy>(),
                It.IsAny<byte[]>(),
                null, null, It.IsAny<bool>(), null, null)).Returns(mockRddProxy.Object);

            var sc = new SparkContext(null);
            SetPrivatePropertyValue(sc, "SparkContextProxy", mockSparkContextProxy.Object);

            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            SetPrivateFieldValue(dataFrame, "rdd", new RDD<Row>(mockRddProxy.Object, sc));

            var f = new Action<IEnumerable<Row>>(iter => Console.WriteLine(iter.Count()));

            dataFrame.ForeachPartition(f);

            // assert
            mockRddProxy.Verify(m => m.Count(), Times.Once);
            mockSparkContextProxy.Verify(m => m.CreateCSharpRdd(It.IsAny<IRDDProxy>(),
                It.IsAny<byte[]>(),
                null, null, It.IsAny<bool>(), null, null), Times.Once);
        }

        [Test]
        public void TestForeach()
        {
            // mock rddProxy
            const int count = 4;
            Mock<IRDDProxy> mockRddProxy = new Mock<IRDDProxy>();
            mockRddProxy.Setup(rdd => rdd.Count()).Returns(count);

            // mock sparkContextProxy
            Mock<ISparkContextProxy> mockSparkContextProxy = new Mock<ISparkContextProxy>();
            mockSparkContextProxy.Setup(ctx => ctx.CreateCSharpRdd(It.IsAny<IRDDProxy>(),
                It.IsAny<byte[]>(),
                null, null, It.IsAny<bool>(), null, null)).Returns(mockRddProxy.Object);

            var sc = new SparkContext(null);
            SetPrivatePropertyValue(sc, "SparkContextProxy", mockSparkContextProxy.Object);

            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            SetPrivateFieldValue(dataFrame, "rdd", new RDD<Row>(mockRddProxy.Object, sc));

            var f = new Action<Row>(row => Console.WriteLine(row.ToString()));

            dataFrame.Foreach(f);

            // assert
            mockRddProxy.Verify(m => m.Count(), Times.Once);
            mockSparkContextProxy.Verify(m => m.CreateCSharpRdd(It.IsAny<IRDDProxy>(),
                It.IsAny<byte[]>(),
                null, null, It.IsAny<bool>(), null, null), Times.Once);
        }

        private static void AssertRow(Row row, int index)
        {
            Assert.IsNotNull(row);
            Assert.AreEqual("name" + index, row.GetAs<string>("name"));
            Assert.AreEqual("id" + index, row.GetAs<string>("id"));
            Row address = row.GetAs<Row>("address");
            Assert.AreNotEqual(address, null);
            string city = address.GetAs<string>("city");
            Assert.IsTrue(city.Equals("city" + index));
            string state = address.GetAs<string>("state");
            Assert.IsTrue(state.Equals("state" + index));
        }

        // Mock a IDataFrameProxy instance to test collect method
        private static IDataFrameProxy MockDataFrameProxyForCollect(int localPort, int size)
        {
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

            var rows = new List<object>();

            for (var i = 0; i < size; i++)
            {
                object row = new object[] 
                {
                    new object[] {"city" + i, "state" + i},
                    i,
                    "id" + i,
                    "name" +i
                };
                rows.Add(row);
            }

            IStructTypeProxy structTypeProxy = new MockStructTypeProxy(jsonSchema);
            return new MockDataFrameProxy(localPort, rows, structTypeProxy);
        }

        /// <summary>
        /// set value for a private field of an object
        /// </summary>
        private static void SetPrivateFieldValue<T>(object obj, string fieldName, T val)
        {
            Type t = obj.GetType();
            var fieldInfo = t.GetField(fieldName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            if (fieldInfo != null)
            {
                fieldInfo.SetValue(obj, val);
            }
            else
            {
                throw new Exception("Failed to find field: " + fieldName + " in type: " + t);
            }
        }

        /// <summary>
        /// set value for a private property of an object
        /// </summary>
        private static void SetPrivatePropertyValue<T>(object obj, string propName, T val)
        {
            Type t = obj.GetType();
            var propInfo = t.GetProperty(propName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            if (propInfo != null)
            {
                propInfo.SetValue(obj, val);
            }
            else
            {
                throw new Exception("Failed to find property: " + propName + " in type: " + t);
            }
        }
    }

}
