// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
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

        [SetUp]
        public void TestInitialize()
        {
            mockDataFrameProxy.Reset();
        }

        [TearDown]
        public void TestCleanUp()
        {
            // Revert to use Static mock class to prevent blocking other test methods which uses static mock class
            SparkCLREnvironment.SparkCLRProxy = new MockSparkCLRProxy();
        }

        [Test]
        public void TestDataFrameJoin()
        {
            var sqlContext = new SqlContext(new SparkContext("", ""));
            var dataFrame = sqlContext.Read().Json(@"c:\path\to\input.json");
            var dataFrame2 = sqlContext.Read().Json(@"c:\path\to\input2.json"); 
            var joinedDataFrame = dataFrame.Join(dataFrame2, "JoinCol");
            var paramValuesToJoinMethod = (joinedDataFrame.DataFrameProxy as MockDataFrameProxy).mockDataFrameReference as object[];
            var paramValuesToSecondDataFrameJsonFileMethod = ((paramValuesToJoinMethod[0] as MockDataFrameProxy).mockDataFrameReference as object[]);
            Assert.AreEqual(@"c:\path\to\input2.json", paramValuesToSecondDataFrameJsonFileMethod[0]);
            Assert.AreEqual("JoinCol", paramValuesToJoinMethod[1]);
        }

        
        [Test]
        public void TestDataFrameCollect()
        {
            var expectedRows = new Row[] {new MockRow(), new MockRow()};
            var mockRddProxy = new Mock<IRDDProxy>();
            var mockRddCollector = new Mock<IRDDCollector>();
            mockRddCollector.Setup(m => m.Collect(It.IsAny<int>(), It.IsAny<SerializedMode>(), It.IsAny<Type>()))
                .Returns(expectedRows);
            mockRddProxy.Setup(m => m.CollectAndServe()).Returns(123);
            mockRddProxy.Setup(m => m.RDDCollector).Returns(mockRddCollector.Object);
            mockDataFrameProxy.Setup(m => m.JavaToCSharp()).Returns(mockRddProxy.Object);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, null);

            var rows = new List<Row>();
            foreach (var row in dataFrame.Collect())
            {
                rows.Add(row);
                Console.WriteLine("{0}", row);
            }

            Assert.AreEqual(rows.Count, expectedRows.Length);
            Assert.AreEqual(expectedRows[0], rows[0]);
            Assert.AreEqual(expectedRows[1], rows[1]);
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
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockSchemaProxy.Object);

            // dataframeNaFunctionsProxy
            var dataFrameNaFunctionsProxy = new Mock<IDataFrameNaFunctionsProxy>();
            dataFrameNaFunctionsProxy.Setup(d => d.Drop(It.IsAny<int>(), It.IsAny<string[]>())).Returns(expectedResultDataFrameProxy);

            mockDataFrameProxy.Setup(m => m.Na()).Returns(dataFrameNaFunctionsProxy.Object);
            
            mockSchemaProxy.Setup(m => m.GetStructTypeFields()).Returns(new List<IStructFieldProxy> { mockFieldProxy.Object });
            mockFieldProxy.Setup(m => m.GetStructFieldName()).Returns(columnName);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.DropNa();

            // Assert
            // assert DropNa of Proxy was invoked with correct parameters
            dataFrameNaFunctionsProxy.Verify(m => m.Drop(1, It.Is<string[]>(subset => subset.Length == 1 && 
                subset.Contains(columnName))));
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
            mockDataFrameProxy.Verify(m => m.RandomSplit(weights, It.IsAny<long?>())); // assert RandomSplit was invoked with correct parameters
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
        public void TestSort()
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
        public void TestSortWithinPartitions()
        {
            // Arrange
            const string columnName = "column1";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            var mockColumnProxy = new Mock<IColumnProxy>();
            var mockSortedColumnProxy = new Mock<IColumnProxy>();
            mockColumnProxy.Setup(m => m.UnaryOp(It.IsAny<string>())).Returns(mockSortedColumnProxy.Object);
            mockDataFrameProxy.Setup(m => m.GetColumn(It.IsAny<string>())).Returns(mockColumnProxy.Object);
            mockDataFrameProxy.Setup(m => m.SortWithinPartitions(It.IsAny<IColumnProxy[]>())).Returns(expectedResultDataFrameProxy);

            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrameProxy = originalDataFrame.SortWithinPartitions(new[] { columnName });

            // Assert
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
            mockDataFrameProxy.Verify(m => m.Alias(alias)); // assert Alias was invoked with correct parameters
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
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockStructTypeProxy.Object);

            var rows = new object[]
            {
                new RowImpl(new object[]
                {
                    34,
                    "123",
                    "Bill"
                }, 
                DataType.ParseDataTypeFromJson(jsonSchema) as StructType)
            };

            mockDataFrameProxy.Setup(m => m.JavaToCSharp()).Returns(new MockRddProxy(rows));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            // act
            var rdd = dataFrame.Rdd;

            Assert.IsNotNull(rdd);
            mockDataFrameProxy.Verify(m => m.JavaToCSharp(), Times.Once);
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
        public void TestRepartition2()
        {
            // arrange
            mockDataFrameProxy.Setup(m => m.Repartition(It.IsAny<int>(), It.IsAny<IColumnProxy[]>()));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);

            const int numPartitions = 5;
            IColumnProxy mockColumn1Proxy = new Mock<IColumnProxy>().Object;
            Column mockColumn = new Column(mockColumn1Proxy);

            // act
            dataFrame.Repartition(new[] { mockColumn }, numPartitions);
            // assert
            mockDataFrameProxy.Verify(m => m.Repartition(numPartitions, new[] { mockColumn1Proxy }), Times.Once());

            // act
            dataFrame.Repartition(new[] { mockColumn });
            // assert
            mockDataFrameProxy.Verify(m => m.Repartition(new[] { mockColumn1Proxy }), Times.Once());
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
        public void TestMap()
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

            var f = new Func<Row, int>(row => row.Size());

            RDD<int> rdd = dataFrame.Map(f);

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
            var expectedRows = new Row[] {new MockRow(), new MockRow(), new MockRow(), new MockRow(), new MockRow()};
            var mockRddProxy = new Mock<IRDDProxy>();
            var mockRddCollector = new Mock<IRDDCollector>();
            mockRddCollector.Setup(m => m.Collect(It.IsAny<int>(), It.IsAny<SerializedMode>(), It.IsAny<Type>()))
                .Returns(expectedRows);
            mockRddProxy.Setup(m => m.CollectAndServe()).Returns(123);
            mockRddProxy.Setup(m => m.RDDCollector).Returns(mockRddCollector.Object);
            mockDataFrameProxy.Setup(m => m.JavaToCSharp()).Returns(mockRddProxy.Object);
            mockDataFrameProxy.Setup(m => m.Limit(It.IsAny<int>())).Returns(mockDataFrameProxy.Object);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, null);

            const int unUsedSizeValue = 100;

            // act
            IEnumerable<Row> rows = dataFrame.Head(unUsedSizeValue); //uses limit & collect
            
            // assert
            var rowsArray = rows.ToArray();
            Assert.IsNotNull(rows);
            Assert.AreEqual(expectedRows.Length, rowsArray.Length);
            for (int i = 0; i < rowsArray.Length; i++)
                Assert.AreEqual(expectedRows[i], rowsArray[i]);
            mockDataFrameProxy.Verify(m => m.Limit(unUsedSizeValue), Times.Once());
        }

        
        [Test]
        public void TestFirst()
        {
            // arrange
            var expectedRows = new Row[] { new MockRow(), new MockRow(), new MockRow(), new MockRow(), new MockRow() };
            var mockRddProxy = new Mock<IRDDProxy>();
            var mockRddCollector = new Mock<IRDDCollector>();
            mockRddCollector.Setup(m => m.Collect(It.IsAny<int>(), It.IsAny<SerializedMode>(), It.IsAny<Type>()))
                .Returns(expectedRows);
            mockRddProxy.Setup(m => m.CollectAndServe()).Returns(123);
            mockRddProxy.Setup(m => m.RDDCollector).Returns(mockRddCollector.Object);
            mockDataFrameProxy.Setup(m => m.JavaToCSharp()).Returns(mockRddProxy.Object);
            mockDataFrameProxy.Setup(m => m.Limit(It.IsAny<int>())).Returns(mockDataFrameProxy.Object);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, null);

            // act
            Row firstRow = dataFrame.First(); //uses limit & collect

            // assert
            Assert.AreEqual(expectedRows[0], firstRow);
            mockDataFrameProxy.Verify(m => m.Limit(1), Times.Once());
        }

        
        [Test]
        public void TestTake()
        {
            // arrange
            var expectedRows = new Row[] { new MockRow(), new MockRow(), new MockRow(), new MockRow(), new MockRow() };
            var mockRddProxy = new Mock<IRDDProxy>();
            var mockRddCollector = new Mock<IRDDCollector>();
            mockRddCollector.Setup(m => m.Collect(It.IsAny<int>(), It.IsAny<SerializedMode>(), It.IsAny<Type>()))
                .Returns(expectedRows);
            mockRddProxy.Setup(m => m.CollectAndServe()).Returns(123);
            mockRddProxy.Setup(m => m.RDDCollector).Returns(mockRddCollector.Object);
            mockDataFrameProxy.Setup(m => m.JavaToCSharp()).Returns(mockRddProxy.Object);
            mockDataFrameProxy.Setup(m => m.Limit(It.IsAny<int>())).Returns(mockDataFrameProxy.Object);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, null);

            const int unUsedSizeValue = 100;
            // act
            IEnumerable<Row> iter = dataFrame.Take(unUsedSizeValue);

            // assert
            Assert.IsNotNull(iter);
            Row[] rows = iter.ToArray();
            Assert.AreEqual(expectedRows.Length, rows.Length);
            for (int i = 0; i < rows.Length; i++)
                Assert.AreEqual(expectedRows[i], rows[i]);
            mockDataFrameProxy.Verify(m => m.Limit(unUsedSizeValue), Times.Once());
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
		
        [Test]
        public void TestSelect_Object_ObjectArray()
        {
            // Arrange
            const string column1Name = "colName";
            IColumnProxy mockColumn1Proxy = new Mock<IColumnProxy>().Object;
            IColumnProxy mockColumn2Proxy = new Mock<IColumnProxy>().Object;
            Column column2 = new Column(mockColumn2Proxy);
            var mockSparkCLRProxy = new Mock<ISparkCLRProxy>();
            var mockSparkContextProxy = new Mock<ISparkContextProxy>();
            mockSparkCLRProxy.Setup(m => m.SparkContextProxy).Returns(mockSparkContextProxy.Object);
            mockSparkCLRProxy.Setup(m => m.CreateSparkConf(It.IsAny<bool>())).Returns(new MockSparkConfProxy()); // some of mocks which rarely change can be kept
            SparkCLREnvironment.SparkCLRProxy = mockSparkCLRProxy.Object;
            mockSparkContextProxy.Setup(m => m.CreateFunction("col", column1Name)).Returns(mockColumn1Proxy);
            
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Select(It.IsAny<IEnumerable<IColumnProxy>>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.Select(column1Name, column2);

            // Assert
            mockDataFrameProxy.Verify(m => m.Select(It.Is<IEnumerable<IColumnProxy>>(clist => 
                clist.Any(cp => cp == mockColumn1Proxy) && clist.Any(cp => cp == mockColumn2Proxy)))); 
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
        public void TestWithColumn()
        {
            // Arrange
            const string alias = "alias";
            IColumnProxy mockColumnStarProxy = new Mock<IColumnProxy>().Object;
            var mockColumn2Proxy = new Mock<IColumnProxy>();
            var mockRenamedColumn2Proxy = new Mock<IColumnProxy>().Object;
            mockColumn2Proxy.Setup(m => m.InvokeMethod("as", alias)).Returns(mockRenamedColumn2Proxy);
            Column column2 = new Column(mockColumn2Proxy.Object);
            var mockSparkCLRProxy = new Mock<ISparkCLRProxy>();
            var mockSparkContextProxy = new Mock<ISparkContextProxy>();
            mockSparkCLRProxy.Setup(m => m.SparkContextProxy).Returns(mockSparkContextProxy.Object);
            mockSparkCLRProxy.Setup(m => m.CreateSparkConf(It.IsAny<bool>())).Returns(new MockSparkConfProxy()); // some of mocks which rarely change can be kept
            SparkCLREnvironment.SparkCLRProxy = mockSparkCLRProxy.Object;
            mockSparkContextProxy.Setup(m => m.CreateFunction("col", "*")).Returns(mockColumnStarProxy);

            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Select(It.IsAny<IEnumerable<IColumnProxy>>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.WithColumn(alias, column2);

            // Assert
            mockDataFrameProxy.Verify(m => m.Select(It.Is<IEnumerable<IColumnProxy>>(clist =>
                clist.Count() == 2 && clist.Any(cp => cp == mockColumnStarProxy) && clist.Any(cp => cp == mockRenamedColumn2Proxy))));
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
        public void TestWithColumnRenamed()
        {
            // Arrange
            const string existingColumn = "existCol";
            const string alias = "alias";
            var mockColumnProxy = new Mock<IColumnProxy>();
            var mockRenamedColumnProxy = new Mock<IColumnProxy>().Object;
            mockColumnProxy.Setup(m => m.InvokeMethod("as", alias)).Returns(mockRenamedColumnProxy);
            var mockSparkCLRProxy = new Mock<ISparkCLRProxy>();
            var mockSparkContextProxy = new Mock<ISparkContextProxy>();
            mockSparkCLRProxy.Setup(m => m.SparkContextProxy).Returns(mockSparkContextProxy.Object);
            mockSparkCLRProxy.Setup(m => m.CreateSparkConf(It.IsAny<bool>())).Returns(new MockSparkConfProxy()); // some of mocks which rarely change can be kept
            SparkCLREnvironment.SparkCLRProxy = mockSparkCLRProxy.Object;

            var mockSchemaProxy = new Mock<IStructTypeProxy>();
            var mockFieldProxy = new Mock<IStructFieldProxy>();
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockSchemaProxy.Object);
            mockSchemaProxy.Setup(m => m.GetStructTypeFields()).Returns(new List<IStructFieldProxy> { mockFieldProxy.Object });
            mockFieldProxy.Setup(m => m.GetStructFieldName()).Returns(existingColumn);
            mockDataFrameProxy.Setup(m => m.GetColumn(existingColumn)).Returns(mockColumnProxy.Object);

            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Select(It.IsAny<IEnumerable<IColumnProxy>>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResultDataFrame = originalDataFrame.WithColumnRenamed(existingColumn, alias);

            // Assert
            mockDataFrameProxy.Verify(m => m.Select(It.Is<IEnumerable<IColumnProxy>>(clist => clist.Count() == 1 &&
                clist.Any(cp => cp == mockRenamedColumnProxy))));
            Assert.AreEqual(expectedResultDataFrameProxy, actualResultDataFrame.DataFrameProxy);
        }

        [Test]
        public void TestCorr()
        {
            // Arrange
            const string column1 = "col1";
            const string column2 = "col2";
            const string method = "pearson";
            const double expectedResult = 1.0;
            mockDataFrameProxy.Setup(m => m.Corr(column1, column2, method)).Returns(expectedResult);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.Corr(column1, column2);

            // Assert
            mockDataFrameProxy.Verify(m => m.Corr(column1, column2, method)); // assert Corr was invoked with correct parameters
            Assert.AreEqual(expectedResult, actualResult);
        }

        [Test]
        public void TestCov()
        {
            // Arrange
            const string column1 = "col1";
            const string column2 = "col2";
            const double expectedResult = 100;
            mockDataFrameProxy.Setup(m => m.Cov(column1, column2)).Returns(expectedResult);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.Cov(column1, column2);

            // Assert
            mockDataFrameProxy.Verify(m => m.Cov(column1, column2)); // assert Cov was invoked with correct parameters
            Assert.AreEqual(expectedResult, actualResult);
        }

        [Test]
        public void TestCrosstab()
        {
            // Arrange
            const string column1 = "col1";
            const string column2 = "col2";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Crosstab(column1, column2)).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.Crosstab(column1, column2);

            // Assert
            mockDataFrameProxy.Verify(m => m.Crosstab(column1, column2)); // assert Crosstab was invoked with correct parameters
            Assert.AreEqual(expectedResultDataFrameProxy, actualResult.DataFrameProxy);
        }

        [Test]
        public void TestFreqItems()
        {
            // Arrange
            const string column1 = "col1";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.FreqItems(It.IsAny<IEnumerable<string>>(), It.IsAny<double>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.FreqItems(new[]{column1});

            // Assert
            mockDataFrameProxy.Verify(m => m.FreqItems(It.Is<IEnumerable<string>>(
                cols => cols.Count() == 1 && cols.Any(c => c == column1)), 0.01)); // assert FreqItems was invoked with correct parameters
            Assert.AreEqual(expectedResultDataFrameProxy, actualResult.DataFrameProxy);
        }

        [Test]
        public void TestDescribe()
        {
            // Arrange
            const string column1 = "age";
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Describe(It.IsAny<string[]>())).Returns(expectedResultDataFrameProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.Describe(column1);

            // Assert
            mockDataFrameProxy.Verify(m => m.Describe(It.Is<string[]>(
                cols => cols.Count() == 1 && cols.Any(c => c == column1)))); // assert Describe was invoked with correct parameters
            Assert.AreEqual(expectedResultDataFrameProxy, actualResult.DataFrameProxy);
        }

        [Test]
        public void TestRollup()
        {
            // Arrange
            const string column1 = "age";
            var expectedResultGroupedDataProxy = new Mock<IGroupedDataProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Rollup(It.IsAny<string>(), It.IsAny<string[]>())).Returns(expectedResultGroupedDataProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.Rollup(column1);

            // Assert
            mockDataFrameProxy.Verify(m => m.Rollup(column1, new string[0])); // assert Rollup was invoked with correct parameters
            Assert.AreEqual(expectedResultGroupedDataProxy, actualResult.GroupedDataProxy);
        }

        [Test]
        public void TestCube()
        {
            // Arrange
            const string column1 = "age";
            var expectedResultGroupedDataProxy = new Mock<IGroupedDataProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Cube(It.IsAny<string>(), It.IsAny<string[]>())).Returns(expectedResultGroupedDataProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.Cube(column1);

            // Assert
            mockDataFrameProxy.Verify(m => m.Cube(column1, new string[0])); // assert Cube was invoked with correct parameters
            Assert.AreEqual(expectedResultGroupedDataProxy, actualResult.GroupedDataProxy);
        }

        [Test]
        public void TestWrite()
        {
            // Arrange
            var expectedDataFrameWriterProxy = new Mock<IDataFrameWriterProxy>().Object;
            mockDataFrameProxy.Setup(m => m.Write()).Returns(expectedDataFrameWriterProxy);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var dataFrameWriter = originalDataFrame.Write();

            // Assert
            mockDataFrameProxy.Verify(m => m.Write(), Times.Once); 
            Assert.AreEqual(expectedDataFrameWriterProxy, dataFrameWriter.DataFrameWriterProxy);
        }

        #region GroupedDataTest

        [Test]
        public void TestCount()
        {
            // Arrange
            const string column1 = "age";
            var mockGroupedDataProxy = new Mock<IGroupedDataProxy>();
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockGroupedDataProxy.Setup(m => m.Count()).Returns(expectedResultDataFrameProxy);
            mockDataFrameProxy.Setup(m => m.GroupBy(It.IsAny<string>(), It.IsAny<string[]>())).Returns(mockGroupedDataProxy.Object);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.GroupBy(column1).Count();

            // Assert
            mockGroupedDataProxy.Verify(m => m.Count()); // assert Count was invoked with correct parameters
            Assert.AreEqual(expectedResultDataFrameProxy, actualResult.DataFrameProxy);
        }

        [Test]
        public void TestMean()
        {
            // Arrange
            const string column1 = "name";
            const string columnMean = "age";
            var mockGroupedDataProxy = new Mock<IGroupedDataProxy>();
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockGroupedDataProxy.Setup(m => m.Mean(columnMean)).Returns(expectedResultDataFrameProxy);
            mockDataFrameProxy.Setup(m => m.GroupBy(It.IsAny<string>(), It.IsAny<string[]>())).Returns(mockGroupedDataProxy.Object);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.GroupBy(column1).Mean(columnMean);

            // Assert
            mockGroupedDataProxy.Verify(m => m.Mean(columnMean)); // assert Count was invoked with correct parameters
            Assert.AreEqual(expectedResultDataFrameProxy, actualResult.DataFrameProxy);
        }

        [Test]
        public void TestMax()
        {
            // Arrange
            const string column1 = "name";
            const string columnMean = "age";
            var mockGroupedDataProxy = new Mock<IGroupedDataProxy>();
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockGroupedDataProxy.Setup(m => m.Max(columnMean)).Returns(expectedResultDataFrameProxy);
            mockDataFrameProxy.Setup(m => m.GroupBy(It.IsAny<string>(), It.IsAny<string[]>())).Returns(mockGroupedDataProxy.Object);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.GroupBy(column1).Max(columnMean);

            // Assert
            mockGroupedDataProxy.Verify(m => m.Max(columnMean)); // assert Count was invoked with correct parameters
            Assert.AreEqual(expectedResultDataFrameProxy, actualResult.DataFrameProxy);
        }

        [Test]
        public void TestMin()
        {
            // Arrange
            const string column1 = "name";
            const string columnMean = "age";
            var mockGroupedDataProxy = new Mock<IGroupedDataProxy>();
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockGroupedDataProxy.Setup(m => m.Min(columnMean)).Returns(expectedResultDataFrameProxy);
            mockDataFrameProxy.Setup(m => m.GroupBy(It.IsAny<string>(), It.IsAny<string[]>())).Returns(mockGroupedDataProxy.Object);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.GroupBy(column1).Min(columnMean);

            // Assert
            mockGroupedDataProxy.Verify(m => m.Min(columnMean)); // assert Count was invoked with correct parameters
            Assert.AreEqual(expectedResultDataFrameProxy, actualResult.DataFrameProxy);
        }

        [Test]
        public void TestAvg()
        {
            // Arrange
            const string column1 = "name";
            const string columnMean = "age";
            var mockGroupedDataProxy = new Mock<IGroupedDataProxy>();
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockGroupedDataProxy.Setup(m => m.Avg(columnMean)).Returns(expectedResultDataFrameProxy);
            mockDataFrameProxy.Setup(m => m.GroupBy(It.IsAny<string>(), It.IsAny<string[]>())).Returns(mockGroupedDataProxy.Object);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.GroupBy(column1).Avg(columnMean);

            // Assert
            mockGroupedDataProxy.Verify(m => m.Avg(columnMean)); // assert Count was invoked with correct parameters
            Assert.AreEqual(expectedResultDataFrameProxy, actualResult.DataFrameProxy);
        }

        [Test]
        public void TestSum()
        {
            // Arrange
            const string column1 = "name";
            const string columnMean = "age";
            var mockGroupedDataProxy = new Mock<IGroupedDataProxy>();
            var expectedResultDataFrameProxy = new Mock<IDataFrameProxy>().Object;
            mockGroupedDataProxy.Setup(m => m.Sum(columnMean)).Returns(expectedResultDataFrameProxy);
            mockDataFrameProxy.Setup(m => m.GroupBy(It.IsAny<string>(), It.IsAny<string[]>())).Returns(mockGroupedDataProxy.Object);
            var sc = new SparkContext(null);

            // Act
            var originalDataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            var actualResult = originalDataFrame.GroupBy(column1).Sum(columnMean);

            // Assert
            mockGroupedDataProxy.Verify(m => m.Sum(columnMean)); // assert Count was invoked with correct parameters
            Assert.AreEqual(expectedResultDataFrameProxy, actualResult.DataFrameProxy);
        }

        #endregion

        [Test]
        public void TestSaveAsParquetFile()
        {
            Mock<IDataFrameWriterProxy> mockDataFrameWriterProxy = new Mock<IDataFrameWriterProxy>();

            // arrange
            mockDataFrameProxy.Setup(m => m.Write()).Returns(mockDataFrameWriterProxy.Object);
            mockDataFrameWriterProxy.Setup(m => m.Format(It.IsAny<string>()));
            mockDataFrameWriterProxy.Setup(m => m.Save());
            mockDataFrameWriterProxy.Setup(m => m.Options(It.IsAny<Dictionary<string, string>>()));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            const string path = "file_path";

            // Act
            dataFrame.Write().Parquet(path);

            // assert
            mockDataFrameProxy.Verify(m => m.Write(), Times.Once());
           
            mockDataFrameWriterProxy.Verify(m => m.Save(), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.Format("parquet"), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.Options(
                It.Is<Dictionary<string, string>>(dict => dict["path"] == "file_path" && dict.Count == 1)), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.Save(), Times.Once);
        }

        [Test]
        public void TestInsertInto()
        {
            Mock<IDataFrameWriterProxy> mockDataFrameWriterProxy = new Mock<IDataFrameWriterProxy>();

            // arrange
            mockDataFrameProxy.Setup(m => m.Write()).Returns(mockDataFrameWriterProxy.Object);
            mockDataFrameWriterProxy.Setup(m => m.Mode(It.IsAny<string>()));
            mockDataFrameWriterProxy.Setup(m => m.SaveAsTable(It.IsAny<string>()));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            const string table = "table_name";
            // Act
            dataFrame.Write().Mode(SaveMode.Overwrite).SaveAsTable(table);

            // assert
            mockDataFrameProxy.Verify(m => m.Write(), Times.Once());

            mockDataFrameWriterProxy.Verify(m => m.SaveAsTable(table), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.Mode(SaveMode.Overwrite.GetStringValue()), Times.Once);
        }

        [Test]
        public void TestSaveAsTable()
        {
            Mock<IDataFrameWriterProxy> mockDataFrameWriterProxy = new Mock<IDataFrameWriterProxy>();

            // arrange
            mockDataFrameProxy.Setup(m => m.Write()).Returns(mockDataFrameWriterProxy.Object);
            mockDataFrameWriterProxy.Setup(m => m.Format(It.IsAny<string>()));
            mockDataFrameWriterProxy.Setup(m => m.Mode(It.IsAny<string>()));
            mockDataFrameWriterProxy.Setup(m => m.Options(It.IsAny<Dictionary<string, string>>()));
            mockDataFrameWriterProxy.Setup(m => m.SaveAsTable(It.IsAny<string>()));

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            const string table = "table_name";
            
            // Act
            dataFrame.Write().Mode(SaveMode.ErrorIfExists).SaveAsTable(table);

            // assert
            mockDataFrameProxy.Verify(m => m.Write(), Times.Once());
            mockDataFrameWriterProxy.Verify(m => m.Mode(SaveMode.ErrorIfExists.GetStringValue()), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.SaveAsTable(table), Times.Once);
        }

        [Test]
        public void TestSave()
        {
            Mock<IDataFrameWriterProxy> mockDataFrameWriterProxy = new Mock<IDataFrameWriterProxy>();

            // arrange
            mockDataFrameProxy.Setup(m => m.Write()).Returns(mockDataFrameWriterProxy.Object);
            mockDataFrameWriterProxy.Setup(m => m.Format(It.IsAny<string>()));
            mockDataFrameWriterProxy.Setup(m => m.Mode(It.IsAny<string>()));
            mockDataFrameWriterProxy.Setup(m => m.Options(It.IsAny<Dictionary<string, string>>()));
            mockDataFrameWriterProxy.Setup(m => m.Save());

            var sc = new SparkContext(null);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sc);
            const string path = "path_value";

            // Act
            dataFrame.Write().Mode(SaveMode.ErrorIfExists).Save(path);

            // assert
            mockDataFrameProxy.Verify(m => m.Write(), Times.Once());
            mockDataFrameWriterProxy.Verify(m => m.Mode(SaveMode.ErrorIfExists.GetStringValue()), Times.Once);
            // mockDataFrameWriterProxy.Verify(m => m.Format(null), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.Options(It.IsAny<Dictionary<string,string>>()), Times.Exactly(1));
            mockDataFrameWriterProxy.Verify(m => m.Save(), Times.Once);
        }
    }
}
