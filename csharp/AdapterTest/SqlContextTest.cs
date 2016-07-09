// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;
using NUnit.Framework;
using Moq;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using System.Collections.Generic;
using Tests.Common;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between SqlContext and its proxies
    /// </summary>
    [TestFixture]
    public class SqlContextTest
    {
        private static Mock<ISqlContextProxy> mockSqlContextProxy;

        [OneTimeSetUp]
        public static void ClassInitialize()
        {
            mockSqlContextProxy = new Mock<ISqlContextProxy>();
        }

        [SetUp]
        public void TestInitialize()
        {
            mockSqlContextProxy.Reset();
        }

        [TearDown]
        public void TestCleanUp()
        {
            // Revert to use Static mock class to prevent blocking other test methods which uses static mock class
            SparkCLREnvironment.SparkCLRProxy = new MockSparkCLRProxy();
        }

        [Test]
        public void TestSqlContextConstructor()
        {
            var sqlContext = new SqlContext(new SparkContext("", ""));
            Assert.IsNotNull((sqlContext.SqlContextProxy as MockSqlContextProxy).mockSqlContextReference);
        }

        [Test]
        public void TestSqlContextGetOrCreate()
        {
            var sqlContext = SqlContext.GetOrCreate(new SparkContext("", ""));
            Assert.IsNotNull(sqlContext.SqlContextProxy);
        }

        [Test]
        public void TestSqlContextNewSession()
        {
            // arrange
            var sessionProxy = new SqlContextIpcProxy(new JvmObjectReference("1"));
            mockSqlContextProxy.Setup(m => m.NewSession()).Returns(sessionProxy);
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);

            // act
            var actualNewSession = sqlContext.NewSession();

            // assert
            Assert.AreEqual(sessionProxy, actualNewSession.SqlContextProxy);
        }

        [Test]
        public void TestSqlContextGetConf()
        {
            // arrange
            const string key = "key";
            const string value = "value";
            mockSqlContextProxy.Setup(m => m.GetConf(key, "")).Returns(value);
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);

            // act
            var actualValue = sqlContext.GetConf(key, "");

            // assert
            Assert.AreEqual(value, actualValue);
        }

        [Test]
        public void TestSqlContextSetConf()
        {
            // arrange
            const string key = "key";
            const string value = "value";
            mockSqlContextProxy.Setup(m => m.SetConf(key, value));
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);

            // act
            sqlContext.SetConf(key, value);

            // assert
            mockSqlContextProxy.Verify(m => m.SetConf(key, value));
        }

        [Test]
        public void TestSqlContextReadDataFrame()
        {
            var dataFrameProxy = new DataFrameIpcProxy(new JvmObjectReference("1"), mockSqlContextProxy.Object);
            mockSqlContextProxy.Setup(
                m => m.ReadDataFrame(It.IsAny<string>(), It.IsAny<StructType>(), It.IsAny<Dictionary<string, string>>()))
                .Returns(dataFrameProxy);
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);
            var structTypeProxy = new Mock<IStructTypeProxy>();
            structTypeProxy.Setup(m => m.ToJson()).Returns(RowHelper.BasicJsonSchema);
            // act
            var dataFrame = sqlContext.ReadDataFrame(@"c:\path\to\input.txt", new StructType(structTypeProxy.Object), null);

            // assert
            Assert.AreEqual(dataFrameProxy, dataFrame.DataFrameProxy);
        }

        [Test]
        public void TestSqlContextCreateDataFrame()
        {
            // arrange
            var mockSparkContextProxy = new Mock<ISparkContextProxy>();
            mockSparkContextProxy.Setup(m => m.CreateCSharpRdd(It.IsAny<IRDDProxy>(), It.IsAny<byte[]>(), It.IsAny<Dictionary<string, string>>(),
                It.IsAny<List<string>>(), It.IsAny<bool>(), It.IsAny<List<Broadcast>>(), It.IsAny<List<byte[]>>()));
            var rddProxy = new Mock<IRDDProxy>();
            var rdd = new RDD<object[]>(rddProxy.Object, new SparkContext(mockSparkContextProxy.Object, new SparkConf()));
            var dataFrameProxy = new DataFrameIpcProxy(new JvmObjectReference("1"), mockSqlContextProxy.Object);
            mockSqlContextProxy.Setup(m => m.CreateDataFrame(It.IsAny<IRDDProxy>(), It.IsAny<IStructTypeProxy>())).Returns(dataFrameProxy);
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);
            var structTypeProxy = new Mock<IStructTypeProxy>();
            structTypeProxy.Setup(m => m.ToJson()).Returns(RowHelper.ComplexJsonSchema);
            // act
            var dataFrame = sqlContext.CreateDataFrame(rdd, new StructType(structTypeProxy.Object));

            // assert
            Assert.AreEqual(dataFrameProxy, dataFrame.DataFrameProxy);
        }

        [Test]
        public void TestSqlContextRegisterDataFrameAsTable()
        {
            // arrange
            mockSqlContextProxy.Setup(m => m.RegisterDataFrameAsTable(It.IsAny<IDataFrameProxy>(), It.IsAny<string>()));
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);
            var dataFrameProxy = new DataFrameIpcProxy(new JvmObjectReference("1"), mockSqlContextProxy.Object);
            var dataFrame = new DataFrame(dataFrameProxy, new SparkContext(new SparkConf()));

            // act
            sqlContext.RegisterDataFrameAsTable(dataFrame, "table");

            // assert
            mockSqlContextProxy.Verify(m => m.RegisterDataFrameAsTable(dataFrameProxy, "table"));
        }

        [Test]
        public void TestSqlContextDropTempTable()
        {
            // arrange
            mockSqlContextProxy.Setup(m => m.DropTempTable(It.IsAny<string>()));
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);

            // act
            sqlContext.DropTempTable("table");

            // assert
            mockSqlContextProxy.Verify(m => m.DropTempTable("table"));
        }

        [Test]
        public void TestSqlContextTable()
        {
            // arrange
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);
            var dataFrameProxy = new DataFrameIpcProxy(new JvmObjectReference("1"), mockSqlContextProxy.Object);
            mockSqlContextProxy.Setup(m => m.Table(It.IsAny<string>())).Returns(dataFrameProxy);

            // act
            var actualTableDataFrame = sqlContext.Table("table");

            // assert
            Assert.AreEqual(dataFrameProxy, actualTableDataFrame.DataFrameProxy);
        }

        [Test]
        public void TestSqlContextTables()
        {
            // arrange
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);
            var dataFrameProxy = new DataFrameIpcProxy(new JvmObjectReference("1"), mockSqlContextProxy.Object);
            mockSqlContextProxy.Setup(m => m.Tables()).Returns(dataFrameProxy);
            mockSqlContextProxy.Setup(m => m.Tables(It.IsAny<string>())).Returns(dataFrameProxy);

            // act
            var actualTablesDataFrame = sqlContext.Tables();

            // assert
            Assert.AreEqual(dataFrameProxy, actualTablesDataFrame.DataFrameProxy);

            // act
            actualTablesDataFrame = sqlContext.Tables("db");

            // assert
            Assert.AreEqual(dataFrameProxy, actualTablesDataFrame.DataFrameProxy);
        }

        [Test]
        public void TestSqlContextTableNames()
        {
            // arrange
            string[] tableNames = new string[] { "table1", "table2" };
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);
            mockSqlContextProxy.Setup(m => m.TableNames()).Returns(tableNames);
            mockSqlContextProxy.Setup(m => m.TableNames(It.IsAny<string>())).Returns(tableNames);

            // act
            var actualTableNames = sqlContext.TableNames();

            // assert
            Assert.AreEqual(tableNames, actualTableNames);

            // act
            actualTableNames = sqlContext.TableNames("db");

            // assert
            Assert.AreEqual(tableNames, actualTableNames);
        }

        [Test]
        public void TestSqlContextCacheTable()
        {
            // arrange
            mockSqlContextProxy.Setup(m => m.CacheTable(It.IsAny<string>()));
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);

            // act
            sqlContext.CacheTable("table");

            // assert
            mockSqlContextProxy.Verify(m => m.CacheTable("table"));
        }

        [Test]
        public void TestSqlContextUncacheTable()
        {
            // arrange
            mockSqlContextProxy.Setup(m => m.UncacheTable(It.IsAny<string>()));
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);

            // act
            sqlContext.UncacheTable("table");

            // assert
            mockSqlContextProxy.Verify(m => m.UncacheTable("table"));
        }

        [Test]
        public void TestSqlContextClearCache()
        {
            // arrange
            mockSqlContextProxy.Setup(m => m.ClearCache());
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);

            // act
            sqlContext.ClearCache();

            // assert
            mockSqlContextProxy.Verify(m => m.ClearCache());
        }

        [Test]
        public void TestSqlContextIsCached()
        {
            // arrange
            mockSqlContextProxy.Setup(m => m.IsCached(It.IsAny<string>()));
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);

            // act
            sqlContext.IsCached("table");

            // assert
            mockSqlContextProxy.Verify(m => m.IsCached("table"));
        }

        [Test]
        public void TestSqlContextSql()
        {
            var sqlContext = new SqlContext(new SparkContext("", ""));
            var dataFrame = sqlContext.Sql("Query of SQL text");
            var paramValuesToJsonFileMethod = (dataFrame.DataFrameProxy as MockDataFrameProxy).mockDataFrameReference;
            Assert.AreEqual("Query of SQL text", paramValuesToJsonFileMethod[0]);
        }

        [Test]
        public void TestSqlContextJsonFile()
        {
            var sqlContext = new SqlContext(new SparkContext("", "")); 
            var dataFrame = sqlContext.Read().Json(@"c:\path\to\input.json");
            var paramValuesToJsonFileMethod = (dataFrame.DataFrameProxy as MockDataFrameProxy).mockDataFrameReference;
            Assert.AreEqual(@"c:\path\to\input.json", paramValuesToJsonFileMethod[0]);
        }

        [Test]
        public void TestSqlContextTextFile()
        {
            var sqlContext = new SqlContext(new SparkContext("", ""));
            var dataFrame = sqlContext.TextFile(@"c:\path\to\input.txt");
            var paramValuesToTextFileMethod = (dataFrame.DataFrameProxy as MockDataFrameProxy).mockDataFrameReference;
            Assert.AreEqual(@"c:\path\to\input.txt", paramValuesToTextFileMethod[0]);
            Assert.AreEqual(@",", paramValuesToTextFileMethod[1]);
            Assert.IsFalse(bool.Parse(paramValuesToTextFileMethod[2].ToString()));
            Assert.IsFalse(bool.Parse(paramValuesToTextFileMethod[3].ToString()));

            sqlContext = new SqlContext(new SparkContext("", "")); 
            dataFrame = sqlContext.TextFile(@"c:\path\to\input.txt", "|", true, true);
            paramValuesToTextFileMethod = (dataFrame.DataFrameProxy as MockDataFrameProxy).mockDataFrameReference;
            Assert.AreEqual(@"c:\path\to\input.txt", paramValuesToTextFileMethod[0]);
            Assert.AreEqual(@"|", paramValuesToTextFileMethod[1]);
            Assert.IsTrue(bool.Parse(paramValuesToTextFileMethod[2].ToString()));
            Assert.IsTrue(bool.Parse(paramValuesToTextFileMethod[3].ToString()));

            // Test with a given schema
            sqlContext = new SqlContext(new SparkContext("", ""));
            var structTypeProxy = new Mock<IStructTypeProxy>();
            structTypeProxy.Setup(m => m.ToJson()).Returns(RowHelper.BasicJsonSchema);
            var structType = new StructType(structTypeProxy.Object);
            dataFrame = sqlContext.TextFile(@"c:\path\to\input.txt", structType);
            paramValuesToTextFileMethod = (dataFrame.DataFrameProxy as MockDataFrameProxy).mockDataFrameReference;
            Assert.AreEqual(@"c:\path\to\input.txt", paramValuesToTextFileMethod[0]);
            Assert.AreEqual(structType, paramValuesToTextFileMethod[1]);
            Assert.AreEqual(@",", paramValuesToTextFileMethod[2]);
        }

        [Test]
        public void TestSqlContextRegisterFunction()
        {
            mockSqlContextProxy.Setup(m => m.RegisterFunction(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<string>()));
            var sqlContext = new SqlContext(new SparkContext("", ""), mockSqlContextProxy.Object);

            sqlContext.RegisterFunction("Func0", () => "Func0");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func0", It.IsAny<byte[]>(), "string"));

            sqlContext.RegisterFunction<string, string>("Func1", s => "Func1");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func1", It.IsAny<byte[]>(), "string"));

            sqlContext.RegisterFunction<string, string, string>("Func2", (s1, s2) => "Func2");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func2", It.IsAny<byte[]>(), "string"));

            sqlContext.RegisterFunction<string, string, string, string>("Func3", (s1, s2, s3) => "Func3");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func3", It.IsAny<byte[]>(), "string"));

            sqlContext.RegisterFunction<string, string, string, string, string>("Func4", (s1, s2, s3, s4) => "Func4");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func4", It.IsAny<byte[]>(), "string"));

            sqlContext.RegisterFunction<string, string, string, string, string, string>("Func5", (s1, s2, s3, s4, s5) => "Func5");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func5", It.IsAny<byte[]>(), "string"));

            sqlContext.RegisterFunction<string, string, string, string, string, string, string>("Func6", (s1, s2, s3, s4, s5, s6) => "Func6");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func6", It.IsAny<byte[]>(), "string"));

            sqlContext.RegisterFunction<string, string, string, string, string, string, string, string>("Func7", (s1, s2, s3, s4, s5, s6, s7) => "Func7");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func7", It.IsAny<byte[]>(), "string"));

            sqlContext.RegisterFunction<string, string, string, string, string, string, string, string, string>("Func8", (s1, s2, s3, s4, s5, s6, s7, s8) => "Func8");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func8", It.IsAny<byte[]>(), "string"));

            sqlContext.RegisterFunction<string, string, string, string, string, string, string, string, string, string>("Func9", (s1, s2, s3, s4, s5, s6, s7, s8, s9) => "Func9");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func9", It.IsAny<byte[]>(), "string"));

            sqlContext.RegisterFunction<string, string, string, string, string, string, string, string, string, string, string>("Func10", (s1, s2, s3, s4, s5, s6, s7, s8, s9, s10) => "Func10");
            mockSqlContextProxy.Verify(m => m.RegisterFunction("Func10", It.IsAny<byte[]>(), "string"));
        }
    }
}
