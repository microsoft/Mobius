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

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between SqlContext and its proxies
    /// </summary>
    [TestFixture]
    public class HiveContextTest
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
        public void TestHiveContextConstructor()
        {
            var mockSparkContextProxy = new Mock<ISparkContextProxy>();

            var mockSparkSessionProxy = new Mock<ISparkSessionProxy>();
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            mockCatalogProxy.Setup(m => m.RefreshTable(It.IsAny<string>()));
            mockSparkSessionProxy.Setup(m => m.GetCatalog()).Returns(mockCatalogProxy.Object);
            mockSparkContextProxy.Setup(m => m.CreateSparkSession()).Returns(mockSparkSessionProxy.Object);

            var mockSparkConfProxy = new Mock<ISparkConfProxy>();
            mockSparkConfProxy.Setup(m => m.GetSparkConfAsString())
                .Returns("spark.master=master;spark.app.name=appname;config1=value1;config2=value2;");

            var conf = new SparkConf(mockSparkConfProxy.Object);
            var hiveContext = new HiveContext(new SparkContext(mockSparkContextProxy.Object, conf));
            Assert.IsNotNull(hiveContext.SparkSession);
        }
        
        [Test]
        public void TestHiveContextRefreshTable()
        {
            // arrange
            var mockSparkContextProxy = new Mock<ISparkContextProxy>();
            var mockSparkSessionProxy = new Mock<ISparkSessionProxy>();
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            mockCatalogProxy.Setup(m => m.RefreshTable(It.IsAny<string>()));
            mockSparkSessionProxy.Setup(m => m.GetCatalog()).Returns(mockCatalogProxy.Object);
            mockSparkContextProxy.Setup(m => m.CreateSparkSession()).Returns(mockSparkSessionProxy.Object);

            var mockSparkConfProxy = new Mock<ISparkConfProxy>();
            mockSparkConfProxy.Setup(m => m.GetSparkConfAsString())
                .Returns("spark.master=master;spark.app.name=appname;config1=value1;config2=value2;");

            var conf = new SparkConf(mockSparkConfProxy.Object);
            var hiveContext = new HiveContext(new SparkContext(mockSparkContextProxy.Object, conf));
            hiveContext.SparkSession.SparkSessionProxy = mockSparkSessionProxy.Object;

            // act
            hiveContext.RefreshTable("table");

            // assert
            mockCatalogProxy.Verify(m => m.RefreshTable("table"));
        }
    }
}
