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
            var hiveContext = new HiveContext(new SparkContext("", ""));
            Assert.IsNotNull((hiveContext.SqlContextProxy as MockSqlContextProxy).mockSqlContextReference);
        }

        [Test]
        public void TestHiveContextSql()
        {
            mockSqlContextProxy.Setup(m => m.Sql(It.IsAny<string>()));
            var hiveContext = new HiveContext(new SparkContext("", ""), mockSqlContextProxy.Object);
            hiveContext.Sql("SELECT * FROM ABC");
            mockSqlContextProxy.Verify(m => m.Sql("SELECT * FROM ABC"));
        }

        [Test]
        public void TestHiveContextRefreshTable()
        {
            // arrange
            mockSqlContextProxy.Setup(m => m.RefreshTable(It.IsAny<string>()));
            var hiveContext = new HiveContext(new SparkContext("", ""), mockSqlContextProxy.Object);

            // act
            hiveContext.RefreshTable("table");

            // assert
            mockSqlContextProxy.Verify(m => m.RefreshTable("table"));
        }
    }
}
