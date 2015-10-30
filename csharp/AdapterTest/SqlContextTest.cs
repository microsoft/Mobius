// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between SqlContext and its proxies
    /// </summary>
    [TestClass]
    public class SqlContextTest
    {
        //TODO - complete impl

        [TestMethod]
        public void TestSqlContextConstructor()
        {
            var sqlContext = new SqlContext(new SparkContext("", ""));
            Assert.IsNotNull((sqlContext.SqlContextProxy as MockSqlContextProxy).mockSqlContextReference);
        }

        [TestMethod]
        public void TestJsonFile()
        {
            var sqlContext = new SqlContext(new SparkContext("", "")); 
            var dataFrame = sqlContext.JsonFile(@"c:\path\to\input.json");
            var paramValuesToJsonFileMethod = (dataFrame.DataFrameProxy as MockDataFrameProxy).mockDataFrameReference as object[];
            Assert.AreEqual(@"c:\path\to\input.json", paramValuesToJsonFileMethod[0]);
        }

        [TestMethod]
        public void TestTextFile()
        {
            var sqlContext = new SqlContext(new SparkContext("", ""));
            var dataFrame = sqlContext.TextFile(@"c:\path\to\input.txt");
            var paramValuesToTextFileMethod = (dataFrame.DataFrameProxy as MockDataFrameProxy).mockDataFrameReference as object[];
            Assert.AreEqual(@"c:\path\to\input.txt", paramValuesToTextFileMethod[0]);
            Assert.AreEqual(@",", paramValuesToTextFileMethod[1]);
            Assert.IsFalse(bool.Parse(paramValuesToTextFileMethod[2].ToString()));
            Assert.IsFalse(bool.Parse(paramValuesToTextFileMethod[3].ToString()));

            sqlContext = new SqlContext(new SparkContext("", "")); 
            dataFrame = sqlContext.TextFile(@"c:\path\to\input.txt", "|", true, true);
            paramValuesToTextFileMethod = (dataFrame.DataFrameProxy as MockDataFrameProxy).mockDataFrameReference as object[];
            Assert.AreEqual(@"c:\path\to\input.txt", paramValuesToTextFileMethod[0]);
            Assert.AreEqual(@"|", paramValuesToTextFileMethod[1]);
            Assert.IsTrue(bool.Parse(paramValuesToTextFileMethod[2].ToString()));
            Assert.IsTrue(bool.Parse(paramValuesToTextFileMethod[3].ToString()));
        }
    }
}
