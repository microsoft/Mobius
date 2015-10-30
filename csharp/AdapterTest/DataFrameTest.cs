// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
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
        public void TestJoin()
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
    }


}
