// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between RDD and its proxy
    /// </summary>
    [TestClass]
    public class RDDTest
    {
        //TODO - complete impl

        [TestMethod]
        public void TestRddMap()
        {
            var sparkContext = new SparkContext(null);
            var rdd = sparkContext.TextFile(@"c:\path\to\rddinput.txt");
            var rdd2 = rdd.Map(s => s.ToLower() + ".com");
            Assert.IsTrue(rdd2.GetType() == typeof(PipelinedRDD<string>));
            var pipelinedRdd = rdd2 as PipelinedRDD<string>;
            var func = pipelinedRdd.func;
            var result = func(1, new String[] { "ABC" });
            var output = result.First();
            Assert.AreEqual("ABC".ToLower() + ".com", output);

            var pipelinedRdd2 = rdd2.Map(s => "HTTP://" + s) as PipelinedRDD<string>;
            var func2 = pipelinedRdd2.func;
            var result2 = func2(1, new String[] { "ABC" });
            var output2 = result2.First();
            Assert.AreEqual("HTTP://" + ("ABC".ToLower() + ".com"), output2); //tolower and ".com" appended first before adding prefix due to the way func2 wraps func in implementation
        }

        [TestMethod]
        public void TestRddTextFile()
        {
            var sparkContext = new SparkContext(null);
            var rdd = sparkContext.TextFile(@"c:\path\to\rddinput.txt");
            var paramValuesToTextFileMethod = (rdd.RddProxy as MockRddProxy).mockRddReference as object[];
            Assert.AreEqual(@"c:\path\to\rddinput.txt", paramValuesToTextFileMethod[0]);
            Assert.AreEqual(0, int.Parse(paramValuesToTextFileMethod[1].ToString())); //checking default partitions
        }

        [TestMethod]
        public void TestRddUnion()
        {
            var sparkContext = new SparkContext(null);
            var rdd = sparkContext.TextFile(@"c:\path\to\rddinput.txt"); 
            var rdd2 = sparkContext.TextFile(@"c:\path\to\rddinput2.txt");
            var unionRdd = rdd.Union(rdd2);
            var paramValuesToUnionMethod = ((unionRdd.RddProxy as MockRddProxy).mockRddReference as object[]);
            var paramValuesToTextFileMethodInRdd1 = (paramValuesToUnionMethod[0] as MockRddProxy).mockRddReference as object[];
            Assert.AreEqual(@"c:\path\to\rddinput.txt", paramValuesToTextFileMethodInRdd1[0]);
            var paramValuesToTextFileMethodInRdd2 = (paramValuesToUnionMethod[1] as MockRddProxy).mockRddReference as object[];
            Assert.AreEqual(@"c:\path\to\rddinput2.txt", paramValuesToTextFileMethodInRdd2[0]);
        }
    }
}
