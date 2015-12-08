// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Text;
using System.Collections.Generic;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between SparkContext and its proxy
    /// </summary>
    [TestFixture]
    public class SparkContextTest
    {
        //TODO - complete impl

        [Test]
        public void TestSparkContextConstructor()
        {
            var sparkContext = new SparkContext("masterUrl", "appName");
            Assert.IsNotNull((sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference);
            var paramValuesToConstructor = (sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference as object[];
            Assert.AreEqual("masterUrl", (paramValuesToConstructor[0] as MockSparkConfProxy).stringConfDictionary["mockmaster"]);
            Assert.AreEqual("appName", (paramValuesToConstructor[0] as MockSparkConfProxy).stringConfDictionary["mockappName"]);

            sparkContext = new SparkContext("masterUrl", "appName", "sparkhome");
            Assert.IsNotNull((sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference);
            paramValuesToConstructor = (sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference as object[];
            Assert.AreEqual("masterUrl", (paramValuesToConstructor[0] as MockSparkConfProxy).stringConfDictionary["mockmaster"]);
            Assert.AreEqual("appName", (paramValuesToConstructor[0] as MockSparkConfProxy).stringConfDictionary["mockappName"]);
            Assert.AreEqual("sparkhome", (paramValuesToConstructor[0] as MockSparkConfProxy).stringConfDictionary["mockhome"]);

            sparkContext = new SparkContext(null);
            Assert.IsNotNull((sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference);
            paramValuesToConstructor = (sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference as object[];
            Assert.IsNotNull(paramValuesToConstructor[0]); //because SparkContext constructor create default sparkConf
        }

        [Test]
        public void TestSparkContextStop()
        {
            var sparkContext = new SparkContext(null);
            Assert.IsNotNull((sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference);
            sparkContext.Stop();
            Assert.IsNull((sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference);
        }

        [Test]
        public void TestSparkContextTextFile()
        {
            var sparkContext = new SparkContext(null);
            var rdd = sparkContext.TextFile(@"c:\path\to\rddinput.txt", 8);
            var paramValuesToTextFileMethod = (rdd.RddProxy as MockRddProxy).mockRddReference as object[];
            Assert.AreEqual(@"c:\path\to\rddinput.txt", paramValuesToTextFileMethod[0]);
            Assert.AreEqual(8, paramValuesToTextFileMethod[1]);
        }
    }
}
