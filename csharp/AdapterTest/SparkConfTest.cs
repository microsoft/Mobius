// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between SparkConf and its proxy
    /// </summary>
    [TestFixture]
    public class SparkConfTest
    {
        //TODO - complete impl

        [Test]
        public void TestSparkConfMethods()
        {
            var sparkConf = new SparkConf();
            sparkConf.SetMaster("masterUrl");
            Assert.AreEqual("masterUrl", sparkConf.Get(MockSparkConfProxy.MockMasterKey, ""));

            sparkConf.SetAppName("app name ");
            Assert.AreEqual("app name ", sparkConf.Get(MockSparkConfProxy.MockAppNameKey, ""));

            sparkConf.SetSparkHome(@"c:\path\to\sparkfolder");
            Assert.AreEqual(@"c:\path\to\sparkfolder", sparkConf.Get(MockSparkConfProxy.MockHomeKey, ""));

            Assert.AreEqual("default value", sparkConf.Get("non existent key", "default value"));
            Assert.AreEqual(3, sparkConf.GetInt("non existent key", 3));
        }
    }
}
