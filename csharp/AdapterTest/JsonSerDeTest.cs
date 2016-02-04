// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Linq;
using System.Text;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Moq;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validate sort the properties in JObject when parse Json.
    /// </summary>
    [TestFixture]
    class JsonSerDeTest
    {
        [Test]
        public void TestSortProperties()
        {
            const string json = @"
                {
                  ""prop3"" : {
                    ""name"" : ""age""
                  },
                  ""prop2"" : ""value1"",
                  ""prop1"" : [],
                  ""prop7"" : [{
                    ""name"" : ""age"",
                    ""type"" : ""long""
                  }, []]
                }";

            JObject jo = JObject.Parse(json).SortProperties();
            Assert.IsNotNull(jo);
            Assert.AreEqual(new[] { "prop1", "prop2", "prop3", "prop7" }, jo.Properties().Select(p => p.Name).ToArray());
        }
    }
}
