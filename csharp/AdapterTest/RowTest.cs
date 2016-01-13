// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using NUnit.Framework;
using Microsoft.Spark.CSharp.Proxy;
using Moq;
using Microsoft.Spark.CSharp.Sql;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using Microsoft.CSharp.RuntimeBinder;

namespace AdapterTest
{
    [TestFixture]
    public class RowTest
    {
        private static StructType complexSchema;
        private const string complexJsonSchema = @"
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


        private static StructType basicSchema;
        private const string basicJsonSchema = @"
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

        [OneTimeSetUp]
        public static void Initialize()
        {
            basicSchema = new StructType(JObject.Parse(basicJsonSchema));
            complexSchema = new StructType(JObject.Parse(complexJsonSchema));
        }

        [Test]
        public void RowConstructTest()
        {
            Row row1 = new RowImpl(new object[]{1, "id", "name"}, basicSchema);
            Row row2 = new RowImpl(new List<object>(){1, "id", "name"}, basicSchema);
            
            // dynamic data tyoe is not array or list
            Assert.Throws(typeof(Exception), new TestDelegate(() => { new RowImpl(new object(), basicSchema); }));

            // dynamic data count is not same with its schema
            Assert.Throws(typeof(Exception), new TestDelegate(() => { new RowImpl(new List<object>() { "id", "name" }, basicSchema); }));

            // complex scheme
            Row row3 = new RowImpl(new object[]{ new object[]{"redmond", "washington"}, 1, "id", "name"}, complexSchema);
        }

        [Test]
        public void RowGetTest()
        {
            Row row = new RowImpl(new object[] { 1, "1234", "John" }, basicSchema);
            Assert.AreEqual(row.Get(0), 1);
            Assert.AreEqual(row.Get(1), "1234");
            Assert.AreEqual(row.Get(2), "John");
            Assert.Throws(typeof(Exception), new TestDelegate(() => { row.Get(3); }));

            Assert.AreEqual(row.Get("age"), 1);
            Assert.AreEqual(row.Get("id"), "1234");
            Assert.AreEqual(row.Get("name"), "John");
            Assert.Throws(typeof(Exception), new TestDelegate(() => { row.Get("address"); }));

            Assert.AreEqual(row.Size(), 3);

            Assert.AreEqual(row.GetAs<long>(0), 1);
            Assert.AreEqual(row.GetAs<string>(1), "1234");
            Assert.AreEqual(row.GetAs<string>(2), "John");
            Assert.Throws(typeof(RuntimeBinderException), new TestDelegate(() => { row.GetAs<int>(1); }));

            Assert.AreEqual(row.GetAs<long>("age"), 1);
            Assert.AreEqual(row.GetAs<string>("id"), "1234");
            Assert.AreEqual(row.GetAs<string>("name"), "John");
            Assert.Throws(typeof(RuntimeBinderException), new TestDelegate(() => { row.GetAs<int>("id"); }));
        }

        [Test]
        public void RowToStringTest()
        {
            Row row1 = new RowImpl(new object[] { 1, "1234", "John" }, basicSchema);
            string result1 = row1.ToString();
            Assert.AreEqual(result1, "[1,1234,John]");

            Row row2 = new RowImpl(new object[] { 1, "1234", null }, basicSchema);
            string result2 = row2.ToString();
            Assert.AreEqual(result2, "[1,1234,]");
        }
    }
}
