// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using NUnit.Framework;
using Microsoft.Spark.CSharp.Sql;
using System;
using System.Collections.Generic;
using Microsoft.CSharp.RuntimeBinder;
using Tests.Common;

namespace AdapterTest
{
    [TestFixture]
    public class RowTest
    {
        [Test]
        public void RowConstructTest()
        {
            Row row1 = new RowImpl(new object[]{1, "id", "name"}, RowHelper.BasicSchema);
            Row row2 = new RowImpl(new List<object> {1, "id", "name"}, RowHelper.BasicSchema);
            
            // dynamic data tyoe is not array or list
            Assert.Throws(typeof(Exception), () => { new RowImpl(new object(), RowHelper.BasicSchema); });

            // dynamic data count is not same with its schema
            Assert.Throws(typeof(Exception), () => { new RowImpl(new List<object> { "id", "name" }, RowHelper.BasicSchema); });

            // complex scheme
            Row row3 = new RowImpl(new object[]{ new object[]{"redmond", "washington"}, 1, "id", "name", new object[]{ "Tel3", "Tel4" } }, RowHelper.ComplexSchema);
        }

        [Test]
        public void RowGetTest()
        {
            Row row = new RowImpl(new object[] { 1, "1234", "John" }, RowHelper.BasicSchema);
            Assert.AreEqual(row.Get(0), 1);
            Assert.AreEqual(row.Get(1), "1234");
            Assert.AreEqual(row.Get(2), "John");
            Assert.Throws(typeof(Exception), () => { row.Get(3); });

            Assert.AreEqual(row.Get("age"), 1);
            Assert.AreEqual(row.Get("id"), "1234");
            Assert.AreEqual(row.Get("name"), "John");
            Assert.Throws(typeof(Exception), () => { row.Get("address"); });

            Assert.AreEqual(row.Size(), 3);

            Assert.AreEqual(row.GetAs<long>(0), 1);
            Assert.AreEqual(row.GetAs<string>(1), "1234");
            Assert.AreEqual(row.GetAs<string>(2), "John");
            Assert.Throws(typeof(RuntimeBinderException), () => { row.GetAs<int>(1); });

            Assert.AreEqual(row.GetAs<long>("age"), 1);
            Assert.AreEqual(row.GetAs<string>("id"), "1234");
            Assert.AreEqual(row.GetAs<string>("name"), "John");
            Assert.Throws(typeof(RuntimeBinderException), () => { row.GetAs<int>("id"); });
        }

        [Test]
        public void RowToStringTest()
        {
            Row row1 = new RowImpl(new object[] { 1, "1234", "John" }, RowHelper.BasicSchema);
            string result1 = row1.ToString();
            Assert.AreEqual(result1, "[1,1234,John]");

            Row row2 = new RowImpl(new object[] { 1, "1234", null }, RowHelper.BasicSchema);
            string result2 = row2.ToString();
            Assert.AreEqual(result2, "[1,1234,]");
        }
    }
}
