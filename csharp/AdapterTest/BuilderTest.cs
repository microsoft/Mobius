using System;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class BuilderTest
    {
        [Test]
        public void TestMaster()
        {
            var builder = new Builder();
            builder.Master("test");
            Assert.AreEqual("test", builder.options["spark.master"]);
        }

        [Test]
        public void TestAppName()
        {
            var builder = new Builder();
            builder.AppName("test");
            Assert.AreEqual("test", builder.options["spark.app.name"]);
        }

        [Test]
        public void TestBoolConfig()
        {
            var builder = new Builder();
            builder.Config("boolvalue", true);
            Assert.True(builder.options["boolvalue"].Equals("true", StringComparison.InvariantCultureIgnoreCase));
        }

        [Test]
        public void TestLongConfig()
        {
            var builder = new Builder();
            builder.Config("longvalue", 3L);
            Assert.True(builder.options["longvalue"].Equals("3", StringComparison.InvariantCultureIgnoreCase));
        }

        [Test]
        public void TestDoubleConfig()
        {
            var builder = new Builder();
            builder.Config("doublevalue", 3.5D);
            Assert.True(builder.options["doublevalue"].Equals("3.5", StringComparison.InvariantCultureIgnoreCase));
        }

        [Test]
        public void TestEnableHiveSupport()
        {
            var builder = new Builder();
            builder.EnableHiveSupport();
            Assert.True(builder.options["spark.sql.catalogImplementation"].Equals("hive", StringComparison.InvariantCultureIgnoreCase));
        } 
    }
}
