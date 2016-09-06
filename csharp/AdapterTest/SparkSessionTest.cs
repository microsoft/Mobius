using System;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class SparkSessionTest
    {
        [Test]
        public void TestRead()
        {
            var mockSparkSessionProxy = new Mock<ISparkSessionProxy>();
            var sparkSession = new SparkSession(mockSparkSessionProxy.Object);
            var reader = sparkSession.Read();
            mockSparkSessionProxy.Verify(m => m.Read(), Times.Once);
        }

        [Test]
        public void TestStop()
        {
            var mockSparkSessionProxy = new Mock<ISparkSessionProxy>();
            var sparkSession = new SparkSession(mockSparkSessionProxy.Object);
            sparkSession.Stop();
            mockSparkSessionProxy.Verify(m => m.Stop(), Times.Once);
        }
    }
}
