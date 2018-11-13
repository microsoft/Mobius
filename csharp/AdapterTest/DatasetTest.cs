using System;
using System.Collections.Generic;
using System.Linq;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class DatasetTest
    {
        private static Mock<IDatasetProxy> mockDatasetProxy;

        [OneTimeSetUp]
        public static void ClassInitialize()
        {
            mockDatasetProxy = new Mock<IDatasetProxy>();
        }

        [SetUp]
        public void TestInitialize()
        {
            mockDatasetProxy.Reset();
        }

        [TearDown]
        public void TestCleanUp()
        {
            // Revert to use Static mock class to prevent blocking other test methods which uses static mock class
            SparkCLREnvironment.SparkCLRProxy = new MockSparkCLRProxy();
        }

        [Test]
        public void TestShow()
        {
            Mock<IDataFrameProxy> mockDataFrameProxy = new Mock<IDataFrameProxy>();
            mockDataFrameProxy.Setup(m => m.GetShowString(It.IsAny<int>(), It.IsAny<int>(), It.IsAny<bool>())).Returns("Show");
            mockDatasetProxy.Setup(m => m.ToDF()).Returns(mockDataFrameProxy.Object);

            var dataset = new Dataset(mockDatasetProxy.Object);
            dataset.Show();
            mockDataFrameProxy.Verify(m => m.GetShowString(20, 20, false), Times.Once);
        }

        [Test]
        public void TestExplain()
        {
            Mock<IDataFrameProxy> mockDataFrameProxy = new Mock<IDataFrameProxy>();
            mockDataFrameProxy.Setup(m => m.GetQueryExecution()).Returns("Execution Plan");
            mockDataFrameProxy.Setup(m => m.GetExecutedPlan()).Returns("Execution Plan");
            mockDatasetProxy.Setup(m => m.ToDF()).Returns(mockDataFrameProxy.Object);

            var dataset = new Dataset(mockDatasetProxy.Object);
            dataset.Explain();
            mockDataFrameProxy.Verify(m => m.GetQueryExecution(), Times.Once);

            dataset.Explain(true);
            mockDataFrameProxy.Verify(m => m.GetExecutedPlan(), Times.Once);
        }

        [Test]
        public void TestSchema()
        {
            TestSchema(true);
            TestSchema(false);
        }

        public void TestSchema(bool usePrintSchema)
        {
            var requestsSchema = new StructType(new List<StructField>
            {
                new StructField("test", new StringType(), false),
            });
            var jsonValue = requestsSchema.JsonValue.ToString();
            Mock<IStructTypeProxy> mockStructTypeProxy = new Mock<IStructTypeProxy>();
            mockStructTypeProxy.Setup(m => m.ToJson()).Returns(jsonValue);
            Mock<IDataFrameProxy> mockDataFrameProxy = new Mock<IDataFrameProxy>();
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockStructTypeProxy.Object);
            mockDatasetProxy.Setup(m => m.ToDF()).Returns(mockDataFrameProxy.Object);

            var dataset = new Dataset(mockDatasetProxy.Object);

            if (usePrintSchema)
                dataset.PrintSchema();
            else
                dataset.ShowSchema();

            mockDataFrameProxy.Verify(m => m.GetSchema(), Times.Once);
            mockStructTypeProxy.Verify(m => m.ToJson(), Times.Once());
        }

        [Test]
        public void TestColumns()
        {
            var requestsSchema = new StructType(new List<StructField>
            {
                new StructField("test", new StringType(), false),
            });
            var x = requestsSchema.JsonValue.ToString();
            Mock<IStructTypeProxy> mockStructTypeProxy = new Mock<IStructTypeProxy>();
            mockStructTypeProxy.Setup(m => m.ToJson()).Returns(x);
            Mock<IStructFieldProxy> mockStructFieldProxy = new Mock<IStructFieldProxy>();
            mockStructFieldProxy.Setup(m => m.GetStructFieldName()).Returns("testcol");
            mockStructTypeProxy.Setup(m => m.GetStructTypeFields())
                .Returns(new List<IStructFieldProxy>() {mockStructFieldProxy.Object});
            Mock<IDataFrameProxy> mockDataFrameProxy = new Mock<IDataFrameProxy>();
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockStructTypeProxy.Object);
            mockDatasetProxy.Setup(m => m.ToDF()).Returns(mockDataFrameProxy.Object);

            var dataset = new Dataset(mockDatasetProxy.Object);
            var columns = dataset.Columns();
            Assert.AreEqual(1, columns.Count());
            Assert.AreEqual("testcol", columns.First());
        }

        [Test]
        public void TestDTypes()
        {
            var requestsSchema = new StructType(new List<StructField>
            {
                new StructField("test", new StringType(), false),
            });
            var x = requestsSchema.JsonValue.ToString();
            Mock<IStructTypeProxy> mockStructTypeProxy = new Mock<IStructTypeProxy>();
            mockStructTypeProxy.Setup(m => m.ToJson()).Returns(x);
            Mock<IStructFieldProxy> mockStructFieldProxy = new Mock<IStructFieldProxy>();
            mockStructFieldProxy.Setup(m => m.GetStructFieldName()).Returns("testcol");
            Mock<IStructDataTypeProxy> mockStructDataTypeProxy = new Mock<IStructDataTypeProxy>();
            mockStructDataTypeProxy.Setup(m => m.GetDataTypeSimpleString()).Returns("ss");
            mockStructFieldProxy.Setup(m => m.GetStructFieldDataType()).Returns(mockStructDataTypeProxy.Object);
            mockStructTypeProxy.Setup(m => m.GetStructTypeFields())
                .Returns(new List<IStructFieldProxy>() { mockStructFieldProxy.Object });
            Mock<IDataFrameProxy> mockDataFrameProxy = new Mock<IDataFrameProxy>();
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockStructTypeProxy.Object);
            mockDatasetProxy.Setup(m => m.ToDF()).Returns(mockDataFrameProxy.Object);

            var dataset = new Dataset(mockDatasetProxy.Object);
            var dTypes = dataset.DTypes();
            Assert.AreEqual(1, dTypes.Count());
            var first = dTypes.First();
            Assert.AreEqual("testcol", first.Item1);
            Assert.AreEqual("ss", first.Item2);
        }

    }
}
