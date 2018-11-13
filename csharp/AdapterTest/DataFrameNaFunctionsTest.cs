// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between DataFrameWriter and its proxies
    /// </summary>
    [TestFixture]
    public class DataFrameNaFunctionsTest
    {
        private static Mock<IDataFrameNaFunctionsProxy> mockDataFrameNaFunctionsProxy;

        private static Mock<IDataFrameProxy> mockDataFrameProxy;

        [OneTimeSetUp]
        public static void ClassInitialize()
        {
            mockDataFrameNaFunctionsProxy = new Mock<IDataFrameNaFunctionsProxy>();
            mockDataFrameProxy = new Mock<IDataFrameProxy>();
        }

        [SetUp]
        public void TestInitialize()
        {
            mockDataFrameNaFunctionsProxy.Reset();
            mockDataFrameProxy.Reset();
        }

        [Test]
        public void TestDropWithAny()
        {
            // arrange
            const string columnName = "column1";
            var mockSchemaProxy = new Mock<IStructTypeProxy>();
            var mockFieldProxy = new Mock<IStructFieldProxy>();
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockSchemaProxy.Object);
            mockSchemaProxy.Setup(m => m.GetStructTypeFields()).Returns(new List<IStructFieldProxy> { mockFieldProxy.Object });
            mockFieldProxy.Setup(m => m.GetStructFieldName()).Returns(columnName);

            var sparkContext = new SparkContext("", "");
            mockDataFrameNaFunctionsProxy.Setup(m => m.Drop(It.IsAny<int>(), It.IsAny<string[]>())).Returns(mockDataFrameProxy.Object);

            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sparkContext);
            var f = new DataFrameNaFunctions(mockDataFrameNaFunctionsProxy.Object, dataFrame, sparkContext);

            // act
            var cols = new[] { "col1", "col2" };
            var df1 = f.Drop("any", cols);
            var df2 = f.Drop();
            var df3 = f.Drop("any");

            // verify
            Assert.IsNotNull(df1);
            Assert.AreEqual(df1.DataFrameProxy, dataFrame.DataFrameProxy);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Drop(cols.Length, cols), Times.Once);

            Assert.IsNotNull(df2);
            Assert.AreEqual(df2.DataFrameProxy, dataFrame.DataFrameProxy);
            
            Assert.IsNotNull(df3);
            Assert.AreEqual(df3.DataFrameProxy, dataFrame.DataFrameProxy);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Drop(1, new[] { columnName }), Times.Exactly(2));
        }

        [Test]
        public void TestDropWithAll()
        {
            // arrange
            var sparkContext = new SparkContext("", "");
            mockDataFrameNaFunctionsProxy.Setup(m => m.Drop(It.IsAny<int>(), It.IsAny<string[]>())).Returns(mockDataFrameProxy.Object);

            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sparkContext);
            var f = new DataFrameNaFunctions(mockDataFrameNaFunctionsProxy.Object, dataFrame, sparkContext);

            // act
            var cols = new[] { "col1", "col2" };
            var df = f.Drop("all", cols);

            // verify
            Assert.IsNotNull(df);
            Assert.AreEqual(df.DataFrameProxy, dataFrame.DataFrameProxy);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Drop(1, cols), Times.Once);
        }

        [Test]
        public void TestDropWithCols()
        {
            // arrange
            var sparkContext = new SparkContext("", "");
            mockDataFrameNaFunctionsProxy.Setup(m => m.Drop(It.IsAny<int>(), It.IsAny<string[]>())).Returns(mockDataFrameProxy.Object);

            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sparkContext);
            var f = new DataFrameNaFunctions(mockDataFrameNaFunctionsProxy.Object, dataFrame, sparkContext);

            // act
            var cols = new[] { "col1", "col2" };
            var df = f.Drop(cols);

            // verify
            Assert.IsNotNull(df);
            Assert.AreEqual(df.DataFrameProxy, dataFrame.DataFrameProxy);
            Assert.AreNotSame(dataFrame, df);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Drop(cols.Length, cols), Times.Once);

            mockDataFrameNaFunctionsProxy.Reset();

            df = f.Drop(new string[] { });

            Assert.AreSame(dataFrame, df);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Drop(It.IsAny<int>(), It.IsAny<string[]>()), Times.Never);
        }

        [Test]
        public void TestDropWithMinNonNulls()
        {
            const string columnName = "column1";
            var mockSchemaProxy = new Mock<IStructTypeProxy>();
            var mockFieldProxy = new Mock<IStructFieldProxy>();
            mockDataFrameProxy.Setup(m => m.GetSchema()).Returns(mockSchemaProxy.Object);
            mockSchemaProxy.Setup(m => m.GetStructTypeFields()).Returns(new List<IStructFieldProxy> { mockFieldProxy.Object });
            mockFieldProxy.Setup(m => m.GetStructFieldName()).Returns(columnName);

            var sparkContext = new SparkContext("", "");
            mockDataFrameNaFunctionsProxy.Setup(m => m.Drop(It.IsAny<int>(), It.IsAny<string[]>())).Returns(mockDataFrameProxy.Object);

            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sparkContext);
            var f = new DataFrameNaFunctions(mockDataFrameNaFunctionsProxy.Object, dataFrame, sparkContext);

            var df = f.Drop(20);
            Assert.IsNotNull(df);
            Assert.AreEqual(df.DataFrameProxy, dataFrame.DataFrameProxy);
            Assert.AreNotSame(dataFrame, df);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Drop(20, new[] { columnName }), Times.Once);
        }

        [Test]
        public void TestFill()
        {
            // arrange
            var sparkContext = new SparkContext("", "");

            // test fill with double value
            mockDataFrameNaFunctionsProxy.Setup(m => m.Fill(It.IsAny<double>(), It.IsAny<string[]>())).Returns(mockDataFrameProxy.Object);
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sparkContext);
            var f = new DataFrameNaFunctions(mockDataFrameNaFunctionsProxy.Object, dataFrame, sparkContext);

            var cols = new[] { "col1", "col2" };
            const double doubleValue = 0.001;
            var df = f.Fill(doubleValue, cols);

            // verify
            Assert.IsNotNull(df);
            Assert.AreEqual(df.DataFrameProxy, dataFrame.DataFrameProxy);
            Assert.AreNotSame(dataFrame, df);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Fill(doubleValue, cols), Times.Once);

            // test fill with string value
            mockDataFrameNaFunctionsProxy.Reset();
            mockDataFrameNaFunctionsProxy.Setup(m => m.Fill(It.IsAny<string>(), It.IsAny<string[]>())).Returns(mockDataFrameProxy.Object);
            dataFrame = new DataFrame(mockDataFrameProxy.Object, sparkContext);
            f = new DataFrameNaFunctions(mockDataFrameNaFunctionsProxy.Object, dataFrame, sparkContext);
            const string strValue = "UNKNOWN";
            df = f.Fill(strValue, cols);

            Assert.IsNotNull(df);
            Assert.AreEqual(df.DataFrameProxy, dataFrame.DataFrameProxy);
            Assert.AreNotSame(dataFrame, df);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Fill(strValue, cols), Times.Once);

            // test fill with dictonary
            mockDataFrameNaFunctionsProxy.Reset();
            mockDataFrameNaFunctionsProxy.Setup(m => m.Fill(It.IsAny<Dictionary<string,object>>())).Returns(mockDataFrameProxy.Object);
            dataFrame = new DataFrame(mockDataFrameProxy.Object, sparkContext);
            f = new DataFrameNaFunctions(mockDataFrameNaFunctionsProxy.Object, dataFrame, sparkContext);
            var valueDict = new Dictionary<string, object>()
            {
                {"col1", -1},
                {"col2", "UNKNOWN"}
            };
            df = f.Fill(valueDict);

            // verify
            Assert.IsNotNull(df);
            Assert.AreEqual(df.DataFrameProxy, dataFrame.DataFrameProxy);
            Assert.AreNotSame(dataFrame, df);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Fill(valueDict), Times.Once);
        }

        [Test]
        public void TestReplace()
        {
            // arrange
            var sparkContext = new SparkContext("", "");
            mockDataFrameNaFunctionsProxy.Setup(m => m.Replace(It.IsAny<string>(), It.IsAny<Dictionary<string,string>>()))
                .Returns(mockDataFrameProxy.Object);

            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sparkContext);
            var f = new DataFrameNaFunctions(mockDataFrameNaFunctionsProxy.Object, dataFrame, sparkContext);

            // act
            const string col = "col";
            var replacement = new Dictionary<string, string>()
            {
                {"", "unknown"},
                {"?", "unknown"}
            };
            var df = f.Replace(col, replacement);

            // verify
            Assert.IsNotNull(df);
            Assert.AreEqual(df.DataFrameProxy, dataFrame.DataFrameProxy);
            Assert.AreNotSame(dataFrame, df);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Replace(col, replacement), Times.Once);


        }

        [Test]
        public void TestReplaceWithColumns()
        {
            // arrange
            var sparkContext = new SparkContext("", "");
            mockDataFrameNaFunctionsProxy.Setup(m => m.Replace(It.IsAny<string[]>(), It.IsAny<Dictionary<string, string>>()))
                .Returns(mockDataFrameProxy.Object);

            // act
            var replacement = new Dictionary<string, string>()
            {
                {"", "unknown"},
                {"?", "unknown"}
            };
            var dataFrame = new DataFrame(mockDataFrameProxy.Object, sparkContext);
            var f = new DataFrameNaFunctions(mockDataFrameNaFunctionsProxy.Object, dataFrame, sparkContext);
            var cols = new[] { "col1", "col2" };

            var df = f.Replace(cols, replacement);

            // verify
            Assert.IsNotNull(df);
            Assert.AreEqual(df.DataFrameProxy, dataFrame.DataFrameProxy);
            Assert.AreNotSame(dataFrame, df);
            mockDataFrameNaFunctionsProxy.Verify(m => m.Replace(cols, replacement), Times.Once);
        }
    }
}
