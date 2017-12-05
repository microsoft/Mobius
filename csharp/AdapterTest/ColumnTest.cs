// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using NUnit.Framework;
using Microsoft.Spark.CSharp.Proxy;
using Moq;
using Microsoft.Spark.CSharp.Sql;

namespace AdapterTest
{
    [TestFixture]
    public class ColumnTest
    {
        private static Mock<IColumnProxy> mockColumnProxy;
        private static Mock<IColumnProxy> generatedMockColumnProxy;

        [OneTimeSetUp]
        public static void Initialize()
        {
            mockColumnProxy = new Mock<IColumnProxy>();
            generatedMockColumnProxy = new Mock<IColumnProxy>();
        }
            
        [Test]
        public void TestColumnNotOperator() 
        {
            mockColumnProxy.Setup(m => m.FuncOp(It.IsAny<string>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = !column1;
            Assert.AreNotEqual(column1.ColumnProxy, column2.ColumnProxy);
            mockColumnProxy.Verify(m => m.FuncOp("not"), Times.Once);
        }

        [Test]
        public void TestColumnNegateOperator()
        {
            mockColumnProxy.Setup(m => m.FuncOp(It.IsAny<string>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = -column1;
            Assert.AreNotEqual(column1.ColumnProxy, column2.ColumnProxy);
            mockColumnProxy.Verify(m => m.FuncOp("negate"), Times.Once);
        }

        [Test]
        public void TestColumnPlusOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 + column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("plus", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnMinusOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 - column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("minus", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnMultiplyOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 * column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("multiply", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnDivideOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 / column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("divide", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnModeOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 % column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("mod", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnEqualOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 == column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("equalTo", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnNotEqualOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 != column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("notEqual", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnNotLTOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 < column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("lt", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnNotLEQOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 <= column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("leq", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnNotGEQOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 >= column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("geq", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnNotGTOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 > column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("gt", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnOrOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 | column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("bitwiseOR", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnAndOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 & column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("bitwiseAND", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnXorOperator()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>())).Returns(generatedMockColumnProxy.Object);
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            Column column3 = column1 ^ column2;
            Assert.AreNotEqual(column1.ColumnProxy, column3.ColumnProxy);
            mockColumnProxy.Verify(m => m.BinOp("bitwiseXOR", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnGetHashCode()
        {
            var column1 = new Column(null);
            Assert.AreEqual(0, column1.GetHashCode());

            var column2 = new Column(mockColumnProxy.Object);
            Assert.AreNotEqual(0, column2.GetHashCode());
        }

        [Test]
        public void TestColumnEquals()
        {
            var column1 = new Column(mockColumnProxy.Object);
            var column2 = new Column(mockColumnProxy.Object);
            Assert.IsTrue(column1.Equals(column2));
        }

        [Test]
        public void TestColumnLike()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<string>()));
            Column column1 = new Column(mockColumnProxy.Object);
            column1.Like("AnyString");
            mockColumnProxy.Verify(m => m.BinOp("like", "AnyString"), Times.Once);
        }

        [Test]
        public void TestColumnRLike()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<string>()));
            Column column1 = new Column(mockColumnProxy.Object);
            column1.RLike("AnyString");
            mockColumnProxy.Verify(m => m.BinOp("rlike", "AnyString"), Times.Once);
        }

        [Test]
        public void TestColumnStartsWith()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            column1.StartsWith(column2);
            mockColumnProxy.Verify(m => m.BinOp("startsWith", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnEndsWith()
        {
            mockColumnProxy.Setup(m => m.BinOp(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column1 = new Column(mockColumnProxy.Object);
            Column column2 = new Column(mockColumnProxy.Object);
            column1.EndsWith(column2);
            mockColumnProxy.Verify(m => m.BinOp("endsWith", column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestColumnAsc()
        {
            mockColumnProxy.Setup(m => m.UnaryOp(It.IsAny<string>()));
            Column column1 = new Column(mockColumnProxy.Object);
            column1.Asc();
            mockColumnProxy.Verify(m => m.UnaryOp("asc"), Times.Once);
        }

        [Test]
        public void TestColumnDesc()
        {
            mockColumnProxy.Setup(m => m.UnaryOp(It.IsAny<string>()));
            Column column1 = new Column(mockColumnProxy.Object);
            column1.Desc();
            mockColumnProxy.Verify(m => m.UnaryOp("desc"), Times.Once);
        }

        [Test]
        public void TestColumnAlias()
        {
            mockColumnProxy.Setup(m => m.InvokeMethod(It.IsAny<string>(), It.IsAny<string>()));
            Column column1 = new Column(mockColumnProxy.Object);
            column1.Alias("AnyAlias");
            mockColumnProxy.Verify(m => m.InvokeMethod("as", "AnyAlias"), Times.Once);

            mockColumnProxy.Setup(m => m.InvokeMethod(It.IsAny<string>(), It.IsAny<object[]>()));
            string[] array = new string[] { "AnyAlias1", "AnyAlias2" };
            column1.Alias(array);
            mockColumnProxy.Verify(m => m.InvokeMethod("as", new object[]{array}), Times.Once);
        }

        [Test]
        public void TestColumnCast()
        {
            Column column1 = new Column(mockColumnProxy.Object);
            mockColumnProxy.Setup(m => m.InvokeMethod(It.IsAny<string>(), It.IsAny<object[]>()));
            column1.Cast("int");
            mockColumnProxy.Verify(m => m.InvokeMethod("cast", new object[] { "int" }), Times.Once);
        }
    }
}
