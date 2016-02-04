using NUnit.Framework;
using Microsoft.Spark.CSharp.Proxy;
using Moq;
using Microsoft.Spark.CSharp.Sql;

namespace AdapterTest
{
    [TestFixture]
    public class UserDefinedFunctionTest
    {
        private static Mock<IUDFProxy> mockUDFProxy;

        [OneTimeSetUp]
        public static void Initialize()
        {
            mockUDFProxy = new Mock<IUDFProxy>();
        }

        [Test]
        public void UserDefinedFunctionExecute0Test() 
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            udf.Execute0();
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[]{}), Times.Once);
        }

        [Test]
        public void UserDefinedFunctionExecute1Test()
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            Column column1 = GeneratorColum();
            udf.Execute1(column1);
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[] { column1.ColumnProxy }), Times.Once);
        }

        [Test]
        public void UserDefinedFunctionExecute2Test()
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            udf.Execute2(column1, column2);
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy }), Times.Once);
        }

        [Test]
        public void UserDefinedFunctionExecute3Test()
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Column column3 = GeneratorColum();
            udf.Execute3(column1, column2, column3);
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy }), Times.Once);
        }

        [Test]
        public void UserDefinedFunctionExecute4Test()
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Column column3 = GeneratorColum();
            Column column4 = GeneratorColum();
            udf.Execute4(column1, column2, column3, column4);
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy }), Times.Once);
        }

        [Test]
        public void UserDefinedFunctionExecute5Test()
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Column column3 = GeneratorColum();
            Column column4 = GeneratorColum();
            Column column5 = GeneratorColum();
            udf.Execute5(column1, column2, column3, column4, column5);
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, column5.ColumnProxy }), Times.Once);
        }

        [Test]
        public void UserDefinedFunctionExecute6Test()
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Column column3 = GeneratorColum();
            Column column4 = GeneratorColum();
            Column column5 = GeneratorColum();
            Column column6 = GeneratorColum();
            udf.Execute6(column1, column2, column3, column4, column5, column6);
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, 
                column5.ColumnProxy, column6.ColumnProxy}), Times.Once);
        }

        [Test]
        public void UserDefinedFunctionExecute7Test()
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Column column3 = GeneratorColum();
            Column column4 = GeneratorColum();
            Column column5 = GeneratorColum();
            Column column6 = GeneratorColum();
            Column column7 = GeneratorColum();
            udf.Execute7(column1, column2, column3, column4, column5, column6, column7);
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, 
                column5.ColumnProxy, column6.ColumnProxy,column7.ColumnProxy}), Times.Once);
        }

        [Test]
        public void UserDefinedFunctionExecute8Test()
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Column column3 = GeneratorColum();
            Column column4 = GeneratorColum();
            Column column5 = GeneratorColum();
            Column column6 = GeneratorColum();
            Column column7 = GeneratorColum();
            Column column8 = GeneratorColum();
            udf.Execute8(column1, column2, column3, column4, column5, column6, column7, column8);
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, 
                column5.ColumnProxy, column6.ColumnProxy,column7.ColumnProxy,column8.ColumnProxy}), Times.Once);
        }

        [Test]
        public void UserDefinedFunctionExecute9Test()
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Column column3 = GeneratorColum();
            Column column4 = GeneratorColum();
            Column column5 = GeneratorColum();
            Column column6 = GeneratorColum();
            Column column7 = GeneratorColum();
            Column column8 = GeneratorColum();
            Column column9 = GeneratorColum();
            udf.Execute9(column1, column2, column3, column4, column5, column6, column7, column8, column9);
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, 
                column5.ColumnProxy, column6.ColumnProxy,column7.ColumnProxy,column8.ColumnProxy,column9.ColumnProxy}), Times.Once);
        }

        [Test]
        public void UserDefinedFunctionExecute10Test()
        {
            mockUDFProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            UserDefinedFunction<int> udf = new UserDefinedFunction<int>(mockUDFProxy.Object);
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Column column3 = GeneratorColum();
            Column column4 = GeneratorColum();
            Column column5 = GeneratorColum();
            Column column6 = GeneratorColum();
            Column column7 = GeneratorColum();
            Column column8 = GeneratorColum();
            Column column9 = GeneratorColum();
            Column column10 = GeneratorColum();
            udf.Execute10(column1, column2, column3, column4, column5, column6, column7, column8, column9, column10);
            mockUDFProxy.Verify(m => m.Apply(new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, 
                column5.ColumnProxy, column6.ColumnProxy,column7.ColumnProxy,column8.ColumnProxy,column9.ColumnProxy,column10.ColumnProxy}), Times.Once);
        }

        private Column GeneratorColum()
        {
            Mock<IColumnProxy> mockColumnProxy = new Mock<IColumnProxy>();
            return new Column(mockColumnProxy.Object);
        }
    }
}
