using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Sql.Catalog;
using Moq;
using NUnit.Framework;
using NUnit.Framework.Internal;
using Column = Microsoft.Spark.CSharp.Sql.Catalog.Column;

namespace AdapterTest
{
    [TestFixture]
    public class CatalogTest
    {
        [Test]
        public void TestCurrentCatalog()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            mockCatalogProxy.Setup(m => m.CurrentDatabase).Returns("currentdb");

            var catalog = new Catalog(mockCatalogProxy.Object);
            Assert.AreEqual("currentdb", catalog.CurrentDatabase);
        }

        [Test]
        public void TestGetDatabasesList()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var mockDatasetProxy = new Mock<IDatasetProxy>();
            var mockDataFrameProxy = new Mock<IDataFrameProxy>();
            mockDatasetProxy.Setup(m => m.ToDF()).Returns(mockDataFrameProxy.Object);
            mockCatalogProxy.Setup(m => m.ListDatabases()).Returns(new Dataset<Database>(mockDatasetProxy.Object));

            var catalog = new Catalog(mockCatalogProxy.Object);
            var databases = catalog.ListDatabases();
            Assert.AreSame(mockDataFrameProxy.Object, databases.DataFrameProxy);
        }

        [Test]
        public void TestGetTablesList()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var mockDatasetProxy = new Mock<IDatasetProxy>();
            var mockDataFrameProxy = new Mock<IDataFrameProxy>();
            mockDatasetProxy.Setup(m => m.ToDF()).Returns(mockDataFrameProxy.Object);
            mockCatalogProxy.Setup(m => m.ListTables(It.IsAny<string>())).Returns(new Dataset<Table>(mockDatasetProxy.Object));

            var catalog = new Catalog(mockCatalogProxy.Object);
            var tables = catalog.ListTables();
            Assert.AreSame(mockDataFrameProxy.Object, tables.DataFrameProxy);
        }

        [Test]
        public void TestGetColumnsList()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var mockDatasetProxy = new Mock<IDatasetProxy>();
            var mockDataFrameProxy = new Mock<IDataFrameProxy>();
            mockDatasetProxy.Setup(m => m.ToDF()).Returns(mockDataFrameProxy.Object);
            mockCatalogProxy.Setup(m => m.ListColumns(It.IsAny<string>(), It.IsAny<string>())).Returns(new Dataset<Column>(mockDatasetProxy.Object));

            var catalog = new Catalog(mockCatalogProxy.Object);
            var columns = catalog.ListColumns("dbname");
            Assert.AreSame(mockDataFrameProxy.Object, columns.DataFrameProxy);
        }

        [Test]
        public void TestGetFunctionsList()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var mockDatasetProxy = new Mock<IDatasetProxy>();
            var mockDataFrameProxy = new Mock<IDataFrameProxy>();
            mockDatasetProxy.Setup(m => m.ToDF()).Returns(mockDataFrameProxy.Object);
            mockCatalogProxy.Setup(m => m.ListFunctions(It.IsAny<string>())).Returns(new Dataset<Function>(mockDatasetProxy.Object));

            var catalog = new Catalog(mockCatalogProxy.Object);
            var columns = catalog.ListFunctions("dbname");
            Assert.AreSame(mockDataFrameProxy.Object, columns.DataFrameProxy);
        }

        [Test]
        public void TestSetCurrentDatabase()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var catalog = new Catalog(mockCatalogProxy.Object);
            catalog.SetCurrentDatabase("dbname");
            mockCatalogProxy.Verify(m => m.SetCurrentDatabase("dbname"), Times.Once);
        }

        [Test]
        public void TestDropTempTable()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var catalog = new Catalog(mockCatalogProxy.Object);
            catalog.DropTempView("tablename");
            mockCatalogProxy.Verify(m => m.DropTempTable("tablename"), Times.Once);
        }

        [Test]
        public void TestIsCached()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var catalog = new Catalog(mockCatalogProxy.Object);
            mockCatalogProxy.Setup(m => m.IsCached(It.IsAny<string>())).Returns(false);
            var isCached = catalog.IsCached("tablename");
            mockCatalogProxy.Verify(m => m.IsCached(It.IsAny<string>()), Times.Once);
            Assert.False(isCached);
        }

        [Test]
        public void TestCacheTable()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var catalog = new Catalog(mockCatalogProxy.Object);
            catalog.CacheTable("tablename");
            mockCatalogProxy.Verify(m => m.CacheTable("tablename"), Times.Once);
        }

        [Test]
        public void TestUnCacheTable()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var catalog = new Catalog(mockCatalogProxy.Object);
            catalog.UnCacheTable("tablename");
            mockCatalogProxy.Verify(m => m.UnCacheTable("tablename"), Times.Once);
        }

        [Test]
        public void TestRefreshTable()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var catalog = new Catalog(mockCatalogProxy.Object);
            catalog.RefreshTable("tablename");
            mockCatalogProxy.Verify(m => m.RefreshTable("tablename"), Times.Once);
        }

        [Test]
        public void TestClearCache()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            var catalog = new Catalog(mockCatalogProxy.Object);
            catalog.ClearCache();
            mockCatalogProxy.Verify(m => m.ClearCache(), Times.Once);
        }

        [Test]
        public void TestCreateExternalTable()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            DataFrame dataFrame = null;
            mockCatalogProxy.Setup(m => m.CreateExternalTable(It.IsAny<string>(), It.IsAny<string>())).Returns(dataFrame);
            var catalog = new Catalog(mockCatalogProxy.Object);
            var df = catalog.CreateExternalTable("tableName", "path");
            mockCatalogProxy.Verify(m => m.CreateExternalTable("tableName", "path"), Times.Once);
        }

        [Test]
        public void TestCreateExternalTable2()
        {
            var mockCatalogProxy = new Mock<ICatalogProxy>();
            DataFrame dataFrame = null;
            mockCatalogProxy.Setup(m => m.CreateExternalTable(It.IsAny<string>(), It.IsAny<string>())).Returns(dataFrame);
            var catalog = new Catalog(mockCatalogProxy.Object);
            var df = catalog.CreateExternalTable("tableName", "path", "source");
            mockCatalogProxy.Verify(m => m.CreateExternalTable("tableName", "path", "source"), Times.Once);
        }

        [Test]
        public void TestDatabaseProperties()
        {
            var database = new Database {Description = "desc", Name = "name", LocationUri = "uri"};
            Assert.AreEqual("desc", database.Description);
            Assert.AreEqual("name", database.Name);
            Assert.AreEqual("uri", database.LocationUri);
        }

        [Test]
        public void TestTableProperties()
        {
            var table = new Table { Description = "desc", Name = "name", Database = "db", TableType = "type", IsTemporary = false};
            Assert.AreEqual("desc", table.Description);
            Assert.AreEqual("name", table.Name);
            Assert.AreEqual("db", table.Database);
            Assert.AreEqual("type", table.TableType);
            Assert.False(table.IsTemporary);
        }

        [Test]
        public void TestColumnProperties()
        {
            var column = new Column { Description = "desc", Name = "name", DataType = "dtype", IsNullable = true, IsPartition = false, IsBucket = true};
            Assert.AreEqual("desc", column.Description);
            Assert.AreEqual("name", column.Name);
            Assert.AreEqual("dtype", column.DataType);
            Assert.False(column.IsPartition);
            Assert.True(column.IsNullable);
            Assert.True(column.IsBucket);
        }

        [Test]
        public void TestFunctionProperties()
        {
            var function = new Function { Description = "desc", Name = "name", Database = "db", ClassName = "classname", IsTemporary = false };
            Assert.AreEqual("desc", function.Description);
            Assert.AreEqual("name", function.Name);
            Assert.AreEqual("db", function.Database);
            Assert.AreEqual("classname", function.ClassName);
            Assert.False(function.IsTemporary);
        }
    }
}
