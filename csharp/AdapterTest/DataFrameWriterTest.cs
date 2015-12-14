// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Linq.Expressions;
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
    public class DataFrameWriterTest
    {
        private static Mock<IDataFrameWriterProxy> mockDataFrameWriterProxy;

        [OneTimeSetUp]
        public static void ClassInitialize()
        {
            mockDataFrameWriterProxy = new Mock<IDataFrameWriterProxy>();
        }

        [SetUp]
        public void TestInitialize()
        {
            mockDataFrameWriterProxy.Reset();
        }

        [Test]
        public void TestMode()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.Mode(It.IsAny<string>()));
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);

            dataFrameWriter.Mode(SaveMode.Append);
            mockDataFrameWriterProxy.Verify(m => m.Mode(SaveMode.Append.ToString()));
            mockDataFrameWriterProxy.Reset();

            dataFrameWriter.Mode(SaveMode.Ignore);
            mockDataFrameWriterProxy.Verify(m => m.Mode(SaveMode.Ignore.ToString()));
            mockDataFrameWriterProxy.Reset();

            dataFrameWriter.Mode(SaveMode.Overwrite);
            mockDataFrameWriterProxy.Verify(m => m.Mode(SaveMode.Overwrite.ToString()));
            mockDataFrameWriterProxy.Reset();

            dataFrameWriter.Mode(SaveMode.ErrorIfExists);
            mockDataFrameWriterProxy.Verify(m => m.Mode(It.IsIn("error", "default")));
            mockDataFrameWriterProxy.Reset();
        }

        [Test]
        public void TestStringMode()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.Mode(It.IsAny<string>()));
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);

            foreach (var mode in new string[] { "append", "ignore", "overwrite", "error", "default" })
            {
                dataFrameWriter.Mode(mode);
                mockDataFrameWriterProxy.Verify(m => m.Mode(mode));
                mockDataFrameWriterProxy.Reset();
            }
        }

        [Test]
        public void TestFormat()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.Format(It.IsAny<string>()));
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);

            foreach (var format in new string[] { "parquet", "json" })
            {
                dataFrameWriter.Format(format);
                mockDataFrameWriterProxy.Verify(m => m.Format(format));
                mockDataFrameWriterProxy.Reset();
            }
        }

        [Test]
        public void TestOption()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.Options(It.IsAny<Dictionary<string, string>>()));
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);
            const string key = "path";
            const string value = "path_value";

            // Act
            dataFrameWriter.Option(key, value);

            // Assert
            mockDataFrameWriterProxy.Verify(m => m.Options(
                It.Is<Dictionary<string, string>>(dict => dict[key] == value && dict.Count == 1)), Times.Once);
        }

        [Test]
        public void TestOptions()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.Options(It.IsAny<Dictionary<string, string>>()));
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);
            const string key1 = "key1";
            const string value1 = "value1";
            const string key2 = "key2";
            const string value2 = "value2";

            var opts = new Dictionary<string, string>()
            {
                {key1, value1},
                {key2, value2}
            };

            // Act
            dataFrameWriter.Options(opts);

            // Assert
            mockDataFrameWriterProxy.Verify(m => m.Options(It.Is<Dictionary<string, string>>(
                dict =>
                    dict[key1] == value1
                    && dict[key2] == value2
                    && dict.Count == 2)
                ),
                Times.Once
           );
        }

        [Test]
        public void TestPartitionBy()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.PartitionBy(It.IsAny<string[]>()));
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);
            var colNames = new string[] { "col1", "col2", "col3" };
            
            // Act
            dataFrameWriter.PartitionBy(colNames);

            // Assert
            mockDataFrameWriterProxy.Verify(m => m.PartitionBy(colNames));
        }

        [Test]
        public void TestSaveWithPath()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.Save());
            mockDataFrameWriterProxy.Setup(m => m.Options(It.IsAny<Dictionary<string,string>>()));
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);
            const string path = "/path/to/save";

            // Act
            dataFrameWriter.Save(path);

            // Assert
            mockDataFrameWriterProxy.Verify(m => m.Save(), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.Options(
                It.Is<Dictionary<string, string>>(dict => dict["path"] == path && dict.Count == 1)), Times.Once);
        }

        [Test]
        public void TestSave()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.Save());
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);

            // Act
            dataFrameWriter.Save();

            // Assert
            mockDataFrameWriterProxy.Verify(m => m.Save(), Times.Once);
        }

        [Test]
        public void TestInsertInto()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.InsertInto(It.IsAny<string>()));
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);
            const string table = "table";

            // Act
            dataFrameWriter.InsertInto(table);

            // Assert
            mockDataFrameWriterProxy.Verify(m => m.InsertInto(table), Times.Once);
        }

        [Test]
        public void TestSaveAsTable()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.SaveAsTable(It.IsAny<string>()));
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);
            const string table = "table";

            // Act
            dataFrameWriter.SaveAsTable(table);

            // Assert
            mockDataFrameWriterProxy.Verify(m => m.SaveAsTable(table), Times.Once);
        }

        [Test]
        public void TestJson()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.Format(It.IsAny<string>()));
            mockDataFrameWriterProxy.Setup(m => m.Options(It.IsAny<Dictionary<string, string>>()));
            mockDataFrameWriterProxy.Setup(m => m.Save());
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);
            const string path = "/path/to/save";

            // Act
            dataFrameWriter.Json(path);

            // Assert
            mockDataFrameWriterProxy.Verify(m => m.Save(), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.Format("json"), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.Options(
                It.Is<Dictionary<string, string>>(dict => dict["path"] == path && dict.Count == 1)), Times.Once);
        }

        [Test]
        public void TestParquet()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.Format(It.IsAny<string>()));
            mockDataFrameWriterProxy.Setup(m => m.Options(It.IsAny<Dictionary<string, string>>()));
            mockDataFrameWriterProxy.Setup(m => m.Save());
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);
            const string path = "/path/to/save";

            // Act
            dataFrameWriter.Parquet(path);

            // Assert
            mockDataFrameWriterProxy.Verify(m => m.Save(), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.Format("parquet"), Times.Once);
            mockDataFrameWriterProxy.Verify(m => m.Options(
                It.Is<Dictionary<string, string>>(dict => dict["path"] == path && dict.Count == 1)), Times.Once);
        }

        [Test]
        public void TestJdbc()
        {
            // arrange
            mockDataFrameWriterProxy.Setup(m => m.Jdbc(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<Dictionary<string, string>>()));
            var dataFrameWriter = new DataFrameWriter(mockDataFrameWriterProxy.Object);
            const string url = "jdbc:subprotocol:subname";
            const string table = "table";
            var properties = new Dictionary<string, string>() { { "autocommit", "false" } };

            // Act
            dataFrameWriter.Jdbc(url, table, properties);

            // Assert
            mockDataFrameWriterProxy.Verify(m => m.Jdbc(url, table, properties), Times.Once);
        }
    }
}
