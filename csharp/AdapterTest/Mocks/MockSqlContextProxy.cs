// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;
using Moq;

namespace AdapterTest.Mocks
{
    internal class MockSqlContextProxy : ISqlContextProxy
    {
        internal object mockSqlContextReference;
        private ISparkContextProxy mockSparkContextProxy;

        public ISparkContextProxy SparkContextProxy
        {
            get { return mockSparkContextProxy; }
        }

        public MockSqlContextProxy(ISparkContextProxy scProxy)
        {
            mockSqlContextReference = new object();
            mockSparkContextProxy = scProxy;
        }

        public IDataFrameReaderProxy Read()
        {
            return new MockDataFrameReaderProxy(this);
        }

        public IDataFrameProxy CreateDataFrame(IRDDProxy rddProxy, IStructTypeProxy structTypeProxy)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy ReadDataFrame(string path, StructType schema, System.Collections.Generic.Dictionary<string, string> options)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy JsonFile(string path)
        {
            return new MockDataFrameProxy(new object[] { path }, this);
        }

        public IDataFrameProxy TextFile(string path, StructType schema, string delimiter)
        {
            return new MockDataFrameProxy(new object[] { path, schema, delimiter }, this);
        }

        public IDataFrameProxy TextFile(string path, string delimiter, bool hasHeader, bool inferSchema)
        {
            return new MockDataFrameProxy(new object[] { path, delimiter, hasHeader, inferSchema }, this);
        }

        public IDataFrameProxy Sql(string query)
        {
            return new MockDataFrameProxy(new object[] { query }, this);
        }


        public void RegisterFunction(string name, byte[] command, string returnType)
        {
            throw new NotImplementedException();
        }

        public ISqlContextProxy NewSession()
        {
            throw new NotImplementedException();
        }

        public string GetConf(string key, string defaultValue)
        {
            throw new NotImplementedException();
        }

        public void SetConf(string key, string value)
        {
            throw new NotImplementedException();
        }

        public void RegisterDataFrameAsTable(IDataFrameProxy dataFrameProxy, string tableName)
        {
            throw new NotImplementedException();
        }

        public void DropTempTable(string tableName)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Table(string tableName)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Tables()
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Tables(string databaseName)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> TableNames()
        {
            throw new NotImplementedException();
        }

        public void CacheTable(string tableName)
        {
            throw new NotImplementedException();
        }

        public void UncacheTable(string tableName)
        {
            throw new NotImplementedException();
        }

        public void ClearCache()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> TableNames(string databaseName)
        {
            throw new NotImplementedException();
        }

        public bool IsCached(string tableName)
        {
            throw new NotImplementedException();
        }
    }
}
