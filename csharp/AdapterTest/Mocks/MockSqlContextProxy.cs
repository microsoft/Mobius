// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;

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
            throw new NotImplementedException();
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
    }
}
