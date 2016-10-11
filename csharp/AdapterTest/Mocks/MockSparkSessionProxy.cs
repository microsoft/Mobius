// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy;

namespace AdapterTest.Mocks
{
    class MockSparkSessionProxy : ISparkSessionProxy
    {
        public ISqlContextProxy SqlContextProxy { get { return new MockSqlContextProxy(new MockSparkContextProxy(new MockSparkConfProxy()));} }
        public IUdfRegistration Udf { get; }
        public ICatalogProxy GetCatalog()
        {
            throw new NotImplementedException();
        }

        public IDataFrameReaderProxy Read()
        {
            return new MockDataFrameReaderProxy(SqlContextProxy);
        }

        internal ISparkSessionProxy InjectedSparkSessionProxy { get; set; }
        public ISparkSessionProxy NewSession()
        {
            return InjectedSparkSessionProxy;
        }

        public IDataFrameProxy CreateDataFrame(IRDDProxy rddProxy, IStructTypeProxy structTypeProxy)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Table(string tableName)
        {
            return new MockDataFrameProxy(new object[] { tableName }, null);
        }

        public IDataFrameProxy Sql(string query)
        {
            return new MockDataFrameProxy(new object[] {query}, null);
        }

        public void Stop()
        {
            throw new NotImplementedException();
        }
    }
}
