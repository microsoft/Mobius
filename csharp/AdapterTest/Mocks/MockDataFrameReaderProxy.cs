// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Spark.CSharp.Proxy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdapterTest.Mocks
{
    internal class MockDataFrameReaderProxy : IDataFrameReaderProxy
    {
        private ISqlContextProxy mockSqlContextProxy;
        private string path;
        private string format;

        public MockDataFrameReaderProxy(ISqlContextProxy sqlContextProxy)
        {
            mockSqlContextProxy = sqlContextProxy;
        }

        public void Format(string source)
        {
            format = source;
        }

        public void Schema(Microsoft.Spark.CSharp.Sql.StructType schema)
        {
            throw new NotImplementedException();
        }

        public void Options(Dictionary<string, string> options)
        {
            if (options.ContainsKey("path"))
            {
                path = options["path"];
            }
        }

        public IDataFrameProxy Load()
        {
            return new MockDataFrameProxy(new object[] { path }, mockSqlContextProxy);
        }

        public IDataFrameProxy Jdbc(string url, string table, string[] predicates, Dictionary<string, string> connectionProperties)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Jdbc(string url, string table, Dictionary<string, string> properties)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Jdbc(string url, string table, string columnName, string lowerBound, string upperBound, int numPartitions, Dictionary<string, string> connectionProperties)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Parquet(string[] paths)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Table(string tableName)
        {
            throw new NotImplementedException();
        }
    }
}
