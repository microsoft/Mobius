// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;
using Microsoft.Spark.CSharp.Sql;
using Razorvine.Pickle;
using Microsoft.Spark.CSharp.Proxy;

namespace AdapterTest.Mocks
{
    internal class MockDataFrameProxy : IDataFrameProxy
    {
        internal object[] mockDataFrameReference;
        private ISqlContextProxy mockSqlContextProxy;
        private List<object> mockRows;
        private int mockPort;
        private IStructTypeProxy mockSchema;

        public ISqlContextProxy SqlContextProxy
        {
            get { return mockSqlContextProxy; }
        }

        //just saving the parameter collection to mock the proxy reference that will be used in Assert statements
        internal MockDataFrameProxy(object[] parameterCollection, ISqlContextProxy scProxy)
        {
            mockDataFrameReference = parameterCollection;
            mockSqlContextProxy = scProxy;
        }

        // prepare data for mock CollectAndServe()
        // input data is List of Rows and each row is modeled as array of columns 
        internal MockDataFrameProxy(int port, List<object> rows, IStructTypeProxy schema)
        {
            mockPort = port;
            mockRows = rows;
            mockSchema = schema;
        }

        public void RegisterTempTable(string tableName)
        {
            throw new NotImplementedException();
        }

        public long Count()
        {
            throw new NotImplementedException();
        }

        public string GetQueryExecution()
        {
            throw new NotImplementedException();
        }

        public string GetExecutedPlan()
        {
            throw new NotImplementedException();
        }

        public string GetShowString(int numberOfRows, bool truncate)
        {
            throw new NotImplementedException();
        }

        bool IDataFrameProxy.IsLocal()
        {
            throw new NotImplementedException();
        }

        public IStructTypeProxy GetSchema()
        {
            return mockSchema;
        }

        public IRDDProxy ToJSON()
        {
            throw new NotImplementedException();
        }

        public IRDDProxy ToRDD()
        {
            throw new NotImplementedException();
        }

        public IColumnProxy GetColumn(string columnName)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Select(string columnName, string[] columnNames)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Select(IEnumerable<IColumnProxy> columns)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy SelectExpr(string[] columnExpressions)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Filter(string condition)
        {
            throw new NotImplementedException();
        }

        public IGroupedDataProxy GroupBy(string firstColumnName, string[] otherColumnNames)
        {
            throw new NotImplementedException();
        }

        public IGroupedDataProxy GroupBy()
        {
            throw new NotImplementedException();
        }

        public IGroupedDataProxy Rollup(string firstColumnName, string[] otherColumnNames)
        {
            throw new NotImplementedException();
        }

        public IGroupedDataProxy Cube(string firstColumnName, string[] otherColumnNames)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Agg(IGroupedDataProxy scalaGroupedDataReference, System.Collections.Generic.Dictionary<string, string> columnNameAggFunctionDictionary)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string joinColumnName)
        {
            return new MockDataFrameProxy(new object[] { otherScalaDataFrameReference, joinColumnName }, SqlContextProxy);
        }

        public IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string[] joinColumnNames)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, IColumnProxy scalaColumnReference, string joinType)
        {
            throw new NotImplementedException();
        }

        public bool IsLocal
        {
            get { throw new NotImplementedException(); }
        }

        public void Cache()
        {
            throw new NotImplementedException();
        }

        public void Persist(Microsoft.Spark.CSharp.Core.StorageLevelType storageLevelType)
        {
            throw new NotImplementedException();
        }

        public void Unpersist(bool blocking)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy JavaToCSharp()
        {
            Pickler pickler = new Pickler();
            return new MockRddProxy(mockRows.Select(r => pickler.dumps(new object[] { r })), true);
        }

        public IDataFrameProxy Replace<T>(object subset, Dictionary<T, T> toReplaceAndValueDict)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<IDataFrameProxy> RandomSplit(IEnumerable<double> weights, long? seed)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Sort(IColumnProxy[] columns)
        {
            throw new NotImplementedException();
        }

        public double Corr(string column1, string column2, string method)
        {
            throw new NotImplementedException();
        }

        public double Cov(string column1, string column2)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy FreqItems(IEnumerable<string> columns, double support)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Crosstab(string column1, string column2)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Limit(int num)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Coalesce(int numPartitions)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Repartition(int numPartitions)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Distinct()
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Sample(bool withReplacement, double fraction, long seed)
        {
            throw new NotImplementedException();
        }

        public IDataFrameWriterProxy Write()
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Alias(string alias)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Describe(string[] columns)
        {
            throw new NotImplementedException();
        }

        public IGroupedDataProxy Rollup(IColumnProxy[] columns)
        {
            throw new NotImplementedException();
        }

        public IGroupedDataProxy Cube(IColumnProxy[] columns)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy UnionAll(IDataFrameProxy other)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Intersect(IDataFrameProxy other)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Subtract(IDataFrameProxy other)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Drop(string columnName)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy DropNa(int? thresh, string[] subset)
        {
            throw new NotImplementedException();
        }

        public IDataFrameNaFunctionsProxy Na()
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy DropDuplicates()
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy DropNa(string how, int? thresh, string[] subset)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Replace<T>(T toReplace, T value, string[] subset)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy ReplaceAll<T>(IEnumerable<T> toReplace, IEnumerable<T> value, string[] subset)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy ReplaceAll<T>(IEnumerable<T> toReplace, T value, string[] subset)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy DropDuplicates(string[] subset)
        {
            throw new NotImplementedException();
        }

        public void Drop(IColumnProxy column)
        {
            throw new NotImplementedException();
        }
    }
}
