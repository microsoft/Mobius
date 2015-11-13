// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;

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
        private Task mockSocketServerTask;

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


        public int CollectAndServe()
        {
            // start a new task to mock the Socket server side
            mockSocketServerTask = Task.Run(() =>
                {
                    // listen to localPort, and create socket
                    TcpListener listener = new TcpListener(IPAddress.Any, mockPort);
                    listener.Start();
                    Socket socket = listener.AcceptSocket();
                    Stream ns = new NetworkStream(socket);

                    Pickler picker = new Pickler();
                    BinaryWriter bw = new BinaryWriter(ns);

                    // write picked data via socket
                    foreach (var row in mockRows)
                    {

                        byte[] pickledRowData = picker.dumps(new object[] { row });
                        int pickledRowLength = pickledRowData.Count();

                        // write the length in BigEndian format
                        byte[] lengthBuffer = new byte[4];
                        lengthBuffer[0] = (byte)(pickledRowLength >> 24);
                        lengthBuffer[1] = (byte)(pickledRowLength >> 16);
                        lengthBuffer[2] = (byte)(pickledRowLength >> 8);
                        lengthBuffer[3] = (byte)(pickledRowLength);

                        bw.Write(lengthBuffer);
                        bw.Write(pickledRowData);
                    }

                    // close the stream and socket
                    ns.Close();
                    socket.Close();
                }
            );

            // sleep some time to let the server side start first
            Thread.Sleep(100);

            return mockPort;
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

        public IDataFrameProxy Sample(bool withReplacement, double fraction, long? seed)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy[] RandomSplit(double[] weights, long? seed)
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

        public IDataFrameProxy DropDuplicates(string[] subset)
        {
            throw new NotImplementedException();
        }

        public void Drop(IColumnProxy column)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy Drop(string columnName)
        {
            throw new NotImplementedException();
        }

        public IDataFrameProxy DropNa(string how, int? thresh, string[] subset)
        {
            throw new NotImplementedException();
        }
    }
}
