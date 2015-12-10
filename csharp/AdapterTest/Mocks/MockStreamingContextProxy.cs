// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;

namespace AdapterTest.Mocks
{
    internal class MockStreamingContextProxy : IStreamingContextProxy
    {
        public void Start()
        {
        }

        public void Stop()
        {
        }

        public void Remember(long durationMs)
        {
        }

        public void Checkpoint(string directory)
        {
        }

        public IDStreamProxy TextFileStream(string directory)
        {
            return new MockDStreamProxy();
        }

        public IDStreamProxy SocketTextStream(string hostname, int port, Microsoft.Spark.CSharp.Core.StorageLevelType storageLevelType)
        {
            return new MockDStreamProxy();
        }

        public IDStreamProxy KafkaStream(Dictionary<string, int> topics, Dictionary<string, string> kafkaParams, Microsoft.Spark.CSharp.Core.StorageLevelType storageLevelType)
        {
            return new MockDStreamProxy();
        }

        public IDStreamProxy DirectKafkaStream(List<string> topics, Dictionary<string, string> kafkaParams, Dictionary<string, long> fromOffsets)
        {
            return new MockDStreamProxy();
        }

        public IDStreamProxy Union(IDStreamProxy firstDStreams, IDStreamProxy[] otherDStreams)
        {
            return new MockDStreamProxy();
        }

        public void AwaitTermination()
        {
        }

        public void AwaitTermination(int timeout)
        {
        }

        public SparkContext SparkContext
        {
            get { throw new NotImplementedException(); }
        }
    }
}
