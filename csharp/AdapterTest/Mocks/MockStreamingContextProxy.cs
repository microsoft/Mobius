// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;

namespace AdapterTest.Mocks
{
    internal class MockStreamingContextProxy : IStreamingContextProxy
    {
        private IFormatter formatter = new BinaryFormatter();
        public void Start()
        {}

        public void Stop()
        {}

        public void Remember(long durationMs)
        {}

        public void Checkpoint(string directory)
        {}

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

        public IDStreamProxy DirectKafkaStreamWithRepartition(List<string> topics, Dictionary<string, string> kafkaParams, Dictionary<string, long> fromOffsets, int numPartitions)
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

        public IDStreamProxy CreateCSharpDStream(IDStreamProxy jdstream, byte[] func, string serializationMode)
        {
            Func<double, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>>)formatter.Deserialize(new MemoryStream(func));
            RDD<dynamic> rdd = f(DateTime.UtcNow.Ticks,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));
            return new MockDStreamProxy(rdd.RddProxy);
        }

        public IDStreamProxy CreateCSharpTransformed2DStream(IDStreamProxy jdstream, IDStreamProxy jother, byte[] func, string serializationMode, string serializationModeOther)
        {
            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)formatter.Deserialize(new MemoryStream(func));
            RDD<dynamic> rdd = f(DateTime.UtcNow.Ticks,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")),
                new RDD<dynamic>((jother as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));
            return new MockDStreamProxy(rdd.RddProxy);
        }

        public IDStreamProxy CreateCSharpReducedWindowedDStream(IDStreamProxy jdstream, byte[] func, byte[] invFunc, int windowSeconds, int slideSeconds, string serializationMode)
        {
            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)formatter.Deserialize(new MemoryStream(func));
            RDD<dynamic> rdd = f(DateTime.UtcNow.Ticks,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")),
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));
            return new MockDStreamProxy(rdd.RddProxy);
        }


        public IDStreamProxy CreateCSharpStateDStream(IDStreamProxy jdstream, byte[] func, string className, string serializationMode, string serializationMode2)
        {
            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)formatter.Deserialize(new MemoryStream(func));
            RDD<dynamic> rdd = f(DateTime.UtcNow.Ticks,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")),
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));
            return new MockDStreamProxy(rdd.RddProxy);
        }

        public IDStreamProxy CreateConstantInputDStream(IRDDProxy rddProxy)
        {
            return new MockDStreamProxy();
        }

        public IDStreamProxy EventHubsUnionStream(Dictionary<string, string> eventHubsParams, StorageLevelType storageLevelType)
        {
            throw new NotImplementedException();
        }

        public IDStreamProxy KafkaMetaStream(byte[] metaParams, uint numPartitions)
        {
            throw new NotImplementedException();
        }
    }
}
