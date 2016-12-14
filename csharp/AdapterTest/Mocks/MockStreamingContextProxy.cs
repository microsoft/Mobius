// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;                                        
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;   
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

        public IDStreamProxy KafkaStream(IEnumerable<Tuple<string, int>> topics, IEnumerable<Tuple<string, string>> kafkaParams, Microsoft.Spark.CSharp.Core.StorageLevelType storageLevelType)
        {
            return new MockDStreamProxy();
        }

        public IDStreamProxy DirectKafkaStream(List<string> topics, IEnumerable<Tuple<string, string>> kafkaParams, IEnumerable<Tuple<string, long>> fromOffsets)
        {
            return new MockDStreamProxy();
        }

        public IDStreamProxy DirectKafkaStreamWithRepartition(List<string> topics, IEnumerable<Tuple<string, string>> kafkaParams, IEnumerable<Tuple<string, long>> fromOffsets,
            int numPartitions, byte[] readFunc, string serializationMode)
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

        public void AwaitTerminationOrTimeout(long timeout)
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
            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>) formatter.Deserialize(new MemoryStream(func));

            var ticks = DateTime.UtcNow.Ticks;
            RDD<dynamic> rdd = f(ticks,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")),
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));

            if (invFunc == null) return new MockDStreamProxy(rdd.RddProxy);

            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> invf = (Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>) formatter.Deserialize(new MemoryStream(invFunc));
            RDD<dynamic> invRdd = invf(ticks,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")),
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));
            var difference = rdd.Subtract(invRdd);

            return new MockDStreamProxy(difference.RddProxy);
        }

        public IDStreamProxy CreateCSharpStateDStream(IDStreamProxy jdstream, byte[] func, string className, string serializationMode, string serializationMode2)
        {
            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)formatter.Deserialize(new MemoryStream(func));
            RDD<dynamic> rdd = f(DateTime.UtcNow.Ticks,
                null,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));
            return new MockDStreamProxy(rdd.RddProxy);
        }

        public IDStreamProxy CreateConstantInputDStream(IRDDProxy rddProxy)
        {
            return new MockDStreamProxy();
        }
                    
        public IDStreamProxy CreateCSharpInputDStream(byte[] func, string serializationMode)
        {
            return new MockDStreamProxy();
        }

        public IDStreamProxy EventHubsUnionStream(IEnumerable<Tuple<string, string>> eventHubsParams, StorageLevelType storageLevelType)
        {
            throw new NotImplementedException();
        }

        public IDStreamProxy KafkaMetaStream(byte[] metaParams, uint numPartitions)
        {
            throw new NotImplementedException();
        }
    }
}
