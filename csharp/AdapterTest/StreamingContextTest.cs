// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class StreamingContextTest
    {
        [Test]
        public void TestStreamingContext()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000L);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            ssc.Start();
            ssc.Remember(1000L);
            ssc.Checkpoint(Path.GetTempPath());

            var textFile = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(textFile.DStreamProxy);

            var socketStream = ssc.SocketTextStream(IPAddress.Loopback.ToString(), 12345);
            Assert.IsNotNull(socketStream.DStreamProxy);

            var kafkaStream = KafkaUtils.CreateStream(ssc, IPAddress.Loopback + ":2181", "testGroupId", new Dictionary<string, int> { { "testTopic1", 1 } }, new Dictionary<string, string>());
            Assert.IsNotNull(kafkaStream.DStreamProxy);

            var directKafkaStream = KafkaUtils.CreateDirectStream(ssc, new List<string> { "testTopic2" }, new Dictionary<string, string>(), new Dictionary<string, long>());
            Assert.IsNotNull(directKafkaStream.DStreamProxy);

            ssc.SparkContext.SparkConf.Set("spark.mobius.streaming.kafka.numPartitions.testTopic3", "10");

            var directKafkaStreamWithRepartition = KafkaUtils.CreateDirectStream(ssc, new List<string> { "testTopic3" }, new Dictionary<string, string>(), new Dictionary<string, long>());
            Assert.IsNotNull(directKafkaStreamWithRepartition.DStreamProxy);

            var directKafkaStreamWithRepartitionAndReadFunc = KafkaUtils.CreateDirectStream(
                ssc,
                new List<string> { "testTopic3" },
                new Dictionary<string, string>(), new Dictionary<string, long>(),
                (int pid, IEnumerable<KeyValuePair<byte[], byte[]>> input) => { return input; });
            Assert.IsNotNull(directKafkaStreamWithRepartitionAndReadFunc);

            ssc.SparkContext.SparkConf.Set("spark.mobius.streaming.kafka.numReceivers", "10");

            var directKafkaReceiver = KafkaUtils.CreateDirectStream(
                ssc,
                new List<string> { "testTopic3" },
                new Dictionary<string, string>(), new Dictionary<string, long>(),
                (int pid, IEnumerable<KeyValuePair<byte[], byte[]>> input) => { return input; });
            Assert.IsNotNull(directKafkaReceiver.DStreamProxy);

            var union = ssc.Union(textFile, socketStream);
            Assert.IsNotNull(union.DStreamProxy);

            ssc.AwaitTermination();
            ssc.Stop();
        }

        [Test]
        public void TestStreamingAwaitTimeout()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000L);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            ssc.Start();
            ssc.Remember(1000L);
            ssc.Checkpoint(Path.GetTempPath());

            var textFile = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(textFile.DStreamProxy);

            var socketStream = ssc.SocketTextStream(IPAddress.Loopback.ToString(), 12345);
            Assert.IsNotNull(socketStream.DStreamProxy);

            var union = ssc.Union(textFile, socketStream);
            Assert.IsNotNull(union.DStreamProxy);

            ssc.AwaitTerminationOrTimeout(3000);
            ssc.Stop();
        }

        [Test]
        public void TestStreamingOffsetRange()
        {
            byte[] partition = BitConverter.GetBytes(1);
            Array.Reverse(partition);
            byte[] fromOffset = BitConverter.GetBytes(2L);
            Array.Reverse(fromOffset);
            byte[] untilOffset = BitConverter.GetBytes(3L);
            Array.Reverse(untilOffset);

            var offsetRange = KafkaUtils.GetOffsetRange(new List<KeyValuePair<byte[], byte[]>>
                {
                    new KeyValuePair<byte[], byte[]>(Encoding.UTF8.GetBytes("testTopic,testClusterId"), partition),
                    new KeyValuePair<byte[], byte[]>(fromOffset, untilOffset)
                });

            Assert.AreEqual(offsetRange.Topic, "testTopic");
            Assert.AreEqual(offsetRange.ClusterId, "testClusterId");
            Assert.AreEqual(offsetRange.Partition, 1);
            Assert.AreEqual(offsetRange.FromOffset, 2);
            Assert.AreEqual(offsetRange.UntilOffset, 3);
        }
    }
}
