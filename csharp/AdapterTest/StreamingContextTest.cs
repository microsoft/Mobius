// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
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
            var ssc = new StreamingContext(new SparkContext("", ""), 1000);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            ssc.Start();
            ssc.Remember(1000);
            ssc.Checkpoint(Path.GetTempPath());

            var textFile = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(textFile.DStreamProxy);

            var socketStream = ssc.SocketTextStream(IPAddress.Loopback.ToString(), 12345);
            Assert.IsNotNull(socketStream.DStreamProxy);

            var kafkaStream = KafkaUtils.CreateStream(ssc, IPAddress.Loopback + ":2181", "testGroupId", new [] { Tuple.Create("testTopic1", 1) }, new List<Tuple<string, string>>());
            Assert.IsNotNull(kafkaStream.DStreamProxy);

            var directKafkaStream = KafkaUtils.CreateDirectStream(ssc, new List<string> { "testTopic2" }, new List<Tuple<string, string>>(), new List<Tuple<string, long>>());
            Assert.IsNotNull(directKafkaStream.DStreamProxy);

            var directKafkaStreamWithRepartition = KafkaUtils.CreateDirectStreamWithRepartition(ssc, new List<string> { "testTopic3" }, new List<Tuple<string, string>>(), new List<Tuple<string, long>>(), 10);
            Assert.IsNotNull(directKafkaStreamWithRepartition.DStreamProxy);

            var union = ssc.Union(textFile, socketStream);
            Assert.IsNotNull(union.DStreamProxy);

            ssc.AwaitTermination();
            ssc.Stop();
        }
    }
}
