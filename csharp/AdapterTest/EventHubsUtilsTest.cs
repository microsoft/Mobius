// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Streaming;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class EventHubsUtilsTest
    {
        [Test]
        public void TestCreateUnionStream()
        {
            var streamingContextProxy = new Mock<IStreamingContextProxy>();
            var mockDstreamProxy = new Mock<IDStreamProxy>().Object;
            streamingContextProxy.Setup(
                                    m => m.EventHubsUnionStream(It.IsAny<Dictionary<string, string>>(), It.IsAny<StorageLevelType>()))
                                .Returns(mockDstreamProxy);

            var mockSparkClrProxy = new Mock<ISparkCLRProxy>();
            mockSparkClrProxy.Setup(m => m.CreateStreamingContext(It.IsAny<SparkContext>(), It.IsAny<long>()))
                .Returns(streamingContextProxy.Object);
            SparkCLREnvironment.SparkCLRProxy = mockSparkClrProxy.Object;

            var sparkContext = new SparkContext(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy, new SparkConf(new Mock<ISparkConfProxy>().Object));
            var streamingContext = new StreamingContext(sparkContext, 123);
            var dstream = EventHubsUtils.CreateUnionStream(streamingContext, new Dictionary<string, string>());
            Assert.AreEqual(mockDstreamProxy, dstream.DStreamProxy);
        }
    }
}
