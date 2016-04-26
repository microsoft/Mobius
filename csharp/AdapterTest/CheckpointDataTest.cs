// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Streaming;
using Microsoft.Spark.CSharp.Proxy;
using NUnit.Framework;
using Moq;

namespace AdapterTest
{
    [TestFixture]
    class CheckpointDataTest
    {
        [Test]
        public void TestWriteCheckpointData()
        {
            string directory = Path.GetTempPath();
            string checkpointPath = Path.Combine(directory, "checkpoint");
            Broadcast.broadcastVars.Clear();

            // mock broadcastProxy
            var broadcastProxy = new Mock<IBroadcastProxy>();
            broadcastProxy.Setup(m => m.Unpersist(It.IsAny<bool>()));

            // mock sparkContextProxy
            var sparkContextProxy = new Mock<ISparkContextProxy>();
            long expectedBroacastId;
            sparkContextProxy.Setup(m => m.ReadBroadcastFromFile(It.IsAny<string>(), out expectedBroacastId)).Returns(broadcastProxy.Object);

            SparkContext sc = new SparkContext(sparkContextProxy.Object, new SparkConf());
            const int expectedValue = 1024;
            var broadcastVar = sc.Broadcast(expectedValue);

            Func<StreamingContext> creatingFunc = () =>
            {

                StreamingContext context = new StreamingContext(sc, 2000);
                return context;
            };


            ISparkCLRProxy sparkCLRProxy = SparkCLREnvironment.SparkCLRProxy;
            try
            {
                var mockStreamingContextProxy = new Mock<IStreamingContextProxy>();
                mockStreamingContextProxy.Setup(m => m.SparkContext).Returns(sc);

                // mock the behavior of copyFromLocalToCheckpointDir and copyFromCheckpointDirToLocal
                string tempFile = null;
                mockStreamingContextProxy
                    .Setup(m => m.copyFromLocalToCheckpointDir(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                    .Callback((string localPath, string checkpointDir, string checkpointFileName) => tempFile = localPath);
                mockStreamingContextProxy
                    .Setup(m => m.copyFromCheckpointDirToLocal(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                    .Callback((string checkpointDir, string checkpointFileName, string localPath) => File.Copy(tempFile, localPath, true));

                var mockSparkCLRProxy = new Mock<ISparkCLRProxy>();
                mockSparkCLRProxy.Setup(m => m.CheckpointExists(It.IsAny<string>())).Returns(false);
                mockSparkCLRProxy.Setup(m => m.CreateStreamingContext(It.IsAny<SparkContext>(), It.IsAny<long>())).
                  Returns(mockStreamingContextProxy.Object);
                mockSparkCLRProxy.Setup(m => m.CreateStreamingContext(It.IsAny<string>())).
                    Returns(mockStreamingContextProxy.Object);

                // change SparkCLRProxy temporarily to the mocked instance
                SparkCLREnvironment.SparkCLRProxy = (ISparkCLRProxy)mockSparkCLRProxy.Object;

                // test the case to create a new streamingContext
                StreamingContext ssc = StreamingContext.GetOrCreate(checkpointPath, creatingFunc);
                Assert.IsTrue(ssc.streamingContextProxy == mockStreamingContextProxy.Object);

                // test the case to read streamingContext from checkpoint
                mockSparkCLRProxy.Setup(m => m.CheckpointExists(It.IsAny<string>())).Returns(true);
                StreamingContext ssc2 = StreamingContext.GetOrCreate(checkpointPath, creatingFunc);
                Assert.IsTrue(ssc2.streamingContextProxy == mockStreamingContextProxy.Object);
            }
            catch
            {
                throw; 
            }
            finally
            {
                // restore SparkCLRProxy to the orginal one
                SparkCLREnvironment.SparkCLRProxy = sparkCLRProxy;
            }
        }
    }
}
