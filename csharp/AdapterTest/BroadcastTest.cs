// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using NUnit.Framework;
using Moq;

namespace AdapterTest
{
    [TestFixture]
    public class BroadcastTest
    {
        [Test]
        public void TestBroadcastInDriver()
        {
            // mock broadcastProxy
            var broadcastProxy = new Mock<IBroadcastProxy>();
            broadcastProxy.Setup(m => m.Unpersist(It.IsAny<bool>()));

            // mock sparkContextProxy
            var sparkContextProxy = new Mock<ISparkContextProxy>();
            long expectedBroacastId;
            sparkContextProxy.Setup(m => m.ReadBroadcastFromFile(It.IsAny<string>(), out expectedBroacastId)).Returns(broadcastProxy.Object);

            SparkContext sc = new SparkContext(sparkContextProxy.Object, new SparkConf());
            const int expectedValue = 1024;
            Broadcast<int> broadcastVar = new Broadcast<int>(sc, expectedValue);

            int value = broadcastVar.Value;
            Assert.AreEqual(expectedValue, value);

            // verify
            sparkContextProxy.Verify(m => m.ReadBroadcastFromFile(It.IsAny<string>(), out expectedBroacastId), Times.Once);

            // test unpersist
            var filePath = broadcastVar.path;
            Assert.IsNotNull(filePath);
            Assert.IsTrue(File.Exists(filePath));

            const bool blocking = true;
            broadcastVar.Unpersist(blocking);

            // verify
            broadcastProxy.Verify(m => m.Unpersist(blocking), Times.Once);
            Assert.IsFalse(File.Exists(filePath));
        }

        // create bytes for deserialization
        internal Broadcast<T> CreateBroadcastVarInWorker<T>(T expectedValue, out long bid, out string path)
        {
            // create broadcast variable for serialization

            // mock broadcastProxy
            var broadcastProxy = new Mock<IBroadcastProxy>();
            broadcastProxy.Setup(m => m.Unpersist(It.IsAny<bool>()));

            // mock sparkContextProxy
            var sparkContextProxy = new Mock<ISparkContextProxy>();
            long expectedBroacastId;
            sparkContextProxy.Setup(m => m.ReadBroadcastFromFile(It.IsAny<string>(), out expectedBroacastId)).Returns(broadcastProxy.Object);

            SparkContext sc = new SparkContext(sparkContextProxy.Object, new SparkConf());
            Broadcast<T> broadcastVar = new Broadcast<T>(sc, expectedValue);

            bid = broadcastVar.broadcastId;
            path = broadcastVar.path;

            var formatter = new BinaryFormatter();
            var ms = new MemoryStream();
            formatter.Serialize(ms, broadcastVar);

            Broadcast<T> broadcastVarInWorker = (dynamic)formatter.Deserialize(new MemoryStream(ms.ToArray()));

            return broadcastVarInWorker;
        }

        [Test]
        public void TestBroadcastInWorker()
        {
            const int expectedValue = 1024;
            long bid;
            string dumpPath;

            // worker side operations            
            Broadcast<int> broadcastVarInWorker = CreateBroadcastVarInWorker(expectedValue, out bid, out dumpPath);
            Broadcast.broadcastRegistry[bid] = new Broadcast(dumpPath);

            var broadcastValueInWorker = broadcastVarInWorker.Value;

            // assert
            Assert.IsNotNull(broadcastVarInWorker);
            Assert.AreEqual(expectedValue, broadcastValueInWorker);

            // test unpersist operation in worker
            Assert.Throws<ArgumentException>(() => broadcastVarInWorker.Unpersist());
        }

        [Test]
        public void TestOperationOnDestroyedBroadcastInWorker()
        {
            const int expectedValue = 2048;
            long bid;
            string dumpPath;

            // worker side operations            
            Broadcast<int> broadcastVarInWorker = CreateBroadcastVarInWorker(expectedValue, out bid, out dumpPath);
            Broadcast bc;
            Broadcast.broadcastRegistry.TryRemove(bid, out bc);

            // assert
            Assert.Throws<ArgumentException>(() => { var broadcastValueInWorker = broadcastVarInWorker.Value; });
        }
    }
}
