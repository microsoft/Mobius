// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;
using StreamingContext = Microsoft.Spark.CSharp.Streaming.StreamingContext;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class TestWithMoqDemo
    {
        private static IEnumerable<dynamic> result;
        private static StreamingContext _streamingContext;
        private static Mock<ISparkContextProxy> _mockSparkContextProxy;
        private static Mock<ISparkCLRProxy> _mockSparkCLRProxy;
        private static Mock<IStreamingContextProxy> _mockStreamingContextProxy;
        private static Mock<IRDDProxy> _mockRddProxy;


        [SetUp]
        public void TestInitialize()
        {
            result = null;

            // Create Mock object to mock implementation of T by new Mock<T>();
            _mockSparkCLRProxy = new Mock<ISparkCLRProxy>();
            _mockSparkContextProxy = new Mock<ISparkContextProxy>();
            _mockStreamingContextProxy = new Mock<IStreamingContextProxy>();
            _mockRddProxy = new Mock<IRDDProxy>();

            SparkCLREnvironment.SparkCLRProxy = _mockSparkCLRProxy.Object;

            // Mock method of T by Mock<T>.Setup(). For method with parameters, you can mock different method implementation for different method parameters.
            // e.g., if you want to mock a method regardless of what values the method parameters are, you can use It.IsAny<T>() for each parameter; if you want 
            // to mock the method for certain criteria, use It.Is<T>(Func<T, bool>) can. You can mock the same method multiple times for different criteria of 
            // method parameters.

            // If the method to mock has return value and you want to mock the return value only, Use Returns(TReturnValue); if you want to add logics and return,
            // use Returns<T1, T2, ...>(Func<T1, T2, ..., TReturnValue>). If method is void, use CallBack<T1, T2, ...>(Action<T1, T2, ...>)
			
			// for more info please visit https://github.com/Moq/moq4/wiki/Quickstart
            _mockSparkCLRProxy.Setup(m => m.CreateSparkConf(It.IsAny<bool>())).Returns(new MockSparkConfProxy()); // some of mocks which rarely change can be kept

            _mockSparkCLRProxy.Setup(m => m.CreateSparkContext(It.IsAny<ISparkConfProxy>())).Returns(_mockSparkContextProxy.Object);
            _mockSparkCLRProxy.Setup(m => m.CreateStreamingContext(It.IsAny<SparkContext>(), It.IsAny<long>())).Returns(_mockStreamingContextProxy.Object);
            _mockRddProxy.Setup(m => m.CollectAndServe()).Returns(() =>
            {
                TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();

                Task.Run(() =>
                {
                    using (Socket socket = listener.AcceptSocket())
                    using (Stream ns = new NetworkStream(socket))
                    {
                        foreach (var item in result)
                        {
                            var ms = new MemoryStream();
                            new BinaryFormatter().Serialize(ms, item);
                            byte[] buffer = ms.ToArray();
                            SerDe.Write(ns, buffer.Length);
                            SerDe.Write(ns, buffer);
                        }
                    }
                });
                return (listener.LocalEndpoint as IPEndPoint).Port;
            });
            _mockRddProxy.Setup(m => m.RDDCollector).Returns(new RDDCollector());

            _mockSparkContextProxy.Setup(m => m.CreateCSharpRdd(It.IsAny<IRDDProxy>(), It.IsAny<byte[]>(), It.IsAny<Dictionary<string, string>>(),
                It.IsAny<List<string>>(), It.IsAny<bool>(), It.IsAny<List<Broadcast>>(), It.IsAny<List<byte[]>>()))
                .Returns<IRDDProxy, byte[], Dictionary<string, string>, List<string>, bool, List<Broadcast>, List<byte[]>>(
                (prefvJavaRddReference, command, environmentVariables, cSharpIncludes, preservePartitioning, broadcastVariables, accumulator) =>
                {
                    IEnumerable<dynamic> input = result ?? (new[] {
                    "The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog",
                    "The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog",
                    "The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog",
                    "The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog",
                    "The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog",
                    "The quick brown fox jumps over the lazy dog",
                    "The quick brown fox jumps over the lazy dog",
                    "The quick brown fox jumps over the lazy dog",
                    "The quick brown fox jumps over the lazy dog",
                    "The quick brown fox jumps over the lazy dog",
                    "The dog lazy"
                    }).AsEnumerable();

                    var formatter = new BinaryFormatter();
                    using (MemoryStream s = new MemoryStream(command))
                    {
                        int rddId = SerDe.ReadInt(s);
                        int stageId = SerDe.ReadInt(s);
                        int partitionId = SerDe.ReadInt(s);

                        SerDe.ReadString(s);
                        SerDe.ReadString(s);
                        CSharpWorkerFunc workerFunc = (CSharpWorkerFunc)formatter.Deserialize(new MemoryStream(SerDe.ReadBytes(s)));
                        var func = workerFunc.Func;
                        result = func(default(int), input);
                    }

                    if (result.FirstOrDefault() is byte[] && (result.First() as byte[]).Length == 8)
                    {
                        result = result.Where(e => (e as byte[]).Length != 8).Select(e => formatter.Deserialize(new MemoryStream(e as byte[])));
                    }

                    return _mockRddProxy.Object;
                });

            _streamingContext = new StreamingContext(new SparkContext("", ""), 1000);

        }

        [TearDown]
        public void TestCleanUp()
        {
            // Revert to use Static mock class to prevent blocking other test methods which uses static mock class
            SparkCLREnvironment.SparkCLRProxy = new MockSparkCLRProxy();            
        }

        [Test]
        public void TestDStreamTransform_Moq()
        {
            // Arrange
            var mockDStreamProxy = new Mock<IDStreamProxy>();
            _mockStreamingContextProxy.Setup(m => m.TextFileStream(It.Is<string>(d => d == Path.GetTempPath()))).Returns(mockDStreamProxy.Object);

            mockDStreamProxy.Setup(m => m.CallForeachRDD(It.IsAny<byte[]>(), It.IsAny<string>())).Callback<byte[], string>(
                (func, deserializer) =>
                {
                    Action<double, RDD<dynamic>> f = (Action<double, RDD<dynamic>>)new BinaryFormatter().Deserialize(new MemoryStream(func));
                    f(DateTime.UtcNow.Ticks, new RDD<dynamic>(_mockRddProxy.Object, new SparkContext("", "")));
                });


            IRDDProxy functionedRddProxy = null;

            mockDStreamProxy.Setup(m => m.AsJavaDStream()).Returns(mockDStreamProxy.Object);

            _mockSparkCLRProxy.Setup(m => m.StreamingContextProxy.CreateCSharpDStream(It.IsAny<IDStreamProxy>(), It.IsAny<byte[]>(), It.IsAny<string>()))
                .Returns<IDStreamProxy, byte[], string>((jdstream, func, deserializer) =>
                {
                    Func<double, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>>)new BinaryFormatter().Deserialize(new MemoryStream(func));
                    RDD<dynamic> rdd = f(DateTime.UtcNow.Ticks,
                        new RDD<dynamic>(functionedRddProxy ?? _mockRddProxy.Object, new SparkContext("", "")));
                    functionedRddProxy = rdd.RddProxy;
                    return mockDStreamProxy.Object;
                });

            // Act
            var lines = _streamingContext.TextFileStream(Path.GetTempPath());
            var words = lines.FlatMap(l => l.Split(' '));
            var pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));
            var wordCounts = pairs.ReduceByKey((x, y) => x + y);

            // Assert
            wordCounts.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    KeyValuePair<string, int> countByWord = (KeyValuePair<string, int>)record;
                    Assert.AreEqual(countByWord.Value, countByWord.Key == "The" || countByWord.Key == "dog" || countByWord.Key == "lazy" ? 23 : 22);
                }
            });
            // Use Verify to verify if a method to mock was invoked
            mockDStreamProxy.Verify(m => m.CallForeachRDD(It.IsAny<byte[]>(), It.IsAny<string>()));
        }

    }
}