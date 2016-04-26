// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Text;
using System.Collections.Generic;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between SparkContext and its proxy
    /// </summary>
    [TestFixture]
    public class SparkContextTest
    {
        //TODO - complete impl

        [Test]
        public void TestSparkContextConstructor()
        {
            var sparkContext = new SparkContext("masterUrl", "appName");
            Assert.IsNotNull((sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference);
            var paramValuesToConstructor = (sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference as object[];
            Assert.AreEqual("masterUrl", (paramValuesToConstructor[0] as MockSparkConfProxy).stringConfDictionary["mockmaster"]);
            Assert.AreEqual("appName", (paramValuesToConstructor[0] as MockSparkConfProxy).stringConfDictionary["mockappName"]);
            Assert.AreEqual(sparkContext, SparkContext.GetActiveSparkContext());

            sparkContext = new SparkContext("masterUrl", "appName", "sparkhome");
            Assert.IsNotNull((sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference);
            paramValuesToConstructor = (sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference as object[];
            Assert.AreEqual("masterUrl", (paramValuesToConstructor[0] as MockSparkConfProxy).stringConfDictionary["mockmaster"]);
            Assert.AreEqual("appName", (paramValuesToConstructor[0] as MockSparkConfProxy).stringConfDictionary["mockappName"]);
            Assert.AreEqual("sparkhome", (paramValuesToConstructor[0] as MockSparkConfProxy).stringConfDictionary["mockhome"]);
            Assert.AreEqual(sparkContext, SparkContext.GetActiveSparkContext());

            sparkContext = new SparkContext(null);
            Assert.IsNotNull((sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference);
            paramValuesToConstructor = (sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference as object[];
            Assert.IsNotNull(paramValuesToConstructor[0]); //because SparkContext constructor create default sparkConf
            Assert.AreEqual(sparkContext, SparkContext.GetActiveSparkContext());
        }

        [Test]
        public void TestSparkContextStop()
        {
            var sparkContext = new SparkContext(null);
            Assert.IsNotNull((sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference);
            sparkContext.Stop();
            Assert.IsNull((sparkContext.SparkContextProxy as MockSparkContextProxy).mockSparkContextReference);
        }

        [Test]
        public void TestSparkContextTextFile()
        {
            var sparkContext = new SparkContext(null);
            var rdd = sparkContext.TextFile(@"c:\path\to\rddinput.txt", 8);
            var paramValuesToTextFileMethod = (rdd.RddProxy as MockRddProxy).mockRddReference as object[];
            Assert.AreEqual(@"c:\path\to\rddinput.txt", paramValuesToTextFileMethod[0]);
            Assert.AreEqual(8, paramValuesToTextFileMethod[1]);
        }

        [Test]
        public void TestSparkContextVersionProperty()
        {
            const string expectedVersion = "1.5.2";
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.Version).Returns(expectedVersion);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            string version = sc.Version;

            // assert
            Assert.IsNotNull(version);
            Assert.AreEqual(expectedVersion, version);
        }

        [Test]
        public void TestSparkContextStartTimeProperty()
        {
            long expectedStartTime = DateTime.Now.Millisecond;
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.StartTime).Returns(expectedStartTime);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            long startTime = sc.StartTime;

            // assert
            Assert.AreEqual(expectedStartTime, startTime);
        }

        [Test]
        public void TestSparkContextDefaultMinPartitionsProperty()
        {
            const int expectedDefaultMinPartitions = 5;
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.DefaultMinPartitions).Returns(expectedDefaultMinPartitions);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            int defaultMinPartitions = sc.DefaultMinPartitions;

            // assert
            Assert.AreEqual(expectedDefaultMinPartitions, defaultMinPartitions);
        }

        [Test]
        public void TestSparkContextSparkUserProperty()
        {
            const string expectedUser = "SPARKCLR_USER";
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.SparkUser).Returns(expectedUser);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            string user = sc.SparkUser;

            // assert
            Assert.IsNotNull(user);
            Assert.AreEqual(expectedUser, user);
        }

        [Test]
        public void TestSparkContextStatusTrackerProperty()
        {
            Mock<IStatusTrackerProxy> statusTrackerProxy = new Mock<IStatusTrackerProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.StatusTracker).Returns(statusTrackerProxy.Object);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            var statusTracker = sc.StatusTracker;

            // assert
            Assert.IsNotNull(statusTracker);
        }

        [Test]
        public void TestCancelAllJobs()
        {
            // Arrange
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.CancelAllJobs());
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            sc.CancelAllJobs();

            // Assert
            sparkContextProxy.Verify(m => m.CancelAllJobs(), Times.Once);
        }

        [Test]
        public void TestCancelJobGroup()
        {
            // Arrange
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.CancelJobGroup(It.IsAny<string>()));
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            const string groupId = "group-0";
            sc.CancelJobGroup(groupId);

            // Assert
            sparkContextProxy.Verify(m => m.CancelJobGroup(groupId), Times.Once);
        }

        [Test]
        public void TestSetLogLevel()
        {
            // Arrange
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.SetLogLevel(It.IsAny<string>()));
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            const string logLevel = "INFO";
            sc.SetLogLevel(logLevel);

            // Assert
            sparkContextProxy.Verify(m => m.SetLogLevel(logLevel), Times.Once);
        }

        [Test]
        public void TestGetLocalProperty()
        {
            // Arrange
            const string key = "spark.local.dir";
            const string expectedValue = @"D:\tmp\";
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.GetLocalProperty(It.IsAny<string>())).Returns(expectedValue);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            var value = sc.GetLocalProperty(key);

            // Assert
            Assert.IsNotNull(value);
            Assert.AreEqual(expectedValue, value);
        }

        [Test]
        public void TestSetLocalProperty()
        {
            // Arrange
            const string key = "spark.local.dir";
            const string value = @"D:\tmp\";
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.SetLocalProperty(It.IsAny<string>(), It.IsAny<string>()));
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            sc.SetLocalProperty(key, value);

            // Assert
            sparkContextProxy.Verify(m => m.SetLocalProperty(key, value), Times.Once);
        }

        [Test]
        public void TestSetJobGroup()
        {
            // Arrange
            const string groupId = "group-1";
            const string description = "group 1 description";
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.SetJobGroup(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>()));
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            sc.SetJobGroup(groupId, description);

            // Assert
            sparkContextProxy.Verify(m => m.SetJobGroup(groupId, description, false), Times.Once);
        }

        [Test]
        public void TestSetCheckpointDir()
        {
            // Arrange
            const string directory = @"D:\tmp";
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.SetCheckpointDir(It.IsAny<string>()));
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            sc.SetCheckpointDir(directory);

            // Assert
            sparkContextProxy.Verify(m => m.SetCheckpointDir(directory), Times.Once);
        }

        [Test]
        public void TestAddFile()
        {
            // Arrange
            const string path = @"D:\tmp";
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.AddFile(It.IsAny<string>()));
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            sc.AddFile(path);

            // Assert
            sparkContextProxy.Verify(m => m.AddFile(path), Times.Once);
        }

        [Test]
        public void TestBroadcast()
        {
            // Arrange
            long broadcastId = 100L;
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.ReadBroadcastFromFile(It.IsAny<string>(), out broadcastId));
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);
            const string expectedValue = "broadcastvar1";

            // Act
            var broadcastVar = sc.Broadcast(expectedValue);

            // Assert
            Assert.IsNotNull(broadcastVar);
            Assert.AreEqual(expectedValue, broadcastVar.Value);
            Assert.AreEqual(broadcastId, broadcastVar.broadcastId);

            sparkContextProxy.Verify(m => m.ReadBroadcastFromFile(It.IsAny<string>(), out broadcastId), Times.Once);
        }

        [Test]
        public void TestParallelize()
        {
            // Arrange
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.Parallelize(It.IsAny<IEnumerable<byte[]>>(), It.IsAny<int>())).Returns(rddProxy.Object);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            var nums = new[] { 0, 2, 3, 4, 6 };
            RDD<int> rdd = sc.Parallelize(nums, -2);

            // Assert
            Assert.IsNotNull(rdd);
            Assert.AreEqual(rddProxy.Object, rdd.RddProxy);
            Assert.AreEqual(sc, rdd.sparkContext);
        }

        [Test]
        public void TestEmptyRDD()
        {
            // Arrange
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.EmptyRDD()).Returns(rddProxy.Object);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            RDD<int> rdd = sc.EmptyRDD<int>();

            // Assert
            Assert.IsNotNull(rdd);
            Assert.AreEqual(rddProxy.Object, rdd.RddProxy);
            Assert.AreEqual(sc, rdd.sparkContext);
            Assert.AreEqual(SerializedMode.None, rdd.serializedMode);
        }

        [Test]
        public void TestWholeTextFiles()
        {
            // Arrange
            const string filePath = @"d:\data";
            const int minPartitions = 10;
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.WholeTextFiles(filePath, minPartitions)).Returns(rddProxy.Object);
            sparkContextProxy.Setup(m => m.DefaultMinPartitions).Returns(minPartitions);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            RDD<KeyValuePair<byte[], byte[]>> rdd = sc.WholeTextFiles(filePath, null);

            // Assert
            Assert.IsNotNull(rdd);
            Assert.AreEqual(rddProxy.Object, rdd.RddProxy);
            Assert.AreEqual(sc, rdd.sparkContext);
            Assert.AreEqual(SerializedMode.Pair, rdd.serializedMode);
        }

        [Test]
        public void TestBinaryFiles()
        {
            // Arrange
            const string filePath = @"d:\data";
            const int minPartitions = 10;
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.BinaryFiles(filePath, minPartitions)).Returns(rddProxy.Object);
            sparkContextProxy.Setup(m => m.DefaultMinPartitions).Returns(minPartitions);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            RDD<KeyValuePair<byte[], byte[]>> rdd = sc.BinaryFiles(filePath, null);

            // Assert
            Assert.IsNotNull(rdd);
            Assert.AreEqual(rddProxy.Object, rdd.RddProxy);
            Assert.AreEqual(sc, rdd.sparkContext);
            Assert.AreEqual(SerializedMode.Pair, rdd.serializedMode);
        }

        [Test]
        public void TestSequenceFiles()
        {
            // Arrange
            const string filePath = @"hdfs://path/to/files";

            const int defaultParallelism = 10;
            const string keyClass = "java.lang.Long";
            const string valueClass = "java.lang.String";
            const string keyConverterClass = "xyz.KeyConveter";
            const string valueConverterClass = "xyz.valueConveter";

            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.SequenceFile(filePath, keyClass, valueClass, keyConverterClass, valueConverterClass, It.IsAny<int>(), It.IsAny<int>()))
                .Returns(rddProxy.Object);
            sparkContextProxy.Setup(m => m.DefaultParallelism).Returns(defaultParallelism);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            // Act
            RDD<byte[]> rdd = sc.SequenceFile(filePath, keyClass, valueClass, keyConverterClass, valueConverterClass, null);

            // Assert
            Assert.IsNotNull(rdd);
            Assert.AreEqual(rddProxy.Object, rdd.RddProxy);
            Assert.AreEqual(sc, rdd.sparkContext);
            Assert.AreEqual(SerializedMode.None, rdd.serializedMode);
        }

        [Test]
        public void TestNewAPIHadoopFile()
        {
            // Arrange
            const string filePath = @"hdfs://path/to/files";

            const string keyClass = "java.lang.Long";
            const string valueClass = "java.lang.String";
            const string keyConverterClass = "xyz.KeyConveter";
            const string valueConverterClass = "xyz.valueConveter";

            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.NewAPIHadoopFile(filePath, It.IsAny<string>(), keyClass, valueClass, keyConverterClass, valueConverterClass, It.IsAny<IEnumerable<KeyValuePair<string, string>>>(), It.IsAny<int>()))
                .Returns(rddProxy.Object);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            const string inputFormatClass = "org.apache.hadoop.mapreduce.lib.input.TextInputFormat";
            // Act
            RDD<byte[]> rdd = sc.NewAPIHadoopFile(filePath, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass);

            // Assert
            Assert.IsNotNull(rdd);
            Assert.AreEqual(rddProxy.Object, rdd.RddProxy);
            Assert.AreEqual(sc, rdd.sparkContext);
            Assert.AreEqual(SerializedMode.None, rdd.serializedMode);
        }

        [Test]
        public void TestHadoopFile()
        {
            // Arrange
            const string filePath = @"hdfs://path/to/files";

            const string keyClass = "java.lang.Long";
            const string valueClass = "java.lang.String";
            const string keyConverterClass = "xyz.KeyConveter";
            const string valueConverterClass = "xyz.valueConveter";

            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.HadoopFile(filePath, It.IsAny<string>(), keyClass, valueClass, keyConverterClass, valueConverterClass, It.IsAny<IEnumerable<KeyValuePair<string, string>>>(), It.IsAny<int>()))
                .Returns(rddProxy.Object);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            const string inputFormatClass = "org.apache.hadoop.mapreduce.lib.input.TextInputFormat";
            // Act
            RDD<byte[]> rdd = sc.HadoopFile(filePath, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass);

            // Assert
            Assert.IsNotNull(rdd);
            Assert.AreEqual(rddProxy.Object, rdd.RddProxy);
            Assert.AreEqual(sc, rdd.sparkContext);
            Assert.AreEqual(SerializedMode.None, rdd.serializedMode);
        }

        [Test]
        public void TestNewAPIHadoopRDD()
        {
            // Arrange
            const string keyClass = "java.lang.Long";
            const string valueClass = "java.lang.String";
            const string keyConverterClass = "xyz.KeyConveter";
            const string valueConverterClass = "xyz.valueConveter";

            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.NewAPIHadoopRDD(It.IsAny<string>(), keyClass, valueClass, keyConverterClass, valueConverterClass, It.IsAny<IEnumerable<KeyValuePair<string, string>>>(), It.IsAny<int>()))
                .Returns(rddProxy.Object);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            const string inputFormatClass = "org.apache.hadoop.mapreduce.lib.input.TextInputFormat";
            var conf = new KeyValuePair<string, string>[] { };
            // Act
            RDD<byte[]> rdd = sc.NewAPIHadoopRDD(inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, conf);

            // Assert
            Assert.IsNotNull(rdd);
            Assert.AreEqual(rddProxy.Object, rdd.RddProxy);
            Assert.AreEqual(sc, rdd.sparkContext);
            Assert.AreEqual(SerializedMode.None, rdd.serializedMode);
        }

        [Test]
        public void TestHadoopRDD()
        {
            // Arrange
            const string keyClass = "java.lang.Long";
            const string valueClass = "java.lang.String";
            const string keyConverterClass = "xyz.KeyConveter";
            const string valueConverterClass = "xyz.valueConveter";

            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.HadoopRDD(It.IsAny<string>(), keyClass, valueClass, keyConverterClass, valueConverterClass, It.IsAny<IEnumerable<KeyValuePair<string, string>>>(), It.IsAny<int>()))
                .Returns(rddProxy.Object);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            const string inputFormatClass = "org.apache.hadoop.mapreduce.lib.input.TextInputFormat";
            var conf = new KeyValuePair<string, string>[] { };
            // Act
            RDD<byte[]> rdd = sc.HadoopRDD(inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, conf);

            // Assert
            Assert.IsNotNull(rdd);
            Assert.AreEqual(rddProxy.Object, rdd.RddProxy);
            Assert.AreEqual(sc, rdd.sparkContext);
            Assert.AreEqual(SerializedMode.None, rdd.serializedMode);
        }

        public RDD<T> TestUnion<T>(IEnumerable<RDD<T>> rdds)
        {
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.EmptyRDD()).Returns(rddProxy.Object);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);
            RDD<T> result = sc.Union<T>(rdds);

            Assert.IsNotNull(result);
            sparkContextProxy.Verify(m => m.EmptyRDD(), Times.Once);
            return result;
        }

        [Test]
        public void TestUnionWhenRddsIsNull()
        {
            TestUnion<string>(null);
        }

        [Test]
        public void TestUnionWhenRddsIsEmpty()
        {
            TestUnion<string>(new RDD<string>[] { });
        }

        [Test]
        public void TestUnionWhenRddsHaveOnlyOneElement()
        {
            var rdds = new RDD<int>[] { new RDD<int>() };

            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            RDD<int> result = sc.Union<int>(rdds);

            Assert.IsNotNull(result);
            Assert.AreEqual(rdds[0], result);
        }

        [Test]
        public void TestUnion()
        {
            var rdds = new RDD<string>[]
            {
                new RDD<string>(new Mock<IRDDProxy>().Object, null, SerializedMode.String),
                new RDD<string>(new Mock<IRDDProxy>().Object, null, SerializedMode.String)
            };

            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<ISparkContextProxy> sparkContextProxy = new Mock<ISparkContextProxy>();
            sparkContextProxy.Setup(m => m.Union(It.IsAny<IEnumerable<IRDDProxy>>())).Returns(rddProxy.Object);
            SparkContext sc = new SparkContext(sparkContextProxy.Object, null);

            RDD<string> result = sc.Union<string>(rdds);

            Assert.IsNotNull(result);
            Assert.AreEqual(rdds[0].serializedMode, result.serializedMode);
            Assert.AreEqual(rddProxy.Object, result.RddProxy);
        }
    }
}
