// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Linq;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between RDD and its proxy
    /// </summary>
    [TestFixture]
    public class RDDTest
    {
        private static RDD<string> words;

        [OneTimeSetUp]
        public static void Initialize()
        {
            var sparkContext = new SparkContext(null);
            var lines = sparkContext.TextFile(Path.GetTempFileName());
            words = lines.FlatMap(l => l.Split(' '));
        }

        [Test]
        public void TestRddName()
        {
            const string name = "RDD-1";
            var rdd = words.SetName(name);
            Assert.AreEqual(name, rdd.Name);
        }

        [Test]
        public void TestRddCountByValue()
        {
            foreach (var record in words.CountByValue())
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value);
            }
        }

        [Test]
        public void TestRddDistinct()
        {
            Assert.AreEqual(9, words.Distinct().Collect().Length);
        }

        [Test]
        public void TestRddSubtract()
        {
            Assert.AreEqual(23, words.Subtract(words.Filter(w => w != "The")).Collect().Length);
        }

        [Test]
        public void TestRddIntersection()
        {
            Assert.AreEqual(1, words.Intersection(words.Filter(w => w == "The")).Collect().Length);
        }

        [Test]
        public void TestRddToLocalIterator()
        {
            // 201 - count of records in each partition of `words` RDD.
            // For this mocked RDD `words`, the count of local iterator will be equal to (# of records in each parition) * numPartitions
            Assert.AreEqual(201 * words.GetNumPartitions(), words.ToLocalIterator().Count());
        }

        [Test]
        public void TestRddTreeReduce()
        {
            Assert.AreEqual(201, words.Map(w => 1).TreeReduce((x, y) => x + y));
        }

        [Test]
        public void TestRddTreeAggregate()
        {
            Assert.AreEqual(201, words.Map(w => 1).TreeAggregate(0, (x, y) => x + y, (x, y) => x + y));
        }

        [Test]
        public void TestRddAggregate()
        {
            Assert.AreEqual(201, words.Map(w => 1).Aggregate(0, (x, y) => x + y, (x, y) => x + y));
        }

        [Test]
        public void TestRddReduce()
        {
            Assert.AreEqual(201, words.Map(w => 1).Reduce((x, y) => x + y));
        }

        [Test]
        public void TestRddFirst()
        {
            Assert.AreEqual("The", words.First());
        }

        [Test]
        public void TestRddFold()
        {
            Assert.AreEqual(201, words.Map(w => 1).Fold(0, (x, y) => x + y));
        }

        [Test]
        public void TestRddGlom()
        {
            Assert.AreEqual(201, words.Map(w => 1).Glom().Collect()[0].Length);
        }

        [Test]
        public void TestRddGroupBy()
        {
            words.GroupBy(w => w).Foreach(record =>
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Count);
            });
            
            words.GroupBy(w => w).ForeachPartition(iter =>
            {
                foreach (var record in iter)
                {
                    Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Count);
                }
            });
        }

        [Test]
        public void TestRddIsEmpty()
        {
            Assert.IsFalse(words.IsEmpty());
            Assert.IsTrue(words.Filter(w => w == null).IsEmpty());
        }

        [Test]
        public void TestRddZipWithIndex()
        {
            int index = 0;
            foreach(var record in words.ZipWithIndex().Collect())
            {
                Assert.AreEqual(index++, record.Value);
            }
        }

        [Test]
        public void TestRddZipWithUniqueId()
        {
            int index = 0;
            int num = words.GetNumPartitions();
            foreach (var record in words.ZipWithUniqueId().Collect())
            {
                Assert.AreEqual(num * index++, record.Value);
            }
        }

        [Test]
        public void TestRddTakeSample()
        {
            Assert.AreEqual(20, words.TakeSample(true, 20, 1).Length);
            Assert.AreEqual(20, words.TakeSample(true, 20, 1).Length);
            Assert.Throws<ArgumentException>(() => words.TakeSample(true, -1, 1));
            Assert.AreEqual(0, words.TakeSample(true, 0, 1).Length);
        }

        [Test]
        public void TestRddMap()
        {
            var sparkContext = new SparkContext(null);
            var rdd = sparkContext.TextFile(@"c:\path\to\rddinput.txt");
            var rdd2 = rdd.Map(s => s.ToLower() + ".com");
            Assert.IsTrue(rdd2.GetType() == typeof(PipelinedRDD<string>));
            var pipelinedRdd = rdd2 as PipelinedRDD<string>;
            var func = pipelinedRdd.workerFunc.Func;
            var result = func(1, new String[] { "ABC" });
            var output = result.First();
            Assert.AreEqual("ABC".ToLower() + ".com", output);

            var pipelinedRdd2 = rdd2.Map(s => "HTTP://" + s) as PipelinedRDD<string>;
            var func2 = pipelinedRdd2.workerFunc.Func;
            var result2 = func2(1, new String[] { "ABC" });
            var output2 = result2.First();
            Assert.AreEqual("HTTP://" + ("ABC".ToLower() + ".com"), output2); //tolower and ".com" appended first before adding prefix due to the way func2 wraps func in implementation
        }

        [Test]
        public void TestRddTextFile()
        {
            var sparkContext = new SparkContext(null);
            var rdd = sparkContext.TextFile(@"c:\path\to\rddinput.txt");
            var paramValuesToTextFileMethod = (rdd.RddProxy as MockRddProxy).mockRddReference as object[];
            Assert.AreEqual(@"c:\path\to\rddinput.txt", paramValuesToTextFileMethod[0]);
            Assert.AreEqual(0, int.Parse(paramValuesToTextFileMethod[1].ToString())); //checking default partitions
        }

        [Test]
        public void TestRddUnion()
        {
            var sparkContext = new SparkContext(null);
            var rdd = sparkContext.TextFile(@"c:\path\to\rddinput.txt"); 
            var rdd2 = sparkContext.TextFile(@"c:\path\to\rddinput2.txt");
            var unionRdd = rdd.Union(rdd2);
            var paramValuesToUnionMethod = ((unionRdd.RddProxy as MockRddProxy).mockRddReference as object[]);
            var paramValuesToTextFileMethodInRdd1 = (paramValuesToUnionMethod[0] as MockRddProxy).mockRddReference as object[];
            Assert.AreEqual(@"c:\path\to\rddinput.txt", paramValuesToTextFileMethodInRdd1[0]);
            var paramValuesToTextFileMethodInRdd2 = (paramValuesToUnionMethod[1] as MockRddProxy).mockRddReference as object[];
            Assert.AreEqual(@"c:\path\to\rddinput2.txt", paramValuesToTextFileMethodInRdd2[0]);
        }

        [Test]
        public void TestRddCache()
        {
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            rddProxy.Setup(m => m.Cache());
            var rdd = new RDD<string> {rddProxy = rddProxy.Object};

            Assert.IsFalse(rdd.IsCached);

            var cachedRdd = rdd.Cache();

            Assert.IsTrue(cachedRdd.IsCached);
        }

        [Test]
        public void TestRddPersistAndUnPersist()
        {
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            rddProxy.Setup(m => m.Persist(It.IsAny<StorageLevelType>()));

            var rdd = new RDD<string> {rddProxy = rddProxy.Object};
           
            Assert.IsFalse(rdd.IsCached);
            // test persist
            var persistedRdd = rdd.Persist(StorageLevelType.MEMORY_AND_DISK);

            Assert.IsNotNull(persistedRdd);
            Assert.IsTrue(persistedRdd.IsCached);

            // test unpersist
            rddProxy.Setup(m => m.Unpersist());
            var unPersistedRdd = persistedRdd.Unpersist();
            Assert.IsNotNull(unPersistedRdd);
            Assert.IsFalse(unPersistedRdd.IsCached);
        }

        [Test]
        public void TestCheckpoint()
        {
            Assert.IsFalse(words.IsCheckpointed);
            words.Checkpoint();
            Assert.IsTrue(words.IsCheckpointed);
        }

        [Test]
        public void TestRandomSplit()
        {
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            rddProxy.Setup(m => m.RandomSplit(It.IsAny<double[]>(), It.IsAny<long>())).Returns(new[] {new Mock<IRDDProxy>().Object, new Mock<IRDDProxy>().Object});

            var rdd = new RDD<string> { rddProxy = rddProxy.Object };

            var rdds = rdd.RandomSplit(new double[] {2, 3}, 17);
            Assert.IsNotNull(rdds);
            Assert.AreEqual(2, rdds.Length);
        }

        [Test]
        public void TestCartesian()
        {
            var rdd = words.Cartesian(words);
            Assert.IsNotNull(rdd);
            Assert.AreEqual(SerializedMode.Pair, rdd.serializedMode);
            Assert.IsNotNull(rdd.RddProxy);
        }

        [Test]
        public void TestPipe()
        {
            var rdd = words.Pipe("cat");
            Assert.IsNotNull(rdd);
            Assert.IsNotNull(rdd.RddProxy);
        }

        [Test]
        public void TestToDebugString()
        {
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            const string expectedDebugStr = "Debug String";
            rddProxy.Setup(m => m.ToDebugString()).Returns(expectedDebugStr);

            var rdd = new RDD<string> { rddProxy = rddProxy.Object };

            var debugStr = rdd.ToDebugString();
            Assert.IsNotNull(debugStr);
            Assert.AreEqual(expectedDebugStr, debugStr);
        }

        [Test]
        public void TestGetStorageLevel()
        {
            Assert.AreEqual(StorageLevel.storageLevel[StorageLevelType.MEMORY_ONLY], words.GetStorageLevel());
        }

        [Test]
        public void TestCoalesce()
        {
            const int numPartitions = 4;
            const bool shuffle = true;
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<IRDDProxy> coalescedRddProxy = new Mock<IRDDProxy>();
            rddProxy.Setup(m => m.Coalesce(It.IsAny<int>(), It.IsAny<bool>())).Returns(coalescedRddProxy.Object);

            var rdd = new RDD<string> { rddProxy = rddProxy.Object };

            var coalescedRdd = rdd.Coalesce(numPartitions, shuffle);
            Assert.IsNotNull(coalescedRdd);
            Assert.AreEqual(coalescedRddProxy.Object, coalescedRdd.RddProxy);
        }

        [Test]
        public void TestRandomSampleWithRange()
        {
            Mock<IRDDProxy> rddProxy = new Mock<IRDDProxy>();
            Mock<IRDDProxy> sampledRddProxy = new Mock<IRDDProxy>();
            rddProxy.Setup(m => m.RandomSampleWithRange(It.IsAny<double>(), It.IsAny<double>(), It.IsAny<long>())).Returns(sampledRddProxy.Object);

            var rdd = new RDD<string> { rddProxy = rddProxy.Object };

            var sampledRdd = rdd.RandomSampleWithRange(0.1, 0.8, new Random().Next());
            Assert.IsNotNull(sampledRdd);
            Assert.AreEqual(sampledRddProxy.Object, sampledRdd.RddProxy);
        }

        [Test]
        public void TestGetDefaultPartitionNum()
        {
            var sparkContext = new SparkContext(null);
            var lines = sparkContext.TextFile(Path.GetTempFileName(), 5);
            words = lines.FlatMap(l => l.Split(' '));

            var defaultNumPartitions = words.GetDefaultPartitionNum();
            Assert.AreEqual(2, defaultNumPartitions);
        }
    }
}
