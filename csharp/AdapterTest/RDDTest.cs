// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Linq;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
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
            Assert.AreEqual(201, words.ToLocalIterator().Count());
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
            foreach (var record in words.ZipWithUniqueId().Collect())
            {
                Assert.AreEqual(index++, record.Value);
            }
        }

        [Test]
        public void TestRddTakeSample()
        {
            Assert.AreEqual(20, words.TakeSample(true, 20, 1).Length);
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
    }
}
