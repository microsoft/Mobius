using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class PairRDDTest
    {
        private static RDD<KeyValuePair<string, int>> pairs;

        [OneTimeSetUp]
        public static void Initialize()
        {
            var sparkContext = new SparkContext(null);
            var lines = sparkContext.TextFile(Path.GetTempFileName());
            var words = lines.FlatMap(l => l.Split(' '));
            pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));
        }

        [Test]
        public void TestPairRddCountByKey()
        {
            foreach (var record in pairs.CountByKey())
            {
                // the 1st paramter of AreEqual() method is the expected value, the 2nd one is the acutal value.
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value);
            }
        }

        [Test]
        public void TestPairRddGroupWith()
        {
            foreach (var record in pairs.GroupWith(pairs).Collect())
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item1.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item2.Count);
            }
            foreach (var record in pairs.GroupWith(pairs, pairs).Collect())
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item1.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item2.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item3.Count);
            }
            foreach (var record in pairs.GroupWith(pairs, pairs, pairs).Collect())
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item1.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item2.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item3.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item4.Count);
            }
        }

        /// <summary>
        /// Test RDD.GroupWith() method with different KeyValuePair<K,V> types.
        /// </summary>
        [Test]
        public void TestPairRddGroupWith2()
        {
            var pairs1 = pairs.Map(p => new KeyValuePair<string, double>(p.Key, Convert.ToDouble(p.Value)));
            var pairs2 = pairs.Map(p => new KeyValuePair<string, string>(p.Key, p.Value.ToString()));
            var pairs3 = pairs.Map(p => new KeyValuePair<string, long>(p.Key, Convert.ToInt64(p.Value)));

            foreach (var record in pairs.GroupWith(pairs1).Collect())
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item1.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item2.Count);
            }

            foreach (var record in pairs.GroupWith(pairs1, pairs2).Collect())
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item1.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item2.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item3.Count);
            }

            foreach (var record in pairs.GroupWith(pairs1, pairs2, pairs3).Collect())
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item1.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item2.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item3.Count);
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Item4.Count);
            }
        }

        [Test]
        public void TestPairRddSubtractByKey()
        {
            var reduce = pairs.ReduceByKey((x, y) => x + y);
            var records = reduce.SubtractByKey(reduce.Filter(kvp => kvp.Key != "The")).Collect();
            Assert.AreEqual(1, records.Length);
            Assert.AreEqual("The", records[0].Key);
            Assert.AreEqual(23, records[0].Value);
        }

        [Test]
        public void TestPairRddReduceByKeyLocally()
        {
            foreach (var record in pairs.ReduceByKeyLocally((x, y) => x + y))
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value);
            }
        }

        [Test]
        public void TestPairRddFoldByKey()
        {
            foreach (var record in pairs.FoldByKey(() => 0, (x, y) => x + y).Collect())
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value);
            }
        }

        [Test]
        public void TestPairRddAggregateByKey()
        {
            foreach (var record in pairs.AggregateByKey(() => 0, (x, y) => x + y, (x, y) => x + y).Collect())
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value);
            }
        }

        [Test]
        public void TestPairRddGroupByKey()
        {
            foreach (var record in pairs.GroupByKey().Collect())
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value.Count);
            }
        }

        [Test]
        public void TestPairRddLookup()
        {
            var records = pairs.ReduceByKey((x, y) => x + y).Lookup("The");
            Assert.AreEqual(1, records.Length);
            Assert.AreEqual(23, records[0]);
        }

        [Test]
        public void TestPairRddKeys()
        {
            var records = pairs.ReduceByKey((x, y) => x + y).Keys().Collect();
            Assert.AreEqual(9, records.Length);
        }

        [Test]
        public void TestPairRddValues()
        {
            var records = pairs.ReduceByKey((x, y) => x + y).Values().Collect();
            Assert.AreEqual(9, records.Length);
        }

        [Test]
        public void TestPairRddPartitionBy()
        {
            Func<dynamic, int> partitionFunc = key => 1;
            var rddPartitionBy = pairs.PartitionBy(3, partitionFunc);
            Assert.AreEqual(new Partitioner(3, partitionFunc), rddPartitionBy.partitioner);
        }

        [Test]
        public void TestPairRddSortByKey()
        {
            var expectedSortedRdd = pairs.Collect().OrderBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase).ToArray();
            var rddSortByKey = pairs.SortByKey(true, null, key => key.ToLowerInvariant()).Collect();
            CollectionAssert.AreEqual(expectedSortedRdd, rddSortByKey);
        }

        [Test]
        public void TestPairRddSortByKey2()
        {
            var expectedSortedRdd = pairs.Collect().OrderBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase).ToArray();
            var rddSortByKey = pairs.SortByKey(true, 1, key => key.ToLowerInvariant()).Collect();
            CollectionAssert.AreEqual(expectedSortedRdd, rddSortByKey);
        }

        [Test]
        public void TestPairRddProxy()
        {
            pairs.SaveAsHadoopDataset(null);
            pairs.SaveAsHadoopFile(null, null, null, null, null, null);
            pairs.SaveAsNewAPIHadoopDataset(null);
            pairs.SaveAsNewAPIHadoopFile(null, null, null, null, null);
            pairs.SaveAsSequenceFile(null, null);
        }
    }
}
