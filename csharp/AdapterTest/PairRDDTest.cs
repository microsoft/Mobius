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
        private static RDD<Tuple<string, int>> pairs;

        [OneTimeSetUp]
        public static void Initialize()
        {
            var sparkContext = new SparkContext(null);
            var lines = sparkContext.TextFile(Path.GetTempFileName());
            var words = lines.FlatMap(l => l.Split(' '));
            pairs = words.Map(w => new Tuple<string, int>(w, 1));
        }

        [Test]
        public void TestPairRddCountByKey()
        {
            foreach (var record in pairs.CountByKey())
            {
                // the 1st paramter of AreEqual() method is the expected value, the 2nd one is the acutal value.
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2);
            }
        }

        [Test]
        public void TestPairRddGroupWith()
        {
            foreach (var record in pairs.GroupWith(pairs).Collect())
            {
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item1.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item2.Count);
            }
            foreach (var record in pairs.GroupWith(pairs, pairs).Collect())
            {
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item1.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item2.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item3.Count);
            }
            foreach (var record in pairs.GroupWith(pairs, pairs, pairs).Collect())
            {
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item1.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item2.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item3.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item4.Count);
            }
        }

        /// <summary>
        /// Test RDD.GroupWith() method with different Tuple<K,V> types.
        /// </summary>
        [Test]
        public void TestPairRddGroupWith2()
        {
            var pairs1 = pairs.Map(p => new Tuple<string, double>(p.Item1, Convert.ToDouble(p.Item2)));
            var pairs2 = pairs.Map(p => new Tuple<string, string>(p.Item1, p.Item2.ToString()));
            var pairs3 = pairs.Map(p => new Tuple<string, long>(p.Item1, Convert.ToInt64(p.Item2)));

            foreach (var record in pairs.GroupWith(pairs1).Collect())
            {
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item1.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item2.Count);
            }

            foreach (var record in pairs.GroupWith(pairs1, pairs2).Collect())
            {
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item1.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item2.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item3.Count);
            }

            foreach (var record in pairs.GroupWith(pairs1, pairs2, pairs3).Collect())
            {
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item1.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item2.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item3.Count);
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Item4.Count);
            }
        }

        [Test]
        public void TestPairRddSubtractByKey()
        {
            var reduce = pairs.ReduceByKey((x, y) => x + y);
            var records = reduce.SubtractByKey(reduce.Filter(kvp => kvp.Item1 != "The")).Collect();
            Assert.AreEqual(1, records.Length);
            Assert.AreEqual("The", records[0].Item1);
            Assert.AreEqual(23, records[0].Item2);
        }

        [Test]
        public void TestPairRddReduceByKeyLocally()
        {
            foreach (var record in pairs.ReduceByKeyLocally((x, y) => x + y))
            {
                Assert.AreEqual(record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22, record.Value);
            }
        }

        [Serializable]
        private class IntWrapper
        {
            public IntWrapper(int value)
            {
                Value = value;
            }

            public int Value { get; }
        }

        [Test]
        public void TestPairRddReduceByKeyWithObjects()
        {
            // The ReduceByKey method below fails with NPE if ReduceByKey
            // calls CombineByKey with () => default(V) as seed generator
            var sums = pairs
                .MapValues(value => new IntWrapper(value))
                .ReduceByKey((x, y) => new IntWrapper(x.Value + y.Value));

            var result = sums
                .CollectAsMap()
                .Select(pair => new KeyValuePair<string, int>(pair.Key, pair.Value.Value))
                .ToList();

            var expectedResult = pairs
                .ReduceByKey((x, y) => x + y)
                .CollectAsMap()
                .ToList();

            Assert.That(result, Is.EquivalentTo(expectedResult));
        }

        [Test]
        public void TestPairRddFoldByKey()
        {
            foreach (var record in pairs.FoldByKey(() => 0, (x, y) => x + y).Collect())
            {
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2);
            }
        }

        [Test]
        public void TestPairRddAggregateByKey()
        {
            foreach (var record in pairs.AggregateByKey(() => 0, (x, y) => x + y, (x, y) => x + y).Collect())
            {
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2);
            }
        }

        [Test]
        public void TestPairRddGroupByKey()
        {
            foreach (var record in pairs.GroupByKey().Collect())
            {
                Assert.AreEqual(record.Item1 == "The" || record.Item1 == "dog" || record.Item1 == "lazy" ? 23 : 22, record.Item2.Count);
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
            var expectedSortedRdd = pairs.Collect().OrderBy(kv => kv.Item1, StringComparer.OrdinalIgnoreCase).ToArray();
            var rddSortByKey = pairs.SortByKey(true, null, key => key.ToLowerInvariant()).Collect();
            CollectionAssert.AreEqual(expectedSortedRdd, rddSortByKey);
        }

        [Test]
        public void TestPairRddSortByKey2()
        {
            var expectedSortedRdd = pairs.Collect().OrderBy(kv => kv.Item1, StringComparer.OrdinalIgnoreCase).ToArray();
            var rddSortByKey = pairs.SortByKey(true, 1, key => key.ToLowerInvariant()).Collect();
            CollectionAssert.AreEqual(expectedSortedRdd, rddSortByKey);
        }

        [Test]
        public void TestPairRddSortByKey3()
        {
            var expectedSortedRdd = pairs.Collect().OrderByDescending(kv => kv.Item1, StringComparer.OrdinalIgnoreCase).ToArray();
            var rddSortByKey = pairs.SortByKey(false, 1, key => key.ToLowerInvariant()).Collect();
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
