using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace AdapterTest
{
    [TestClass]
    public class PairRDDTest
    {
        private static RDD<KeyValuePair<string, int>> pairs;

        [ClassInitialize()]
        public static void Initialize(TestContext context)
        {
            var sparkContext = new SparkContext(null);
            var lines = sparkContext.TextFile(Path.GetTempFileName());
            var words = lines.FlatMap(l => l.Split(' '));
            pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));
        }

        [TestMethod]
        public void TestPairRddCountByKey()
        {
            foreach (var record in pairs.CountByKey())
            {
                Assert.AreEqual(record.Value, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
            }
        }

        [TestMethod]
        public void TestPairRddGroupWith()
        {
            foreach (var record in pairs.GroupWith(pairs).Collect())
            {
                Assert.AreEqual(record.Value.Item1.Count, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
                Assert.AreEqual(record.Value.Item2.Count, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
            }
            foreach (var record in pairs.GroupWith(pairs, pairs).Collect())
            {
                Assert.AreEqual(record.Value.Item1.Count, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
                Assert.AreEqual(record.Value.Item2.Count, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
                Assert.AreEqual(record.Value.Item3.Count, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
            }
            foreach (var record in pairs.GroupWith(pairs, pairs, pairs).Collect())
            {
                Assert.AreEqual(record.Value.Item1.Count, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
                Assert.AreEqual(record.Value.Item2.Count, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
                Assert.AreEqual(record.Value.Item3.Count, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
                Assert.AreEqual(record.Value.Item4.Count, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
            }
        }

        [TestMethod]
        public void TestPairRddSubtractByKey()
        {
            var reduce = pairs.ReduceByKey((x, y) => x + y);
            var records = reduce.SubtractByKey(reduce.Filter(kvp => kvp.Key != "The")).Collect();
            Assert.AreEqual(records.Length, 1);
            Assert.AreEqual(records[0].Key, "The");
            Assert.AreEqual(records[0].Value, 23);
        }

        [TestMethod]
        public void TestPairRddReduceByKeyLocally()
        {
            foreach (var record in pairs.ReduceByKeyLocally((x, y) => x + y))
            {
                Assert.AreEqual(record.Value, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
            }
        }

        [TestMethod]
        public void TestPairRddFoldByKey()
        {
            foreach (var record in pairs.FoldByKey(() => 0, (x, y) => x + y).Collect())
            {
                Assert.AreEqual(record.Value, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
            }
        }

        [TestMethod]
        public void TestPairRddAggregateByKey()
        {
            foreach (var record in pairs.AggregateByKey(() => 0, (x, y) => x + y, (x, y) => x + y).Collect())
            {
                Assert.AreEqual(record.Value, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
            }
        }

        [TestMethod]
        public void TestPairRddGroupByKey()
        {
            foreach (var record in pairs.GroupByKey().Collect())
            {
                Assert.AreEqual(record.Value.Count, record.Key == "The" || record.Key == "dog" || record.Key == "lazy" ? 23 : 22);
            }
        }

        [TestMethod]
        public void TestPairRddLookup()
        {
            var records = pairs.ReduceByKey((x, y) => x + y).Lookup("The");
            Assert.AreEqual(records.Length, 1);
            Assert.AreEqual(records[0], 23);
        }

        [TestMethod]
        public void TestPairRddKeys()
        {
            var records = pairs.ReduceByKey((x, y) => x + y).Keys().Collect();
            Assert.AreEqual(records.Length, 9);
        }

        [TestMethod]
        public void TestPairRddValues()
        {
            var records = pairs.ReduceByKey((x, y) => x + y).Values().Collect();
            Assert.AreEqual(records.Length, 9);
        }

        [TestMethod]
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
