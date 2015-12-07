using System;
using System.Collections.Generic;
using System.IO;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class DoubleRDDTest
    {
        private static RDD<double> doubles;

        [OneTimeSetUp]
        public static void Initialize()
        {
            var sparkContext = new SparkContext(null);
            var lines = sparkContext.TextFile(Path.GetTempFileName());
            var words = lines.FlatMap(l => l.Split(' '));
            doubles = words.Map(w => new KeyValuePair<string, int>(w, 1)).ReduceByKey((x, y) => x + y).Map(kv => (double)kv.Value);
        }

        [Test]
        public void TestDoubleRddSum()
        {
            Assert.AreEqual(201, doubles.Sum());
        }

        [Test]
        public void TestDoubleRddStats()
        {
            StatCounter stats = doubles.Stats();
            Assert.AreEqual(9, stats.Count);
            Assert.AreEqual(201, stats.Sum);
            Assert.AreEqual(23, stats.Max);
            Assert.AreEqual(22, stats.Min);
            Assert.AreEqual(22.333333333333332, stats.Mean);
            Assert.AreEqual(0.22222222222222213, stats.Variance);
            Assert.AreEqual(0.24999999999999989, stats.SampleVariance);
            Assert.AreEqual(0.47140452079103157, stats.Stdev);
            Assert.AreEqual(0.49999999999999989, stats.SampleStdev);

            Assert.AreEqual(doubles.Mean(), stats.Mean);
            Assert.AreEqual(doubles.Variance(), stats.Variance);
            Assert.AreEqual(doubles.SampleVariance(), stats.SampleVariance);
            Assert.AreEqual(doubles.Stdev(), stats.Stdev);
            Assert.AreEqual(doubles.SampleStdev(), stats.SampleStdev);
        }
    }
}
