using System;
using System.IO;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class ComparableRDDTest
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
        public void TestComparableRddMax()
        {
            Assert.AreEqual("The", words.Max());
        }

        [Test]
        public void TestComparableRddMin()
        {
            Assert.AreEqual("brown", words.Min());
        }

        [Test]
        public void TestComparableRddTakeOrdered()
        {
            var taken = words.Distinct().TakeOrdered(2);
            Assert.AreEqual(2, taken.Length);
            Assert.AreEqual("brown", taken[0]);
            Assert.AreEqual("dog", taken[1]);
        }

        [Test]
        public void TestComparableRddTop()
        {
            var taken = words.Distinct().Top(2);
            Assert.AreEqual(2, taken.Length);
            Assert.AreEqual("The", taken[0]);
            Assert.AreEqual("the", taken[1]);
        }
    }
}
