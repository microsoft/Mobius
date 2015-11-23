using System;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace AdapterTest
{
    [TestClass]
    public class ComparableRDDTest
    {
        private static RDD<string> words;

        [ClassInitialize()]
        public static void Initialize(TestContext context)
        {
            var sparkContext = new SparkContext(null);
            var lines = sparkContext.TextFile(Path.GetTempFileName());
            words = lines.FlatMap(l => l.Split(' '));
        }

        [TestMethod]
        public void TestComparableRddMax()
        {
            Assert.AreEqual("The", words.Max());
        }

        [TestMethod]
        public void TestComparableRddMin()
        {
            Assert.AreEqual("brown", words.Min());
        }

        [TestMethod]
        public void TestComparableRddTakeOrdered()
        {
            var taken = words.Distinct().TakeOrdered(2);
            Assert.AreEqual(2, taken.Length);
            Assert.AreEqual("brown", taken[0]);
            Assert.AreEqual("dog", taken[1]);
        }

        [TestMethod]
        public void TestComparableRddTop()
        {
            var taken = words.Distinct().Top(2);
            Assert.AreEqual(2, taken.Length);
            Assert.AreEqual("The", taken[0]);
            Assert.AreEqual("the", taken[1]);
        }
    }
}
