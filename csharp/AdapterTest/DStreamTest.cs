// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;

namespace AdapterTest
{
    [TestClass]
    public class DStreamTest
    {
        [TestMethod]
        public void TestDStreamTransform()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            var lines = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(lines.DStreamProxy);

            var words = lines.FlatMap(l => l.Split(' '));

            var pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));

            var wordCounts = pairs.ReduceByKey((x, y) => x + y);

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
        }
        
        [TestMethod]
        public void TestDStreamJoin()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            var lines = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(lines.DStreamProxy);

            var words = lines.FlatMap(l => l.Split(' '));

            var pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));

            var wordCounts = pairs.ReduceByKey((x, y) => x + y);

            var left = wordCounts.Filter(x => x.Key != "quick" && x.Key != "lazy");
            var right = wordCounts.Filter(x => x.Key != "brown");

            var innerJoin = left.Join(right);
            innerJoin.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 6);

                foreach (object record in taken)
                {
                    KeyValuePair<string, Tuple<int, int>> countByWord = (KeyValuePair<string, Tuple<int, int>>)record;
                    Assert.AreEqual(countByWord.Value.Item1, countByWord.Key == "The" || countByWord.Key == "dog" ? 23 : 22);
                    Assert.AreEqual(countByWord.Value.Item2, countByWord.Key == "The" || countByWord.Key == "dog" ? 23 : 22);
                }
            });

            var leftOuterJoin = left.LeftOuterJoin(right);
            leftOuterJoin.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 7);

                foreach (object record in taken)
                {
                    KeyValuePair<string, Tuple<int, int>> countByWord = (KeyValuePair<string, Tuple<int, int>>)record;
                    Assert.AreEqual(countByWord.Value.Item1, countByWord.Key == "The" || countByWord.Key == "dog" ? 23 : 22);
                    Assert.AreEqual(countByWord.Value.Item2, countByWord.Key == "The" || countByWord.Key == "dog" ? 23 : (countByWord.Key == "brown" ? 0 : 22));
                }
            });

            var rightOuterJoin = left.RightOuterJoin(right);
            rightOuterJoin.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 8);

                foreach (object record in taken)
                {
                    KeyValuePair<string, Tuple<int, int>> countByWord = (KeyValuePair<string, Tuple<int, int>>)record;
                    Assert.AreEqual(countByWord.Value.Item1, countByWord.Key == "The" || countByWord.Key == "dog" ? 23 : (countByWord.Key == "quick" || countByWord.Key == "lazy" ? 0 : 22));
                    Assert.AreEqual(countByWord.Value.Item2, countByWord.Key == "The" || countByWord.Key == "dog" || countByWord.Key == "lazy" ? 23 : 22);
                }
            });
            
            var fullOuterJoin = left.FullOuterJoin(right);
            fullOuterJoin.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    KeyValuePair<string, Tuple<int, int>> countByWord = (KeyValuePair<string, Tuple<int, int>>)record;
                    Assert.AreEqual(countByWord.Value.Item1, countByWord.Key == "The" || countByWord.Key == "dog" ? 23 : (countByWord.Key == "quick" || countByWord.Key == "lazy" ? 0 : 22));
                    Assert.AreEqual(countByWord.Value.Item2, countByWord.Key == "The" || countByWord.Key == "dog" || countByWord.Key == "lazy" ? 23 : (countByWord.Key == "brown" ? 0 : 22));
                }
            });
        }
    }
}
