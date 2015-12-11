// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class DStreamTest
    {
        [Test]
        public void TestDStreamMapReduce()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            var lines = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(lines.DStreamProxy);

            var words = lines.FlatMap(l => l.Split(' ')).Filter(w => w != "The").Repartition(1);

            words.Slice(DateTime.MinValue, DateTime.MaxValue);
            words.Cache();
            words.Checkpoint(1000);

            words.Count().ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 1);
                Assert.AreEqual((int)taken[0], 178);
            });

            words.CountByValue().ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 8);

                foreach (object record in taken)
                {
                    KeyValuePair<string, long> countByWord = (KeyValuePair<string, long>)record;
                    Assert.AreEqual(countByWord.Value, countByWord.Key == "The" || countByWord.Key == "dog" || countByWord.Key == "lazy" ? 23 : 22);
                }
            });

            words.CountByValueAndWindow(1, 1).ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken[0], 8);
            });

            words.CountByWindow(1).ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 1);
                Assert.AreEqual((int)taken[0], 356);
            });

            words.Union(words).ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 356);
            });

            words.Glom().ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 1);
                Assert.AreEqual((taken[0] as string[]).Length, 178);
            });
        }

        [Test]
        public void TestDStreamTransform()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            var lines = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(lines.DStreamProxy);

            var words = lines.FlatMap(l => l.Split(' '));

            var pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));

            var wordCounts = pairs.PartitionBy().ReduceByKey((x, y) => x + y);

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

            var wordLists = pairs.GroupByKey();

            wordLists.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    KeyValuePair<string, List<int>> countByWord = (KeyValuePair<string, List<int>>)record;
                    Assert.AreEqual(countByWord.Value.Count, countByWord.Key == "The" || countByWord.Key == "dog" || countByWord.Key == "lazy" ? 23 : 22);
                }
            });

            var wordCountsByWindow = pairs.ReduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, 1);

            wordCountsByWindow.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    KeyValuePair<string, int> countByWord = (KeyValuePair<string, int>)record;
                    Assert.AreEqual(countByWord.Value, countByWord.Key == "The" || countByWord.Key == "dog" || countByWord.Key == "lazy" ? 46 : 44);
                }
            });
        }
        
        [Test]
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

            var groupWith = left.GroupWith(right);
            groupWith.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    KeyValuePair<string, Tuple<List<int>, List<int>>> countByWord = (KeyValuePair<string, Tuple<List<int>, List<int>>>)record;
                    if (countByWord.Key == "quick" || countByWord.Key == "lazy")
                        Assert.AreEqual(countByWord.Value.Item1.Count, 0);
                    else if (countByWord.Key == "brown")
                        Assert.AreEqual(countByWord.Value.Item2.Count, 0);
                    else
                    {
                        Assert.AreEqual(countByWord.Value.Item1[0], countByWord.Key == "The" || countByWord.Key == "dog" ? 23 : 22);
                        Assert.AreEqual(countByWord.Value.Item2[0], countByWord.Key == "The" || countByWord.Key == "dog" || countByWord.Key == "lazy" ? 23 : 22);
                    }
                }
            });

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
            rightOuterJoin.ForeachRDD(rdd =>
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
            fullOuterJoin.ForeachRDD(rdd =>
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

        [Test]
        public void TestDStreamUpdateStateByKey()
        {
            var ssc = new StreamingContext(new SparkContext("", ""), 1000);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            var lines = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(lines.DStreamProxy);

            var words = lines.FlatMap(l => l.Split(' '));

            var pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));

            var doubleCounts = pairs.GroupByKey().FlatMapValues(vs => vs).MapValues(v => 2 * v).ReduceByKey((x, y) => x + y);
            doubleCounts.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    KeyValuePair<string, int> countByWord = (KeyValuePair<string, int>)record;
                    Assert.AreEqual(countByWord.Value, countByWord.Key == "The" || countByWord.Key == "dog" || countByWord.Key == "lazy" ? 2 * 23 : 2 * 22);
                }
            });

            // disable pipeline to UpdateStateByKey which replys on checkpoint mock proxy doesn't support
            pairs.Cache();

            var state = pairs.UpdateStateByKey<string, int, int>((v, s) => s + (v as List<int>).Count);
            state.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 9);

                foreach (object record in taken)
                {
                    KeyValuePair<string, int> countByWord = (KeyValuePair<string, int>)record;
                    Assert.AreEqual(countByWord.Value, countByWord.Key == "The" || countByWord.Key == "dog" || countByWord.Key == "lazy" ? 24 : 23);
                }
            });
        }
    }
}
