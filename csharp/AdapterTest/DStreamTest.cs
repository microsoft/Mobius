// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Streaming;
using Moq;
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
                    KeyValuePair<string, Tuple<int, Option<int>>> countByWord = (KeyValuePair<string, Tuple<int, Option<int>>>)record;
                    Assert.AreEqual(countByWord.Value.Item1, countByWord.Key == "The" || countByWord.Key == "dog" ? 23 : 22);
                    Assert.IsTrue(countByWord.Key == "The" || countByWord.Key == "dog" ? 
                        countByWord.Value.Item2.IsDefined == true && countByWord.Value.Item2.GetValue() == 23 : (countByWord.Key == "brown" ?
                        countByWord.Value.Item2.IsDefined == true == false : countByWord.Value.Item2.IsDefined == true && countByWord.Value.Item2.GetValue() == 22));
                }
            });

            var rightOuterJoin = left.RightOuterJoin(right);
            rightOuterJoin.ForeachRDD(rdd =>
            {
                var taken = rdd.Collect();
                Assert.AreEqual(taken.Length, 8);

                foreach (object record in taken)
                {
                    KeyValuePair<string, Tuple<Option<int>, int>> countByWord = (KeyValuePair<string, Tuple<Option<int>, int>>)record;
                    Assert.IsTrue(countByWord.Key == "The" || countByWord.Key == "dog" ? 
                        countByWord.Value.Item1.IsDefined == true && countByWord.Value.Item1.GetValue() == 23 : 
                        (countByWord.Key == "quick" || countByWord.Key == "lazy" ? countByWord.Value.Item1.IsDefined == false :
                        countByWord.Value.Item1.IsDefined == true && countByWord.Value.Item1.GetValue() == 22));
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
                    KeyValuePair<string, Tuple<Option<int>, Option<int>>> countByWord = (KeyValuePair<string, Tuple<Option<int>, Option<int>>>)record;
                    Assert.IsTrue(countByWord.Key == "The" || countByWord.Key == "dog" ?
                        countByWord.Value.Item1.IsDefined == true && countByWord.Value.Item1.GetValue() == 23 :
                        (countByWord.Key == "quick" || countByWord.Key == "lazy" ? countByWord.Value.Item1.IsDefined == false :
                        countByWord.Value.Item1.IsDefined == true && countByWord.Value.Item1.GetValue() == 22));

                    Assert.IsTrue(countByWord.Key == "The" || countByWord.Key == "dog" || countByWord.Key == "lazy" ? 
                        countByWord.Value.Item2.IsDefined == true && countByWord.Value.Item2.GetValue() == 23 : 
                        (countByWord.Key == "brown" ? countByWord.Value.Item2.IsDefined == false : countByWord.Value.Item2.IsDefined == true && countByWord.Value.Item2.GetValue() == 22));
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

        [Test]
        public void TestDStreamMapWithState()
        {
            var mapwithStateDStreamProxy = new Mock<IDStreamProxy>();
            var streamingContextProxy = new Mock<IStreamingContextProxy>();
            streamingContextProxy.Setup(p =>
                p.CreateCSharpStateDStream(It.IsAny<IDStreamProxy>(), It.IsAny<byte[]>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()))
                .Returns(mapwithStateDStreamProxy.Object);

            var sparkContextProxy = new Mock<ISparkContextProxy>();

            var sparkConfProxy = new Mock<ISparkConfProxy>();

            var sparkClrProxy = new Mock<ISparkCLRProxy>();
            sparkClrProxy.Setup(p => p.StreamingContextProxy).Returns(streamingContextProxy.Object);
            sparkClrProxy.Setup(p => p.SparkContextProxy).Returns(sparkContextProxy.Object);
            sparkClrProxy.Setup(p => p.CreateSparkContext(It.IsAny<ISparkConfProxy>())).Returns(sparkContextProxy.Object);
            sparkClrProxy.Setup(p => p.CreateSparkConf(It.IsAny<bool>())).Returns(sparkConfProxy.Object);

            // reset sparkCLRProxy for after test completes
            var originalSparkCLRProxy = SparkCLREnvironment.SparkCLRProxy;
            try
            {
                SparkCLREnvironment.SparkCLRProxy = sparkClrProxy.Object;

                var sparkConf = new SparkConf(false);
                var ssc = new StreamingContext(new SparkContext(sparkContextProxy.Object, sparkConf), 10000);

                var dstreamProxy = new Mock<IDStreamProxy>();
                var pairDStream = new DStream<KeyValuePair<string, int>>(dstreamProxy.Object, ssc);

                var stateSpec = new StateSpec<string, int, int, int>((k, v, s) => v);
                var stateDStream = pairDStream.MapWithState(stateSpec);
                var snapshotDStream = stateDStream.StateSnapshots();

                Assert.IsNotNull(stateDStream);
                Assert.IsNotNull(snapshotDStream);
            }
            finally
            {
                SparkCLREnvironment.SparkCLRProxy = originalSparkCLRProxy;
            }
        }

        [Test]
        public void TestDStreamMapWithStateMapWithStateHelper()
        {
            // test when initialStateRdd is null
            var stateSpec = new StateSpec<string, int, int, int>((k, v, s) => v).NumPartitions(2).Timeout(TimeSpan.FromSeconds(100));
            var helper = new MapWithStateHelper<string, int, int, int>((t, rdd) => rdd, stateSpec);

            var sparkContextProxy = new Mock<ISparkContextProxy>();
            var sc = new SparkContext(sparkContextProxy.Object, null);

            var pairwiseRddProxy = new Mock<IRDDProxy>();
            sparkContextProxy.Setup(p => p.CreatePairwiseRDD(It.IsAny<IRDDProxy>(), It.IsAny<int>(), It.IsAny<long>())).Returns(pairwiseRddProxy.Object);

            var pipelinedRddProxy = new Mock<IRDDProxy>();
            pipelinedRddProxy.Setup(p => p.Union(It.IsAny<IRDDProxy>())).Returns(new Mock<IRDDProxy>().Object);

            sparkContextProxy.Setup(p => 
                p.CreateCSharpRdd(It.IsAny<IRDDProxy>(), It.IsAny<byte[]>(), It.IsAny<Dictionary<string, string>>(), It.IsAny<List<string>>(), It.IsAny<bool>(), It.IsAny<List<Broadcast>>(), It.IsAny<List<byte[]>>()))
                .Returns(pipelinedRddProxy.Object);

            var valueRddProxy = new Mock<IRDDProxy>();
            var valuesRdd = new RDD<dynamic>(valueRddProxy.Object, sc);

            var resultRdd = helper.Execute(DateTime.UtcNow.Millisecond, null, valuesRdd);

            Assert.IsNotNull(resultRdd);

            // test when initialStateRdd is not null
            var initialStateRdd = new RDD<KeyValuePair<string, int>>(new Mock<IRDDProxy>().Object, null);
            var stateSpec2 = new StateSpec<string, int, int, int>((k, v, s) => v).InitialState(initialStateRdd).NumPartitions(2);
            var helper2 = new MapWithStateHelper<string, int, int, int>((t, rdd) => rdd, stateSpec2);

            var resultRdd2 = helper2.Execute(DateTime.UtcNow.Millisecond, null, valuesRdd);

            Assert.IsNotNull(resultRdd2);
        }

        [Test]
        public void TestDStreamMapWithStateUpdateStateHelper()
        {
            var ticks = DateTime.UtcNow.Ticks;
            var helper = new UpdateStateHelper<string, int, int, int>(
                (k, v, state) =>
                {
                    if (v < 0 && state.Exists())
                    {
                        state.Remove();
                    }
                    else if(!state.IsTimingOut())
                    {
                        state.Update(v + state.Get());
                    }
                    
                    return v;
                },
                ticks, true, TimeSpan.FromSeconds(10));

            var input = new dynamic[4];

            var preStateRddRecord = new MapWithStateRDDRecord<string, int, int>(ticks - TimeSpan.FromSeconds(2).Ticks, new [] { new KeyValuePair<string, int>("1", 1), new KeyValuePair<string, int>("2", 2)});
            preStateRddRecord.stateMap.Add("expired", new KeyedState<int>(0, ticks - TimeSpan.FromSeconds(60).Ticks));

            input[0] = preStateRddRecord;
            input[1] = new KeyValuePair<string, int>("1", -1);
            input[2] = new KeyValuePair<string, int>("2", 2);
            input[3] = new KeyValuePair<string, int>("3", 3);

            var result = helper.Execute(1, input).GetEnumerator();
            Assert.IsNotNull(result);
            Assert.IsTrue(result.MoveNext());

            MapWithStateRDDRecord<string, int, int> stateRddRecord = result.Current;

            Assert.IsNotNull(stateRddRecord);
            Assert.AreEqual(stateRddRecord.mappedData.Count, 4); // timedout record also appears in return results
            Assert.AreEqual(stateRddRecord.stateMap.Count, 2);
        }

        [Test]
        public void TestConstantInputDStream()
        {
            var sc = new SparkContext("", "");
            var rdd = sc.Parallelize(Enumerable.Range(0, 10), 1);
            var ssc = new StreamingContext(sc, 1000);

            // test when rdd is null
            Assert.Throws<ArgumentNullException>(() => new ConstantInputDStream<int>(null, ssc));

            var constantInputDStream = new ConstantInputDStream<int>(rdd, ssc);
            Assert.IsNotNull(constantInputDStream);
            Assert.AreEqual(ssc, constantInputDStream.streamingContext);     
        }
    }
}
