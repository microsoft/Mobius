// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using NUnit.Framework;

namespace Microsoft.Spark.CSharp.Samples
{
    class RDDSamples
    {
        [Sample]
        internal static void RDDCheckpointSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 100), 4);
            rdd.Cache();
            rdd.Unpersist();
            Assert.AreEqual(false, rdd.IsCheckpointed);
            rdd.Checkpoint();
            Assert.AreEqual(false, rdd.IsCheckpointed);
            rdd.Count();
            Console.WriteLine(rdd.IsCheckpointed);
            // rdd.IsCheckpointed is set to true immediately first action invoked on this RDD has completed
            Assert.AreEqual(true, rdd.IsCheckpointed);
        }

        [Sample]
        internal static void RDDSampleSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 100), 4);
            var sample = rdd.Sample(false, 0.1, 81);
            Console.WriteLine(sample.Count());

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var collectedOfOriginal = rdd.Collect();
                var collectedOfSample = sample.Collect();
                // sampled RDD should be a subset of original RDD
                CollectionAssert.AreEqual(collectedOfSample, collectedOfOriginal.Intersect(collectedOfSample));
            }
        }

        [Sample]
        internal static void RDDRandomSplitSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 500), 1);
            var splitted = rdd.RandomSplit(new double[] { 2, 3 }, 17);
            var countOfSplittedPartition1 = splitted[0].Count();
            var countOfSplittedPartition2 = splitted[1].Count();
            Console.WriteLine(countOfSplittedPartition1);
            Console.WriteLine(countOfSplittedPartition2);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                // sum of splitted RDD should be the original RDD
                Assert.AreEqual(500, countOfSplittedPartition1 + countOfSplittedPartition2);
            }
        }

        [Sample]
        internal static void RDDTakeSampleSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 10), 2);

            var sample1 = rdd.TakeSample(true, 20, 1);
            Console.WriteLine(sample1.Length);
            var sample2 = rdd.TakeSample(false, 5, 2);
            Console.WriteLine(sample2.Length);
            var sample3 = rdd.TakeSample(false, 15, 3);
            Console.WriteLine(sample3.Length);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(20, sample1.Length);
                Assert.AreEqual(5, sample2.Length);
                Assert.AreEqual(10, sample3.Length);
            }
        }

        [Sample]
        internal static void RDDUnionSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 1, 2, 3 }, 1);
            var union = rdd.Union(rdd).Collect();
            Console.WriteLine(string.Join(",", union));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEqual(new[] { 1, 1, 2, 3, 1, 1, 2, 3 }, union);
            }
        }

        [Sample]
        internal static void RDDIntersectionSample()
        {
            var rdd1 = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 10, 2, 3, 4, 5 }, 1);
            var rdd2 = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 6, 2, 3, 7, 8 }, 1);
            var intersected = rdd1.Intersection(rdd2).Collect();
            Console.WriteLine(string.Join(",", intersected));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEquivalent(new[] { 1, 2, 3 }, intersected);
            }
        }

        [Sample]
        internal static void RDDGlomSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4 }, 2);
            var glom = rdd.Glom().Collect();
            foreach (var l in glom)
                Console.WriteLine(string.Join(",", l));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                // length of glom should be the partition number of rdd which was specified in SparkContext.Parallelize()
                Assert.AreEqual(2, glom.Length);
                CollectionAssert.AreEquivalent(new int[] { 1, 2, 3, 4 }, glom[0].Union(glom[1]));
            }
        }

        [Sample]
        internal static void RDDGroupBySample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 1, 2, 3, 5, 8 }, 1);
            var groups = rdd.GroupBy(x => x % 2).Collect();
            foreach (var kv in groups)
                Console.WriteLine(kv.Key + ", " + string.Join(",", kv.Value));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, groups.Length);
                foreach (var kv in groups)
                {
                    // the group with key=1 is odd numbers
                    if (kv.Key == 1) CollectionAssert.AreEquivalent(new[] { 1, 1, 3, 5 }, kv.Value);
                    // the group with key=0 is even numbers
                    else if (kv.Key == 0) CollectionAssert.AreEquivalent(new[] { 2, 8 }, kv.Value);
                }
            }
        }

        [Sample]
        internal static void RDDForeachSample()
        {
            SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).Foreach(x => Console.Write(x + " "));
            Console.WriteLine();
        }

        [Sample]
        internal static void RDDForeachPartitionSample()
        {
            SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).ForeachPartition(iter => { foreach (var x in iter) Console.Write(x + " "); });
            Console.WriteLine();
        }

        [Sample]
        internal static void RDDReduceSample()
        {
            var reduced = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).Reduce((x, y) => x + y);
            Console.WriteLine(reduced);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(15, reduced);
            }
        }

        [Sample]
        internal static void RDDTreeReduceSample()
        {
            var treeReduce = SparkCLRSamples.SparkContext.Parallelize(new int[] { -5, -4, -3, -2, -1, 1, 2, 3, 4 }, 10).TreeReduce((x, y) => x + y);
            Console.WriteLine(treeReduce);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(-5, treeReduce);
            }
        }

        [Sample]
        internal static void RDDFoldSample()
        {
            var fold = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).Fold(0, (x, y) => x + y);
            Console.WriteLine(fold);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(15, fold);
            }
        }

        [Sample]
        internal static void RDDAggregateSample()
        {
            var aggregate = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4 }, 1).Aggregate(0, (x, y) => x + y, (x, y) => x + y);
            Console.WriteLine(aggregate);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(10, aggregate);
            }
        }

        [Sample]
        internal static void RDDTreeAggregateSample()
        {
            var treeAggregate = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4 }, 1).Aggregate(0, (x, y) => x + y, (x, y) => x + y);
            Console.WriteLine(treeAggregate);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(10, treeAggregate);
            }
        }

        [Sample]
        internal static void RDDCountByValueSample()
        {
            var countByValue = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 1, 2, 2 }, 2).CountByValue();
            foreach (var item in countByValue)
                Console.WriteLine(item);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, countByValue[1]);
                Assert.AreEqual(3, countByValue[2]);
            }
        }

        [Sample]
        internal static void RDDTakeSample()
        {
            var taked = SparkCLRSamples.SparkContext.Parallelize(new int[] { 2, 3, 4, 5, 6 }, 2).Cache().Take(2);
            Console.WriteLine(string.Join(",", taked));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEquivalent(new[] { 2, 3 }, taked);
            }
        }

        [Sample]
        internal static void RDDFirstSample()
        {
            var first = SparkCLRSamples.SparkContext.Parallelize(new int[] { 2, 3, 4 }, 2).First();
            Console.WriteLine(first);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, first);
            }
        }

        [Sample]
        internal static void RDDIsEmptySample()
        {
            var isEmpty = SparkCLRSamples.SparkContext.Parallelize(new int[0], 1).IsEmpty();
            Console.WriteLine(isEmpty);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(isEmpty);
            }
        }

        [Sample]
        internal static void RDDSubtractSample()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4 }, 1);
            var y = SparkCLRSamples.SparkContext.Parallelize(new int[] { 3 }, 1);
            var subtract = x.Subtract(y).Collect();
            Console.WriteLine(string.Join(",", subtract));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEquivalent(new[] { 1, 2, 4 }, subtract);
            }
        }

        [Sample]
        internal static void RDDKeyBySample()
        {
            var keyBy = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4 }, 1).KeyBy(x => x * x).Collect();
            foreach (var kv in keyBy)
                Console.Write(kv + " ");
            Console.WriteLine();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(keyBy.Contains(new KeyValuePair<int, int>(1, 1)));
                Assert.IsTrue(keyBy.Contains(new KeyValuePair<int, int>(4, 2)));
                Assert.IsTrue(keyBy.Contains(new KeyValuePair<int, int>(9, 3)));
                Assert.IsTrue(keyBy.Contains(new KeyValuePair<int, int>(16, 4)));
            }
        }

        [Sample]
        internal static void RDDRepartitionSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4, 5, 6, 7 }, 4);
            var countBeforeRepartition = rdd.Glom().Collect().Length;
            Console.WriteLine(countBeforeRepartition);
            var countAfterRepartition = rdd.Repartition(2).Glom().Collect().Length;
            Console.WriteLine(countAfterRepartition);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(4, countBeforeRepartition);
                Assert.AreEqual(2, countAfterRepartition);
            }
        }

        [Sample]
        internal static void RDDCoalesceSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 3);
            var countBeforeCoalesce = rdd.Glom().Collect().Length;
            Console.WriteLine(countBeforeCoalesce);
            var countAfterCoalesce = rdd.Coalesce(1).Glom().Collect().Length;
            Console.WriteLine(countAfterCoalesce);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(3, countBeforeCoalesce);
                Assert.AreEqual(1, countAfterCoalesce);
            }
        }

        [Sample]
        internal static void RDDZipSample()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 5), 1);
            var y = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(1000, 5), 1);
            var zip = x.Zip(y).Collect();
            foreach (var t in zip)
                Console.WriteLine(t);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                for (int i = 0; i < 5; i++)
                {
                    Assert.IsTrue(zip.Contains(new KeyValuePair<int, int>(i, 1000 + i)));
                }
            }
        }

        [Sample]
        internal static void RDDZipWithIndexSample()
        {
            var zipWithIndex = SparkCLRSamples.SparkContext.Parallelize(new string[] { "a", "b", "c", "d" }, 3).ZipWithIndex().Collect();
            foreach (var t in zipWithIndex)
                Console.WriteLine(t);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(zipWithIndex.Contains(new KeyValuePair<string, long>("a", 0)));
                Assert.IsTrue(zipWithIndex.Contains(new KeyValuePair<string, long>("b", 1)));
                Assert.IsTrue(zipWithIndex.Contains(new KeyValuePair<string, long>("c", 2)));
                Assert.IsTrue(zipWithIndex.Contains(new KeyValuePair<string, long>("d", 3)));
            }
        }

        [Sample]
        internal static void RDDZipWithUniqueIdSample()
        {
            var zipWithUniqueId = SparkCLRSamples.SparkContext.Parallelize(new string[] { "a", "b", "c", "d", "e" }, 3).ZipWithUniqueId().Collect();
            foreach (var t in zipWithUniqueId)
                Console.WriteLine(t);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(zipWithUniqueId.Contains(new KeyValuePair<string, long>("a", 0)));
                Assert.IsTrue(zipWithUniqueId.Contains(new KeyValuePair<string, long>("b", 1)));
                Assert.IsTrue(zipWithUniqueId.Contains(new KeyValuePair<string, long>("c", 4)));
                Assert.IsTrue(zipWithUniqueId.Contains(new KeyValuePair<string, long>("d", 2)));
                Assert.IsTrue(zipWithUniqueId.Contains(new KeyValuePair<string, long>("e", 5)));
            }
        }

        [Sample]
        internal static void RDDSetNameSample()
        {
            const string name = "SampleRDD";
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new string[] { "a", "b", "c", "d", "e" }, 3).SetName(name);
            Console.WriteLine(rdd.Name);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(name, rdd.Name);
            }
        }

        [Sample]
        internal static void RDDToDebugStringSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new string[] { "a", "b", "c", "d", "e" }, 3);
            Console.WriteLine(rdd.ToDebugString());
        }

        [Sample]
        internal static void RDDToLocalIteratorSample()
        {
            var localIteratorResult = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 10), 1).ToLocalIterator().ToArray();
            Console.WriteLine(string.Join(",", localIteratorResult));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEqual(new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 }, localIteratorResult);
            }
        }

        [Sample]
        internal static void RDDSaveAsTextFileSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new string[] { "a", "b", "c", "d", "e" }, 2);
            var path = Path.GetTempFileName();
            File.Delete(path);
            rdd.SaveAsTextFile(path);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(Directory.Exists(path));
            }
        }

        [Sample]
        internal static void RDDCartesianSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2 }, 1);
            var cartesian = rdd.Cartesian(rdd).Collect();
            foreach (var t in cartesian)
                Console.WriteLine(t);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(cartesian.Contains(new Tuple<int, int>(1, 1)));
                Assert.IsTrue(cartesian.Contains(new Tuple<int, int>(1, 2)));
                Assert.IsTrue(cartesian.Contains(new Tuple<int, int>(2, 1)));
                Assert.IsTrue(cartesian.Contains(new Tuple<int, int>(2, 2)));
            }
        }

        [Sample]
        internal static void RDDDistinctSample()
        {
            var distinct = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 1, 2, 3 }, 1).Distinct(1).Collect();

            foreach (var v in distinct)
                Console.Write(v + " ");
            Console.WriteLine();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEquivalent(new[] { 1, 2, 3 }, distinct);
            }
        }

        [Sample]
        internal static void RDDMaxSample()
        {
            var max = SparkCLRSamples.SparkContext.Parallelize(new double[] { 1.0, 5.0, 43.0, 10.0 }, 2).Max();
            Console.WriteLine(max);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(43.0, max);
            }
        }

        [Sample]
        internal static void RDDMinSample()
        {
            var min = SparkCLRSamples.SparkContext.Parallelize(new double[] { 2.0, 5.0, 43.0, 10.0 }, 2).Min();
            Console.WriteLine(min);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2.0, min);
            }
        }

        [Sample]
        internal static void RDDTakeOrderedSample()
        {
            var takeOrderd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 10, 1, 2, 9, 3, 4, 5, 6, 7 }, 2).TakeOrdered(6);
            Console.WriteLine(string.Join(",", takeOrderd));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEquivalent(new[] { 1, 2, 3, 4, 5, 6 }, takeOrderd);
            }
        }

        [Sample]
        internal static void RDDTopSample()
        {
            var top = SparkCLRSamples.SparkContext.Parallelize(new int[] { 2, 3, 4, 5, 6 }, 2).Top(3);
            Console.WriteLine(string.Join(",", top));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEquivalent(new[] { 6, 5, 4 }, top);
            }
        }

        /// <summary>
        /// Counts words in a file
        /// </summary>
        [Sample]
        internal static void RDDWordCountSample()
        {
            var lines = SparkCLRSamples.SparkContext.TextFile(SparkCLRSamples.Configuration.GetInputDataPath("words.txt"), 1);
            
            var words = lines.FlatMap(s => s.Split(' '));
            
            var wordCounts = words.Map(w => new KeyValuePair<string, int>(w.Trim(), 1))
                                  .ReduceByKey((x, y) => x + y).Collect();

            Console.WriteLine("*** Printing words and their counts ***");
            foreach (var kvp in wordCounts)
            {
                Console.WriteLine("'{0}':{1}", kvp.Key, kvp.Value);
            }

            var wordCountsCaseInsensitve = words.Map(w => new KeyValuePair<string, int>(w.ToLower().Trim(), 1))
                                                .ReduceByKey((x, y) => x + y).Collect();

            Console.WriteLine("*** Printing words and their counts ignoring case ***");
            foreach (var kvp in wordCountsCaseInsensitve)
            {
                Console.WriteLine("'{0}':{1}", kvp.Key, kvp.Value);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var dictionary = new Dictionary<string, int>();
                foreach (var kvp in wordCounts)
                {
                    dictionary[kvp.Key] = kvp.Value;
                }

                Assert.AreEqual(22, dictionary["the"]);
                Assert.AreEqual(23, dictionary["The"]);
                Assert.AreEqual(23, dictionary["dog"]);

                var caseInsenstiveWordCountDictionary = new Dictionary<string, int>();
                foreach (var kvp in wordCountsCaseInsensitve)
                {
                    caseInsenstiveWordCountDictionary[kvp.Key] = kvp.Value;
                }

                Assert.AreEqual(45, caseInsenstiveWordCountDictionary["the"]);
                Assert.AreEqual(23, caseInsenstiveWordCountDictionary["dog"]);
            }

        }

        /// <summary>
        /// Performs a join of 2 RDDs and run reduction
        /// </summary>
        [Sample]
        internal static void RDDJoinSample()
        {
            var requests = SparkCLRSamples.SparkContext.TextFile(SparkCLRSamples.Configuration.GetInputDataPath("requestslog.txt"), 1);
            var metrics = SparkCLRSamples.SparkContext.TextFile(SparkCLRSamples.Configuration.GetInputDataPath("metricslog.txt"), 1);
           
            var requestsColumns = requests.Map(s =>
            {
                var columns = s.Split(',');
                return new KeyValuePair<string, string[]>(columns[0], new[] { columns[1], columns[2], columns[3] });
            });
            var metricsColumns = metrics.Map(s =>
            {
                var columns = s.Split(',');
                return new KeyValuePair<string, string[]>(columns[3], new[] { columns[4], columns[5], columns[6] });
            });

            var requestsJoinedWithMetrics = requestsColumns.Join(metricsColumns)
                                                            .Map(
                                                                s =>
                                                                    new []
                                                                    {
                                                                        s.Key, //guid
                                                                        s.Value.Item1[0], s.Value.Item1[1], s.Value.Item1[2], //dc, abtestid, traffictype
                                                                        s.Value.Item2[0],s.Value.Item2[1], s.Value.Item2[2] //lang, country, metric
                                                                    });


            var latencyByDatacenter = requestsJoinedWithMetrics.Map(i => new KeyValuePair<string, int> (i[1], int.Parse(i[6]))); //key is "datacenter"      
            var maxLatencyByDataCenterList = latencyByDatacenter.ReduceByKey(Math.Max).Collect();

            Console.WriteLine("***** Max latency metrics by DC *****");
            foreach (var keyValuePair in maxLatencyByDataCenterList)
            {
                Console.WriteLine("Datacenter={0}, Max latency={1}", keyValuePair.Key, keyValuePair.Value);
            }
            
            var latencyAndCountByDatacenter = requestsJoinedWithMetrics.Map(i => new KeyValuePair<string, Tuple<int,int>> (i[1], new Tuple<int, int>(int.Parse(i[6]), 1)));
            var sumLatencyAndCountByDatacenter = latencyAndCountByDatacenter.ReduceByKey((tuple, tuple1) => new Tuple<int, int>((tuple == null ? 0 : tuple.Item1) + tuple1.Item1, (tuple == null ? 0 : tuple.Item2) + tuple1.Item2));
            var sumLatencyAndCountByDatacenterList = sumLatencyAndCountByDatacenter.Collect();

            Console.WriteLine("***** Mean latency metrics by DC *****");
            foreach (var keyValuePair in sumLatencyAndCountByDatacenterList)
            {
                Console.WriteLine("Datacenter={0}, Mean latency={1}", keyValuePair.Key, keyValuePair.Value.Item1/keyValuePair.Value.Item2);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var dictionary = new Dictionary<string, int>();
                foreach (var kvp in maxLatencyByDataCenterList)
                {
                    dictionary[kvp.Key] = kvp.Value;
                }

                Assert.AreEqual(835, dictionary["iowa"]);
                Assert.AreEqual(1256, dictionary["singapore"]);

                var meanDictionary = new Dictionary<string, Tuple<int, int>>();
                foreach (var kvp in sumLatencyAndCountByDatacenterList)
                {
                    meanDictionary[kvp.Key] = new Tuple<int, int>(kvp.Value.Item1, kvp.Value.Item2);
                }

                Assert.AreEqual(1621, meanDictionary["iowa"].Item1);
                Assert.AreEqual(2, meanDictionary["iowa"].Item2);
                Assert.AreEqual(1256, meanDictionary["singapore"].Item1);
                Assert.AreEqual(1, meanDictionary["singapore"].Item2);
            }
        }

        /// <summary>
        /// Sample for map and filter in RDD
        /// </summary>
        [Sample]
        internal static void RDDMapFilterSample()
        {

            var mulogs = SparkCLRSamples.SparkContext.TextFile(SparkCLRSamples.Configuration.GetInputDataPath("csvtestlog.txt"), 2);
            var mulogsProjected = mulogs.Map(x =>
            {
                var columns = x.Split(',');
                return string.Format("{0},{1},{2},{3}", columns[0], columns[1], columns[2], columns[3]);
            });

            var muLogsFiltered = mulogsProjected.Filter(s => s.Contains("US,EN"));
            var count = muLogsFiltered.Count();
            var collectedItems = muLogsFiltered.Collect();
            Console.WriteLine("MapFilterExample: EN-US entries count is " + count);
            Console.WriteLine("Items are...");
            foreach (var collectedItem in collectedItems)
            {
                Console.WriteLine(collectedItem);
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(6, count);
                Assert.AreEqual(6, collectedItems.Count());
            }
        }

        /// <summary>
        /// Sample for distributing objects as RDD
        /// </summary>
        [Sample]
        internal static void RDDSerializableObjectCollectionSample()
        {
            var personsRdd = SparkCLRSamples.SparkContext.Parallelize(new[] { new Person { Age = 3 }, new Person { Age = 10 }, new Person { Age = 15 } }, 3);
            var derivedPersonsRdd = personsRdd.Map(x => new Person { Age = x.Age + 1 });
            var countOfPersonsFiltered = derivedPersonsRdd.Filter(person => person.Age >= 11).Count();
            Console.WriteLine("SerializableObjectCollectionExample: countOfPersonsFiltered " + countOfPersonsFiltered);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, countOfPersonsFiltered);
            }
        }

        /// <summary>
        /// Sample for distributing strings as RDD
        /// </summary>
        [Sample]
        internal static void RDDStringCollectionSample()
        {
            var logEntriesRdd = SparkCLRSamples.SparkContext.Parallelize(new[] { "row1col1,row1col2", "row2col1,row2col2", "row3col3" }, 2);
            var logEntriesColumnRdd = logEntriesRdd.Map(x => x.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries));
            var countOfInvalidLogEntries = logEntriesColumnRdd.Filter(stringarray => stringarray.Length != 2).Count();
            Console.WriteLine("StringCollectionExample: countOfInvalidLogEntries " + countOfInvalidLogEntries);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(1, countOfInvalidLogEntries);
            }
        }

        /// <summary>
        /// Sample for distributing int as RDD
        /// </summary>
        [Sample]
        internal static void RDDIntCollectionSample()
        {
            var numbersRdd = SparkCLRSamples.SparkContext.Parallelize(new[] { 1, 100, 5, 55, 65 }, 3);
            var oddNumbersRdd = numbersRdd.Filter(x => x % 2 != 0);
            var countOfOddNumbers = oddNumbersRdd.Count();
            Console.WriteLine("IntCollectionExample: countOfOddNumbers " + countOfOddNumbers);
            
            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(4, countOfOddNumbers);
            }
        }

        /// <summary>
        /// Sample for CombineByKey method
        /// </summary>
        [Sample]
        internal static void RDDCombineBySample()
        {
            var markets = SparkCLRSamples.SparkContext.TextFile(SparkCLRSamples.Configuration.GetInputDataPath("market.tab"), 1);
            long totalMarketsCount = markets.Count();

            var marketsByKey = markets.Map(x => new KeyValuePair<string, string>(x.Substring(0, x.IndexOf('-')), x));
            var categories = marketsByKey.PartitionBy(2)
                .CombineByKey(() => "", (c, v) => v.Substring(0, v.IndexOf('-')), (c1, c2) => c1, 2);
            var categoriesCollectedCount = categories.Collect().Count();

            var joinedRddCollectedItemCount = marketsByKey.Join(categories, 2).Collect().Count();

            var filteredRddCollectedItemCount = markets.Filter(line => line.Contains("EN")).Collect().Count();
            //var markets = filtered.reduce((left, right) => left + right);
            var combinedRddCollectedItemCount = marketsByKey.PartitionBy(2).CombineByKey(() => "", (c, v) => c + v, (c1, c2) => c1 + c2, 2).Collect().Count();
            Console.WriteLine("MarketExample: totalMarketsCount {0}, joinedRddCollectedItemCount {1}, filteredRddCollectedItemCount {2}, combinedRddCollectedItemCount {3}", totalMarketsCount, joinedRddCollectedItemCount, filteredRddCollectedItemCount, combinedRddCollectedItemCount);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(281, totalMarketsCount);
                Assert.AreEqual(281, joinedRddCollectedItemCount);
                Assert.AreEqual(102, filteredRddCollectedItemCount);
                Assert.AreEqual(78, combinedRddCollectedItemCount);
            }
        }
    }

    [Serializable]
    public class Person
    {
        public int Age { get; set; }
    }
}
