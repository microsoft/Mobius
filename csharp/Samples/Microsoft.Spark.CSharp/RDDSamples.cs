// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
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
            rdd.Checkpoint();
            Console.WriteLine(rdd.IsCheckpointed);
        }

        [Sample]
        internal static void RDDSampleSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 100), 4);
            Console.WriteLine(rdd.Sample(false, 0.1, 81).Count());
        }

        [Sample]
        internal static void RDDRandomSplitSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 500), 1);
            var rdds = rdd.RandomSplit(new double[] { 2, 3 }, 17);
            Console.WriteLine(rdds[0].Count());
            Console.WriteLine(rdds[1].Count());
        }

        [Sample]
        internal static void RDDTakeSampleSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 10), 2);

            Console.WriteLine(rdd.TakeSample(true, 20, 1).Length);
            Console.WriteLine(rdd.TakeSample(false, 5, 2).Length);
            Console.WriteLine(rdd.TakeSample(false, 15, 3).Length);
        }

        [Sample]
        internal static void RDDUnionSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 1, 2, 3 }, 1);
            Console.WriteLine(string.Join(",", rdd.Union(rdd).Collect()));
        }

        [Sample]
        internal static void RDDIntersectionSample()
        {
            var rdd1 = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 10, 2, 3, 4, 5 }, 1);
            var rdd2 = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 6, 2, 3, 7, 8 }, 1);
            Console.WriteLine(string.Join(",", rdd1.Intersection(rdd2).Collect()));
        }

        [Sample]
        internal static void RDDGlomSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4 }, 2);
            foreach (var l in rdd.Glom().Collect())
                Console.WriteLine(string.Join(",", l));
        }

        [Sample]
        internal static void RDDGroupBySample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 1, 2, 3, 5, 8 }, 1);
            foreach (var kv in rdd.GroupBy(x => x % 2).Collect())
                Console.WriteLine(kv.Key + ", " + string.Join(",", kv.Value));
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
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).Reduce((x, y) => x + y));
        }

        [Sample]
        internal static void RDDTreeReduceSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new int[] { -5, -4, -3, -2, -1, 1, 2, 3, 4 }, 10).TreeReduce((x, y) => x + y));
        }

        [Sample]
        internal static void RDDFoldSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).Fold(0, (x, y) => x + y));
        }

        [Sample]
        internal static void RDDAggregateSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4 }, 1).Aggregate(0, (x, y) => x + y, (x, y) => x + y));
        }

        [Sample]
        internal static void RDDTreeAggregateSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4 }, 1).TreeAggregate(0, (x, y) => x + y, (x, y) => x + y));
        }

        [Sample]
        internal static void RDDCountByValueSample()
        {
            foreach (var item in SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 1, 2, 2 }, 2).CountByValue())
                Console.WriteLine(item);
        }

        [Sample]
        internal static void RDDTakeSample()
        {
            Console.WriteLine(string.Join(",", SparkCLRSamples.SparkContext.Parallelize(new int[] { 2, 3, 4, 5, 6 }, 2).Cache().Take(2)));
        }

        [Sample]
        internal static void RDDFirstSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new int[] { 2, 3, 4 }, 2).First());
        }

        [Sample]
        internal static void RDDIsEmptySample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new int[0], 1).IsEmpty());
        }

        [Sample]
        internal static void RDDSubtractSample()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4 }, 1);
            var y = SparkCLRSamples.SparkContext.Parallelize(new int[] { 3 }, 1);
            Console.WriteLine(string.Join(",", x.Subtract(y).Collect()));
        }

        [Sample]
        internal static void RDDKeyBySample()
        {
            foreach (var kv in SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4 }, 1).KeyBy(x => x * x).Collect())
                Console.Write(kv + " ");
            Console.WriteLine();
        }

        [Sample]
        internal static void RDDRepartitionSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4, 5, 6, 7 }, 4);
            Console.WriteLine(rdd.Glom().Collect().Length);
            Console.WriteLine(rdd.Repartition(2).Glom().Collect().Length);
        }

        [Sample]
        internal static void RDDCoalesceSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 3);
            Console.WriteLine(rdd.Glom().Collect().Length);
            Console.WriteLine(rdd.Coalesce(1).Glom().Collect().Length);
        }

        [Sample]
        internal static void RDDZipSample()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 5), 1);
            var y = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(1000, 5), 1);
            foreach (var t in x.Zip(y).Collect())
                Console.WriteLine(t);
        }
        
        [Sample]
        internal static void RDDZipWithIndexSample()
        {
            foreach (var t in SparkCLRSamples.SparkContext.Parallelize(new string[] { "a", "b", "c", "d" }, 3).ZipWithIndex().Collect())
                Console.WriteLine(t);
        }

        [Sample]
        internal static void RDDZipWithUniqueIdSample()
        {
            foreach (var t in SparkCLRSamples.SparkContext.Parallelize(new string[] { "a", "b", "c", "d", "e" }, 3).ZipWithUniqueId().Collect())
                Console.WriteLine(t);
        }

        [Sample]
        internal static void RDDSetNameSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new string[] { "a", "b", "c", "d", "e" }, 3);
            Console.WriteLine(rdd.SetName("SampleRDD").Name);
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
            Console.WriteLine(string.Join(",", SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 10), 1).ToLocalIterator().ToArray()));
        }

        [Sample]
        internal static void RDDSaveAsTextFileSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new string[] { "a", "b", "c", "d", "e" }, 2);
            var path = Path.GetTempFileName();
            File.Delete(path);
            rdd.SaveAsTextFile(path);
        }

        [Sample]
        internal static void RDDCartesianSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 2 }, 1);
            foreach (var t in rdd.Cartesian(rdd).Collect())
                Console.WriteLine(t);
        }

        [Sample]
        internal static void RDDDistinctSample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 1, 2, 3 }, 1).Distinct(1).Collect();

            foreach (var v in m)
                Console.Write(v + " ");
            Console.WriteLine();
        }
        
        [Sample]
        internal static void RDDMaxSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new double[] { 1.0, 5.0, 43.0, 10.0 }, 2).Max());
        }

        [Sample]
        internal static void RDDMinSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new double[] { 2.0, 5.0, 43.0, 10.0 }, 2).Min());
        }

        [Sample]
        internal static void RDDTakeOrderedSample()
        {
            Console.WriteLine(string.Join(",", SparkCLRSamples.SparkContext.Parallelize(new int[] { 10, 1, 2, 9, 3, 4, 5, 6, 7 }, 2).TakeOrdered(6)));
        }

        [Sample]
        internal static void RDDTopSample()
        {
            Console.WriteLine(string.Join(",", SparkCLRSamples.SparkContext.Parallelize(new int[] { 2, 3, 4, 5, 6 }, 2).Top(3)));
        }

        /// <summary>
        /// Counts words in a file
        /// </summary>
        [Sample]
        internal static void RDDWordCountSample()
        {
            var lines = SparkCLRSamples.SparkContext.TextFile(SparkCLRSamples.Configuration.GetInputDataPath("words.txt"), 1);
            
            var words = lines.FlatMap(s => s.Split(new[] {" "}, StringSplitOptions.None));
            
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

                Assert.AreEqual(21, dictionary["the"]);
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
                var columns = s.Split(new[] { "," }, StringSplitOptions.None);
                return new KeyValuePair<string, string[]>(columns[0], new[] { columns[1], columns[2], columns[3] });
            });
            var metricsColumns = metrics.Map(s =>
            {
                var columns = s.Split(new[] { "," }, StringSplitOptions.None);
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
                var columns = x.Split(new[] { "," }, StringSplitOptions.None);
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
        }
    }

    [Serializable]
    public class Person
    {
        public int Age { get; set; }
    }
}
