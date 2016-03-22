// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using NUnit.Framework;

namespace Microsoft.Spark.CSharp.Samples
{
    class PairRDDSamples
    {
        [Sample]
        internal static void PairRDDCollectAsMapSample()
        {
            var map = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<int, int>(1, 2), new KeyValuePair<int, int>(3, 4) }, 1).CollectAsMap();

            foreach (var kv in map)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(map.ContainsKey(1) && map[1] == 2);
                Assert.IsTrue(map.ContainsKey(1) && map[3] == 4);
            }
        }

        [Sample]
        internal static void PairRDDKeysSample()
        {
            var keys = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<int, int>(1, 2), new KeyValuePair<int, int>(3, 4) }, 1).Keys().Collect();

            Console.WriteLine(keys[0]);
            Console.WriteLine(keys[1]);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(1, keys[0]);
                Assert.AreEqual(3, keys[1]);
            }
        }

        [Sample]
        internal static void PairRDDValuesSample()
        {
            var values = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<int, int>(1, 2), new KeyValuePair<int, int>(3, 4) }, 1).Values().Collect();

            Console.WriteLine(values[0]);
            Console.WriteLine(values[1]);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, values[0]);
                Assert.AreEqual(4, values[1]);
            }
        }

        [Sample]
        internal static void PairRDDReduceByKeySample()
        {
            var reduced = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .ReduceByKey((x, y) => x + y).Collect();

            foreach (var kv in reduced)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(reduced.Contains(new KeyValuePair<string, int>("a", 2)));
                Assert.IsTrue(reduced.Contains(new KeyValuePair<string, int>("b", 1)));
            }
        }

        [Sample]
        internal static void PairRDDReduceByKeyLocallySample()
        {
            var reduced = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .ReduceByKeyLocally((x, y) => x + y);

            foreach (var kv in reduced)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(reduced.Contains(new KeyValuePair<string, int>("a", 2)));
                Assert.IsTrue(reduced.Contains(new KeyValuePair<string, int>("b", 1)));
            }
        }

        [Sample]
        internal static void PairRDDCountByKeySample()
        {
            var countByKey = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .CountByKey();

            foreach (var kv in countByKey)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, countByKey["a"]);
                Assert.AreEqual(1, countByKey["b"]);
            }
        }

        [Sample]
        internal static void PairRDDJoinSample()
        {
            var l = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 4),
                }, 1);

            var r = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 2), 
                    new KeyValuePair<string, int>("a", 3),
                }, 1);

            var joined = l.Join(r, 2).Collect();

            foreach (var kv in joined)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(joined.Contains(new KeyValuePair<string, Tuple<int, int>>("a", new Tuple<int, int>(1, 2))));
                Assert.IsTrue(joined.Contains(new KeyValuePair<string, Tuple<int, int>>("a", new Tuple<int, int>(1, 3))));
            }
        }

        [Sample]
        internal static void PairRDDLeftOuterJoinSample()
        {
            var l = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 4),
                }, 2);

            var r = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 2), 
                }, 1);

            var joined = l.LeftOuterJoin(r).Collect();

            foreach (var kv in joined)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(joined.Any(kv => kv.Key == "a" && kv.Value.Item1 == 1 && kv.Value.Item2.IsDefined && kv.Value.Item2.GetValue() == 2));
                Assert.IsTrue(joined.Any(kv => kv.Key == "b" && kv.Value.Item1 == 4 && !kv.Value.Item2.IsDefined));
            }
        }

        [Sample]
        internal static void PairRDDRightOuterJoinSample()
        {
            var l = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 2), 
                }, 1);

            var r = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 4),
                }, 2);

            var joined = l.RightOuterJoin(r).Collect();

            foreach (var kv in joined)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(joined.Any(kv => kv.Key == "a" && kv.Value.Item1.IsDefined && kv.Value.Item1.GetValue() == 2 && kv.Value.Item2 == 1));
                Assert.IsTrue(joined.Any(kv => kv.Key == "b" && !kv.Value.Item1.IsDefined && kv.Value.Item2 == 4));
            }
        }

        [Sample]
        internal static void PairRDDFullOuterJoinSample()
        {
            var l = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 4),
                }, 2);

            var r = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 2), 
                    new KeyValuePair<string, int>("c", 8), 
                }, 2);

            var joined = l.FullOuterJoin(r).Collect();

            foreach (var kv in joined)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(joined.Any(kv => kv.Key == "a" && kv.Value.Item1.IsDefined && kv.Value.Item1.GetValue() == 1 && 
                kv.Value.Item2.IsDefined && kv.Value.Item2.GetValue() == 2));
                Assert.IsTrue(joined.Any(kv => kv.Key == "b" && kv.Value.Item1.IsDefined && kv.Value.Item1.GetValue() == 4 &&
                !kv.Value.Item2.IsDefined));
                Assert.IsTrue(joined.Any(kv => kv.Key == "c" && !kv.Value.Item1.IsDefined &&
                kv.Value.Item2.IsDefined && kv.Value.Item2.GetValue() == 8));
            }
        }

        [Sample]
        internal static void PairRDDPartitionBySample()
        {
            Func<dynamic, int> partitionFunc = key =>
            {
                if (key < 3) return 1;
                if (key >= 3 && key < 6) return 2;
                else return 3;
            };

            var partitioned = SparkCLRSamples.SparkContext.Parallelize(new[] { 1, 2, 3, 4, 5, 6, 1 }, 3)
                .Map(x => new KeyValuePair<int, int>(x, x + 100))
                .PartitionBy(3, partitionFunc)
                .Glom()
                .Collect();

            foreach (var partition in partitioned)
            {
                foreach (var kv in partition)
                {
                    Console.Write(kv + " ");
                }
                Console.WriteLine();
            }

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(3, partitioned.Length);
                // Assert that the partition distribution is correct with partitionFunc
                Assert.IsTrue(partitioned.Count(p => p.All(key => key.Key < 3)) == 1);
                Assert.IsTrue(partitioned.Count(p => p.All(key => key.Key >= 3 && key.Key < 6)) == 1);
                Assert.IsTrue(partitioned.Count(p => p.All(key => key.Key >= 6)) == 1);
            }
        }

        [Sample]
        internal static void PairRDDCombineByKeySample()
        {
            var combineByKey = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .CombineByKey(() => string.Empty, (x, y) => x + y.ToString(CultureInfo.InvariantCulture), (x, y) => x + y).Collect();

            foreach (var kv in combineByKey)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(combineByKey.Contains(new KeyValuePair<string, string>("a", "11")));
                Assert.IsTrue(combineByKey.Contains(new KeyValuePair<string, string>("b", "1")));
            }
        }

        [Sample]
        internal static void PairRDDAggregateByKeySample()
        {
            var aggregateByKey = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .AggregateByKey(() => 0, (x, y) => x + y, (x, y) => x + y).Collect();

            foreach (var kv in aggregateByKey)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(aggregateByKey.Contains(new KeyValuePair<string, int>("a", 2)));
                Assert.IsTrue(aggregateByKey.Contains(new KeyValuePair<string, int>("b", 1)));
            }
        }

        [Sample]
        internal static void PairRDDFoldByKeySample()
        {
            var FoldByKey = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .FoldByKey(() => 0, (x, y) => x + y).Collect();

            foreach (var kv in FoldByKey)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(FoldByKey.Contains(new KeyValuePair<string, int>("a", 2)));
                Assert.IsTrue(FoldByKey.Contains(new KeyValuePair<string, int>("b", 1)));
            }
        }

        [Sample]
        internal static void PairRDDGroupByKeySample()
        {
            var groupByKey = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .GroupByKey().Collect();

            foreach (var kv in groupByKey)
                Console.WriteLine(kv.Key + ", " + "(" + string.Join(",", kv.Value) + ")");

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(groupByKey.Any(kv => kv.Key == "a" && kv.Value.Count == 2 && kv.Value[0] == 1 && kv.Value[1] == 1));
                Assert.IsTrue(groupByKey.Any(kv => kv.Key == "b" && kv.Value.Count == 1 && kv.Value[0] == 1));
            }
        }

        [Sample]
        internal static void PairRDDMapValuesSample()
        {
            var mapValues = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, string[]>("a", new[]{"apple", "banana", "lemon"}), 
                    new KeyValuePair<string, string[]>("b", new[]{"grapes"})
                }, 2)
                .MapValues(x => x.Length).Collect();

            foreach (var kv in mapValues)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(mapValues.Any(kv => kv.Key == "a" && kv.Value == 3));
                Assert.IsTrue(mapValues.Any(kv => kv.Key == "b" && kv.Value == 1));
            }
        }

        [Sample]
        internal static void PairRDDFlatMapValuesSample()
        {
            var flatMapValues = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, string[]>("a", new[]{"x", "y", "z"}), 
                    new KeyValuePair<string, string[]>("b", new[]{"p", "r"})
                }, 2)
                .FlatMapValues(x => x).Collect();

            foreach (var kv in flatMapValues)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(flatMapValues.Any(kv => kv.Key == "a" && kv.Value == "x"));
                Assert.IsTrue(flatMapValues.Any(kv => kv.Key == "a" && kv.Value == "y"));
                Assert.IsTrue(flatMapValues.Any(kv => kv.Key == "a" && kv.Value == "z"));
                Assert.IsTrue(flatMapValues.Any(kv => kv.Key == "b" && kv.Value == "p"));
                Assert.IsTrue(flatMapValues.Any(kv => kv.Key == "b" && kv.Value == "r"));
            }
        }

        [Sample]
        internal static void PairRDDGroupWithSample()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 1), new KeyValuePair<string, int>("b", 4)}, 2);
            var y = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 2)}, 1);

            var groupWith = x.GroupWith(y).Collect();

            foreach (var kv in groupWith)
                Console.WriteLine(kv.Key + ", " + "(" + string.Join(",", kv.Value) + ")");

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(groupWith.Any(kv => kv.Key == "a" && kv.Value.Item1[0] == 1 && kv.Value.Item2[0] == 2));
                Assert.IsTrue(groupWith.Any(kv => kv.Key == "b" && kv.Value.Item1[0] == 4 && !kv.Value.Item2.Any()));
            }
        }

        [Sample]
        internal static void PairRDDGroupWithSample2()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 5), new KeyValuePair<string, int>("b", 6) }, 2);
            var y = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 1), new KeyValuePair<string, int>("b", 4) }, 2);
            var z = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 2) }, 1);

            var groupWith = x.GroupWith(y, z).Collect();

            foreach (var kv in groupWith)
                Console.WriteLine(kv.Key + ", " + "(" + string.Join(",", kv.Value) + ")");

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(groupWith.Any(kv => kv.Key == "a" && kv.Value.Item1[0] == 5 && kv.Value.Item2[0] == 1 && kv.Value.Item3[0] == 2));
                Assert.IsTrue(groupWith.Any(kv => kv.Key == "b" && kv.Value.Item1[0] == 6 && kv.Value.Item2[0] == 4 && !kv.Value.Item3.Any()));
            }
        }

        //TO DO: implement PairRDDFunctions.SampleByKey
        //[Sample]
        //internal static void PairRDDSampleByKeySample()
        //{
        //    var fractions = new Dictionary<string, double> { { "a", 0.2 }, { "b", 0.1 } };
        //    var rdd = SparkCLRSamples.SparkContext.Parallelize(fractions.Keys.ToArray(), 2).Cartesian(SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 1000), 2));
        //    var sample = rdd.Map(t => new KeyValuePair<string, int>(t.Item1, t.Item2)).SampleByKey(false, fractions, 2).GroupByKey().Collect();

        //    Console.WriteLine(sample);
        //}

        [Sample]
        internal static void PairRDDSubtractByKeySample()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int?>("a", 1), new KeyValuePair<string, int?>("b", 4), new KeyValuePair<string, int?>("b", 5), new KeyValuePair<string, int?>("a", 2) }, 2);
            var y = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int?>("a", 3), new KeyValuePair<string, int?>("c", null) }, 2);

            var subtractByKey = x.SubtractByKey(y).Collect();

            foreach (var kv in subtractByKey)
                Console.WriteLine(kv);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, subtractByKey.Length);
                subtractByKey.Contains(new KeyValuePair<string, int?>("b", 4));
                subtractByKey.Contains(new KeyValuePair<string, int?>("b", 5));
            }
        }

        [Sample]
        internal static void PairRDDLookupSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 1000).Zip(Enumerable.Range(0, 1000), (x, y) => new KeyValuePair<int, int>(x, y)), 10);
            var lookup42 = rdd.Lookup(42);
            var lookup1024 = rdd.Lookup(1024);
            Console.WriteLine(string.Join(",", lookup42));
            Console.WriteLine(string.Join(",", lookup1024));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEqual(new[] { 42 }, lookup42);
                Assert.AreEqual(0, lookup1024.Length);
            }
        }

        [Sample]
        internal static void PairRDDSortByKeySample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("B", 2),
                new KeyValuePair<string, int>("a", 1), new KeyValuePair<string, int>("c", 3),
                new KeyValuePair<string, int>("E", 5), new KeyValuePair<string, int>("D", 4)}, 3);

            var sortedRdd = rdd.SortByKey(true, 2);
            var sortedInTotal = sortedRdd.Collect();
            var sortedPartitions = sortedRdd.Glom().Collect();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, sortedPartitions.Length);
                // by default SortByKey is case sensitive
                CollectionAssert.AreEqual(new[] { "B", "D", "E", "a", "c" }, sortedInTotal.Select(kv => kv.Key).ToArray());
            }

            // convert the keys to lower case in order to sort with case insensitive
            sortedRdd = rdd.SortByKey(true, 2, key => key.ToLowerInvariant());
            sortedInTotal = sortedRdd.Collect();
            sortedPartitions = sortedRdd.Glom().Collect();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(2, sortedPartitions.Length);
                CollectionAssert.AreEqual(new[] { "a", "B", "c", "D", "E" }, sortedInTotal.Select(kv => kv.Key).ToArray());
            }
        }
    }
}
