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

namespace Microsoft.Spark.CSharp.Samples
{
    class PairRDDSamples
    {
        [Sample]
        internal static void PairRDDCollectAsMapSample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<int, int>(1, 2), new KeyValuePair<int, int>(3, 4) }, 1).CollectAsMap();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDKeysSample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<int, int>(1, 2), new KeyValuePair<int, int>(3, 4) }, 1).Keys().Collect();

            Console.WriteLine(m[0]);
            Console.WriteLine(m[1]);
        }

        [Sample]
        internal static void PairRDDValuesSample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<int, int>(1, 2), new KeyValuePair<int, int>(3, 4) }, 1).Values().Collect();

            Console.WriteLine(m[0]);
            Console.WriteLine(m[1]);
        }

        [Sample]
        internal static void PairRDDReduceByKeySample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .ReduceByKey((x, y) => x + y).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDReduceByKeyLocallySample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .ReduceByKeyLocally((x, y) => x + y);

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDCountByKeySample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .CountByKey();

            foreach (var kv in m)
                Console.WriteLine(kv);
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

            var m = l.Join(r, 2).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
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

            var m = l.LeftOuterJoin(r).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
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

            var m = l.RightOuterJoin(r).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
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

            var m = l.FullOuterJoin(r).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDPartitionBySample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(new[] { 1, 2, 3, 4, 2, 4, 1 }, 1)
                .Map(x => new KeyValuePair<int, int>(x, x))
                .PartitionBy(3)
                .Glom()
                .Collect();

            foreach (var a in m)
            {
                foreach (var kv in a)
                {
                    Console.Write(kv + " ");
                }
                Console.WriteLine();
            }
        }

        [Sample]
        internal static void PairRDDCombineByKeySample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .CombineByKey(() => string.Empty, (x, y) => x + y.ToString(), (x, y) => x + y).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDAggregateByKeySample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .AggregateByKey(() => 0, (x, y) => x + y, (x, y) => x + y).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDFoldByKeySample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .FoldByKey(() => 0, (x, y) => x + y).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDGroupByKeySample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, int>("a", 1), 
                    new KeyValuePair<string, int>("b", 1),
                    new KeyValuePair<string, int>("a", 1)
                }, 2)
                .GroupByKey().MapValues(l => string.Join(" ", l)).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDMapValuesSample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, string[]>("a", new[]{"apple", "banana", "lemon"}), 
                    new KeyValuePair<string, string[]>("b", new[]{"grapes"})
                }, 2)
                .MapValues(x => x.Length).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDFlatMapValuesSample()
        {
            var m = SparkCLRSamples.SparkContext.Parallelize(
                new[] 
                { 
                    new KeyValuePair<string, string[]>("a", new[]{"x", "y", "z"}), 
                    new KeyValuePair<string, string[]>("b", new[]{"p", "r"})
                }, 2)
                .FlatMapValues(x => x).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDGroupWithSample()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 1), new KeyValuePair<string, int>("b", 4)}, 2);
            var y = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 2)}, 1);

            var m = x.GroupWith(y).MapValues(l => string.Join(" ", l.Item1) + " : " + string.Join(" ", l.Item2)).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDGroupWithSample2()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 5), new KeyValuePair<string, int>("b", 6) }, 2);
            var y = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 1), new KeyValuePair<string, int>("b", 4) }, 2);
            var z = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 2) }, 1);

            var m = x.GroupWith(y, z).MapValues(l => string.Join(" ", l.Item1) + " : " + string.Join(" ", l.Item2) + " : " + string.Join(" ", l.Item3)).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDGroupWithSample3()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 5), new KeyValuePair<string, int>("b", 6) }, 2);
            var y = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 1), new KeyValuePair<string, int>("b", 4) }, 2);
            var z = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("a", 2) }, 1);
            var w = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int>("b", 42) }, 1);

            var m = x.GroupWith(y, z, w).MapValues(l => string.Join(" ", l.Item1) + " : " + string.Join(" ", l.Item2) + " : " + string.Join(" ", l.Item3) + " : " + string.Join(" ", l.Item4)).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        //[Sample]
        internal static void PairRDDSampleByKeySample()
        {
            var fractions = new Dictionary<string, double> { { "a", 0.2 }, { "b", 0.1 } };
            var rdd = SparkCLRSamples.SparkContext.Parallelize(fractions.Keys.ToArray(), 2).Cartesian(SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 1000), 2));
            var sample = rdd.Map(t => new KeyValuePair<string, int>(t.Item1, t.Item2)).SampleByKey(false, fractions, 2).GroupByKey().Collect();

            Console.WriteLine(sample);
        }

        [Sample]
        internal static void PairRDDSubtractByKeySample()
        {
            var x = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int?>("a", 1), new KeyValuePair<string, int?>("b", 4), new KeyValuePair<string, int?>("b", 5), new KeyValuePair<string, int?>("a", 2) }, 2);
            var y = SparkCLRSamples.SparkContext.Parallelize(new[] { new KeyValuePair<string, int?>("a", 3), new KeyValuePair<string, int?>("c", null) }, 2);

            var m = x.SubtractByKey(y).Collect();

            foreach (var kv in m)
                Console.WriteLine(kv);
        }

        [Sample]
        internal static void PairRDDLookupSample()
        {
            var rdd = SparkCLRSamples.SparkContext.Parallelize(Enumerable.Range(0, 1000).Zip(Enumerable.Range(0, 1000), (x, y) => new KeyValuePair<int, int>(x, y)), 10);
            Console.WriteLine(string.Join(",", rdd.Lookup(42)));
            Console.WriteLine(string.Join(",", rdd.Lookup(1024)));
        }
    }
}
