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
    class SparkContextSamples
    {
        [Serializable]
        internal class BroadcastHelper<T>
        {
            private readonly T[] value;
            internal BroadcastHelper(T[] value)
            {
                this.value = value;
            }

            internal IEnumerable<T> Execute(int i)
            {
                return value;
            }
        }

        [Sample]
        internal static void SparkContextBroadcastSample()
        {
            var b = SparkCLRSamples.SparkContext.Broadcast<int[]>(Enumerable.Range(1, 5).ToArray());
            foreach (var value in b.Value)
                Console.Write(value + " ");
            Console.WriteLine();

            b.Unpersist();

            var r = SparkCLRSamples.SparkContext.Parallelize(new[] { 0, 0 }, 1).FlatMap(new BroadcastHelper<int>(b.Value).Execute).Collect();
            foreach (var value in r)
                Console.Write(value + " ");
            Console.WriteLine();
        }

        [Serializable]
        internal class AccumulatorHelper
        {
            private Accumulator<int> accumulator;
            internal AccumulatorHelper(Accumulator<int> accumulator)
            {
                this.accumulator = accumulator;
            }

            internal int Execute(int input)
            {
                accumulator += 1;
                return input;
            }
        }

        [Sample]
        internal static void SparkContextAccumulatorSample()
        {
            var a = SparkCLRSamples.SparkContext.Accumulator<int>(1);
            var r = SparkCLRSamples.SparkContext.Parallelize(new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 3).Map(new AccumulatorHelper(a).Execute).Collect();

            Console.WriteLine(a.Value);
        }

        [Sample]
        internal static void SparkContextSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Version);
            Console.WriteLine(SparkCLRSamples.SparkContext.SparkUser);
            Console.WriteLine(SparkCLRSamples.SparkContext.StartTime);
            Console.WriteLine(SparkCLRSamples.SparkContext.DefaultParallelism);
            Console.WriteLine(SparkCLRSamples.SparkContext.DefaultMinPartitions);
            
            StatusTracker StatusTracker = SparkCLRSamples.SparkContext.StatusTracker;

            //var file = Path.GetTempFileName();
            //File.WriteAllText(file, "Sample");
            //SparkCLRSamples.SparkContext.AddFile(file);

            var dir = Path.GetTempPath();
            SparkCLRSamples.SparkContext.SetCheckpointDir(dir);

            SparkCLRSamples.SparkContext.SetLogLevel("DEBUG");
            //SparkCLRSamples.SparkContext.SetJobGroup("SampleGroupId", "Sample Description");
            SparkCLRSamples.SparkContext.SetLocalProperty("SampleKey", "SampleValue");
            
            Console.WriteLine(SparkCLRSamples.SparkContext.GetLocalProperty("SampleKey"));
            SparkCLRSamples.SparkContext.CancelJobGroup("SampleGroupId");
            SparkCLRSamples.SparkContext.CancelAllJobs();
        }

        [Sample]
        internal static void SparkContextUnionSample()
        {
            var rdd1 = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 1, 2, 3 }, 1);
            var rdd2 = SparkCLRSamples.SparkContext.Parallelize(new int[] { 1, 1, 2, 3 }, 1);
            Console.WriteLine(string.Join(",", SparkCLRSamples.SparkContext.Union(new[] { rdd1, rdd2 }).Collect()));
        }
    }
}
