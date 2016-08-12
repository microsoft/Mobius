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
    class SparkContextSamples
    {
        [Serializable]
        internal class BroadcastHelper<T>
        {
            private readonly Broadcast<T[]> broadcastVar;

            internal BroadcastHelper(Broadcast<T[]> broadcastVar)
            {
                this.broadcastVar = broadcastVar;
            }

            internal IEnumerable<T> Execute(int i)
            {
                return broadcastVar.Value;
            }
        }

        [Sample]
        internal static void SparkContextBroadcastSample()
        {
            var b = SparkCLRSamples.SparkContext.Broadcast<int[]>(Enumerable.Range(1, 5).ToArray());
            foreach (var value in b.Value)
            {
                Console.Write(value + " ");
            }
            Console.WriteLine();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEqual(new[] {1, 2, 3, 4, 5}, b.Value);
            }

            RDD<int> rdd = SparkCLRSamples.SparkContext.Parallelize(new[] {0, 0}, 1);
            var r = rdd.FlatMap(new BroadcastHelper<int>(b).Execute).Collect();
            foreach (var value in r)
            {
                Console.Write(value + " ");
            }
            Console.WriteLine();

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                // each item in rdd is mapped to broadcast value.
                CollectionAssert.AreEqual(new[] {1, 2, 3, 4, 5, 1, 2, 3, 4, 5}, r);
            }
        }

        [Serializable]
        internal class AccumulatorHelper
        {
            private Accumulator<int> accumulator;
            private bool async;

            internal AccumulatorHelper(Accumulator<int> accumulator, bool async = false)
            {
                this.accumulator = accumulator;
                this.async = async;
            }

            internal void Execute(int input)
            {
                if (async)
                {
                    // start new task
                    var task = new Task(() =>
                    {
                        accumulator += input;
                    });
                    task.Start();
                    task.Wait();
                }
                else
                {
                    accumulator += input;
                }

            }
        }

        [Sample]
        internal static void SparkContextAccumulatorSample()
        {
            var a = SparkCLRSamples.SparkContext.Accumulator<int>(100);
            var b = SparkCLRSamples.SparkContext.Accumulator<int>(100);

            SparkCLRSamples.SparkContext.Parallelize(new[] {1, 2, 3, 4}, 3).Foreach(new AccumulatorHelper(a).Execute);
            SparkCLRSamples.SparkContext.Parallelize(new[] {1, 2, 3, 4}, 3)
                .Foreach(new AccumulatorHelper(b, true).Execute);
            Console.WriteLine("accumulator value, a: {0}, b: {1}", a.Value, b.Value);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                // The value is accumulated on the initial value of the Accumulator which is 100. 110 = 100 + 1 + 2 + 3 + 4
                Assert.AreEqual(110, a.Value);
                Assert.AreEqual(110, b.Value);
            }
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

            var dir = SparkCLRSamples.FileSystemHelper.GetTempPath();
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
            var rdd1 = SparkCLRSamples.SparkContext.Parallelize(new int[] {1, 1, 2, 3}, 1);
            var rdd2 = SparkCLRSamples.SparkContext.Parallelize(new int[] {1, 1, 2, 3}, 1);

            var union = SparkCLRSamples.SparkContext.Union(new[] {rdd1, rdd2}).Collect();
            Console.WriteLine(string.Join(",", union));

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                CollectionAssert.AreEqual(new[] {1, 1, 2, 3, 1, 1, 2, 3}, union);
            }
        }

        [Sample]
        internal static void SparkContextHadoopConfigurationSample()
        {
            var hadoopConf = SparkCLRSamples.SparkContext.HadoopConfiguration;
            var initialValue = hadoopConf.Get("testproperty", "defaultvalue");

            hadoopConf.Set("testproperty", "testvalue");

            var finalValue = hadoopConf.Get("testproperty", "defaultvalue");

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual("defaultvalue", initialValue);
                Assert.AreEqual("testvalue", finalValue);
            }
        }
    }
}
