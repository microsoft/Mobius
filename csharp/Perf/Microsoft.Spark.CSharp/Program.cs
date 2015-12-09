// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.PerfBenchmark
{
    /// <summary>
    /// Spark driver implementation in scala used for SparkCLR perf benchmarking
    /// </summary>
    class PerfBenchmark
    {
        internal static SparkContext SparkContext;
        internal static List<TimeSpan> ExecutionTimeList = new List<TimeSpan>();
        internal static Dictionary<string, List<TimeSpan>> PerfResults = new Dictionary<string, List<TimeSpan>>();

        public static void Main(string[] args)
        {
            Console.WriteLine("Arguments are {0}", string.Join(",", args));

            InitializeSparkContext(args);
            RunBenchmarks(args);
            StopSparkContext();

            ReportResult();
        }

        private static void InitializeSparkContext(string[] args)
        {
            var sparkConf = new SparkConf();
            sparkConf.Set("spark.local.dir", args[0]);
            sparkConf.SetAppName("SparkCLR perf suite - C#");
            SparkContext = new SparkContext(sparkConf);
        }

        private static void StopSparkContext()
        {
            SparkContext.Stop();
        }

        internal static void RunBenchmarks(string[] args)
        {
            var perfSuites = Assembly.GetEntryAssembly().GetTypes()
                .SelectMany(type => type.GetMethods(BindingFlags.NonPublic | BindingFlags.Static))
                .Where(method => method.GetCustomAttributes(typeof (PerfSuiteAttribute), false).Length > 0)
                .OrderByDescending(method => method.Name);

            foreach (var perfSuite in perfSuites)
            {
                ExecutionTimeList.Clear();
                int runCount = int.Parse(args[1]);

                while (runCount > 0)
                {
                    perfSuite.Invoke(null, new object[] { args });
                    runCount--;
                }

                var executionTimeListRef = new List<TimeSpan>(ExecutionTimeList);
                PerfResults.Add(perfSuite.Name, executionTimeListRef);
            }

        }

        internal static void ReportResult()
        {
            Console.WriteLine("** Printing results of the perf run (C#) **");

            foreach (var perfResultItem in PerfResults)
            {
                var perfResult = perfResultItem.Value;

                var runTimeInSeconds = perfResult.Select(x => (int) x.TotalSeconds);
                //multiple enumeration happening - ignoring that for now
                var max = runTimeInSeconds.Max();
                var min = runTimeInSeconds.Min();
                var avg = runTimeInSeconds.Average();
                Console.WriteLine(
                    "** Execution time for {0} in seconds. Min={1}, Max={2}, Average={3}, Number of runs={4}, Individual values={5} **",
                    perfResultItem.Key, min, max, avg, runTimeInSeconds.Count(), string.Join(", ", runTimeInSeconds));
            }
        }
    }

    class PerfSuiteAttribute : Attribute { }
}
