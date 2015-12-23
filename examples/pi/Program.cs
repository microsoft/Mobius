// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Microsoft.Spark.CSharp.Core;

[assembly: log4net.Config.XmlConfigurator(Watch = true)]

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// SparkCLR Pi example
    /// </summary>
    public static class PiExample
    {
        internal static log4net.ILog Logger { get { return log4net.LogManager.GetLogger(typeof(PiExample)); } }
        internal static SparkContext SparkContext;

        private static void Main(string[] args)
        {
            var success = true;

            SparkContext = CreateSparkContext();
            SparkContext.SetCheckpointDir(Path.GetTempPath());

            var stopWatch = Stopwatch.StartNew();
            var clockStart = stopWatch.Elapsed;
            try
            {
                Logger.Info("----- Running Pi example -----");

                Pi();

                var duration = stopWatch.Elapsed - clockStart;
                Logger.InfoFormat("----- Successfully finished running Pi example (duration={0}) -----", duration);
            }
            catch (Exception ex)
            {
                success = false;
                var duration = stopWatch.Elapsed - clockStart;
                Logger.InfoFormat("----- Error running Pi example (duration={0}) -----{1}{2}", duration, Environment.NewLine, ex);
            }

            Logger.Info("Completed running examples. Calling SparkContext.Stop() to tear down ...");
            // following comment is necessary due to known issue in Spark. See https://issues.apache.org/jira/browse/SPARK-8333
            Logger.Info("If this program (SparkCLRExamples.exe) does not terminate in 10 seconds, please manually terminate java process launched by this program!!!");

            SparkContext.Stop();

            if (!success)
            {
                Environment.Exit(1);
            }
        }

        /// <summary>
        /// Calculate Pi
        /// Reference: https://github.com/apache/spark/blob/branch-1.5/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala
        /// </summary>
        private static void Pi()
        {
            const int slices = 3;
            var n = (int)Math.Min(100000L * slices, int.MaxValue);
            var values = new List<int>(n);
            for (var i = 0; i <= n; i++)
            {
                values.Add(i);
            }

            //
            // Anonymous method approach
            //
            var count = SparkContext.Parallelize(values, slices)
                            .Map(i =>
                                {
                                    var random = new Random();  
                                    var x = random.NextDouble() * 2 - 1;
                                    var y = random.NextDouble() * 2 - 1;

                                    return (x * x + y * y) < 1 ? 1 : 0;
                                }
                             ).Reduce((x, y) => x + y);
            Logger.InfoFormat("(anonymous method approach) Pi is roughly {0}.", 4.0 * (int)count / n);

            //
            // Serialized class approach, an alternative to the anonymous method approach above
            //
            var countComputedUsingAnotherApproach = SparkContext.Parallelize(values, slices).Map(new PiHelper().Execute).Reduce((x, y) => x + y);
            var approximatePiValue = 4.0 * countComputedUsingAnotherApproach / n;
            Logger.InfoFormat("(serialized class approach) Pi is roughly {0}.", approximatePiValue);
        }

        /// <summary>
        /// Serialized class used in RDD Map Transformation
        /// </summary>
        [Serializable]
        private class PiHelper
        {
            private readonly Random random = new Random();
            public int Execute(int input)
            {
                var x = random.NextDouble() * 2 - 1;
                var y = random.NextDouble() * 2 - 1;

                return (x * x + y * y) < 1 ? 1 : 0;
            }
        }

        /// <summary>
        /// Creates and returns a context
        /// </summary>
        /// <returns>SparkContext</returns>
        private static SparkContext CreateSparkContext()
        {
            var conf = new SparkConf();

            // set up local directory
            var tempDir = Environment.GetEnvironmentVariable("spark.local.dir");
            if (string.IsNullOrEmpty(tempDir))
            {
                tempDir = Path.GetTempPath();
            }

            conf.Set("spark.local.dir", tempDir);
            Logger.DebugFormat("spark.local.dir is set to {0}", tempDir);

            return new SparkContext(conf);
        }
    }
}