// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// SparkCLR Pi example
    /// Calculate Pi
    /// Reference: https://github.com/apache/spark/blob/branch-1.5/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala
    /// </summary>
    public static class PiExample
    {

        private static ILoggerService Logger;
        public static void Main(string[] args)
        {
            LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance); //this is optional - DefaultLoggerService will be used if not set
            Logger = LoggerServiceFactory.GetLogger(typeof(PiExample));

            var sparkContext = new SparkContext(new SparkConf());

            try
            {
                const int slices = 3;
                var numberOfItems = (int)Math.Min(100000L * slices, int.MaxValue);
                var values = new List<int>(numberOfItems);
                for (var i = 0; i <= numberOfItems; i++)
                {
                    values.Add(i);
                }

                var rdd = sparkContext.Parallelize(values, slices);

                CalculatePiUsingAnonymousMethod(numberOfItems, rdd);

                CalculatePiUsingSerializedClassApproach(numberOfItems, rdd);

                Logger.LogInfo("Completed calculating the value of Pi");
            }
            catch (Exception ex)
            {
                Logger.LogError("Error calculating Pi");
                Logger.LogException(ex);
            }

            sparkContext.Stop();

        }

        private static void CalculatePiUsingSerializedClassApproach(int n, RDD<int> rdd)
        {
            var count = rdd
                            .Map(new PiHelper().Execute)
                            .Reduce((x, y) => x + y);

            Logger.LogInfo(string.Format("(serialized class approach) Pi is roughly {0}.", 4.0 * count / n));
        }

        private static void CalculatePiUsingAnonymousMethod(int n, RDD<int> rdd)
        {
            var count = rdd
                            .Map(i =>
                            {
                                var random = new Random();
                                var x = random.NextDouble() * 2 - 1;
                                var y = random.NextDouble() * 2 - 1;

                                return (x * x + y * y) < 1 ? 1 : 0;
                            })
                            .Reduce((x, y) => x + y);

            Logger.LogInfo(string.Format("(anonymous method approach) Pi is roughly {0}.", 4.0 * count / n));
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
    }
}