// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using NUnit.Framework;

namespace Microsoft.Spark.CSharp.Samples
{
    class MiscSamples
    {
        /// <summary>
        /// Sample to compute th evalue of Pi
        /// </summary>
        [Sample]
        internal static void PiSample()
        {
            const int slices = 3;
            var n = (int) Math.Min(100000L*slices, int.MaxValue);
            var values = new List<int>(n);
            for (int i = 0; i <= n; i++)
            {
                values.Add(i);
            }
            var count = SparkCLRSamples.SparkContext.Parallelize(values, slices)
                            .Map(i =>
                                    {
                                        var random = new Random();  //if this definition is moved out of the anonymous method, 
                                                                    //the delegate will form a closure and the compiler 
                                                                    //will generate a type for it without Serializable attribute 
                                                                    //and hence serialization will fail
                                                                    //the alternative is to use PiHelper below which is marked Serializable 
                                        var x = random.NextDouble() * 2 - 1;
                                        var y = random.NextDouble() * 2 - 1;

                                        return (x * x + y * y) < 1 ? 1 : 0;
                                    }
                                )
                            .Reduce((x, y) => x + y);
            Console.WriteLine("Pi is roughly " + 4.0 * (int)count / n);

            /********* Alternative to the count method provided above. This produces more accurate results because of the way Random is used ***********/
            var countComputedUsingAnotherApproach = SparkCLRSamples.SparkContext.Parallelize(values, slices).Map(new PiHelper().Execute).Reduce((x, y) => x + y);
            var approximatePiValue = 4.0* countComputedUsingAnotherApproach/n;
            Console.WriteLine("Pi is roughly " + approximatePiValue);
            /********************************************************************/

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.IsTrue(Math.Abs(approximatePiValue - 3.14) <= 0.019);
            }
        }

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
