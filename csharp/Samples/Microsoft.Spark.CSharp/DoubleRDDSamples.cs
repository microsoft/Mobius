// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Samples
{
    class DoubleRDDSamples
    {
        [Sample]
        internal static void DoubleRDDSumSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new double[] { 1.0, 2.0, 3.0 }, 2).Sum());
        }

        [Sample]
        internal static void DoubleRDDStatsSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new double[] { 1, 2, 3 }, 2).Stats().ToString());
        }

        [Sample]
        internal static void DoubleRDDMeanSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new double[] { 1, 2, 3 }, 2).Mean().ToString(CultureInfo.InvariantCulture));
        }

        [Sample]
        internal static void DoubleRDVarianceSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new double[] { 1, 2, 3 }, 2).Variance().ToString(CultureInfo.InvariantCulture));
        }

        [Sample]
        internal static void DoubleRDDStdevSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new double[] { 1, 2, 3 }, 2).Stdev().ToString(CultureInfo.InvariantCulture));
        }

        [Sample]
        internal static void DoubleRDDSampleStdevSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new double[] { 1, 2, 3 }, 2).SampleStdev().ToString(CultureInfo.InvariantCulture));
        }

        [Sample]
        internal static void DoubleRDDSampleVarianceSample()
        {
            Console.WriteLine(SparkCLRSamples.SparkContext.Parallelize(new double[] { 1, 2, 3 }, 2).SampleVariance().ToString(CultureInfo.InvariantCulture));
        }
    }
}
