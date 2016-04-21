// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Extra functions available on RDDs of Doubles through an implicit conversion. 
    /// </summary>
    public static class DoubleRDDFunctions
    {
        /// <summary>
        /// Add up the elements in this RDD.
        /// 
        /// sc.Parallelize(new double[] {1.0, 2.0, 3.0}).Sum()
        /// 6.0
        /// 
        /// </summary>
        /// <param name="self"></param>
        /// <returns></returns>
        public static double Sum(this RDD<double> self)
        {
            return self.Fold(0.0, (x, y) => x + y);
        }

        /// <summary>
        /// Return a <see cref="StatCounter"/> object that captures the mean, variance
        /// and count of the RDD's elements in one operation.
        /// </summary>
        /// <param name="self"></param>
        /// <returns></returns>
        public static StatCounter Stats(this RDD<double> self)
        {
            return self.MapPartitionsWithIndex((pid, iter) => new List<StatCounter> { new StatCounter(iter) }).Reduce((l, r) => l.Merge(r));
        }

        /// <summary>
        /// Compute a histogram using the provided buckets. The buckets
        /// are all open to the right except for the last which is closed.
        /// e.g. [1,10,20,50] means the buckets are [1,10) [10,20) [20,50],
        /// which means 1&lt;=x&lt;10, 10&lt;=x&lt;20, 20&lt;=x&lt;=50. And on the input of 1
        /// and 50 we would have a histogram of 1,0,1.
        /// 
        /// If your histogram is evenly spaced (e.g. [0, 10, 20, 30]),
        /// this can be switched from an O(log n) inseration to O(1) per
        /// element(where n = # buckets).
        /// 
        /// Buckets must be sorted and not contain any duplicates, must be
        /// at least two elements.
        /// 
        /// If `buckets` is a number, it will generates buckets which are
        /// evenly spaced between the minimum and maximum of the RDD. For
        /// example, if the min value is 0 and the max is 100, given buckets
        /// as 2, the resulting buckets will be [0,50) [50,100]. buckets must
        /// be at least 1 If the RDD contains infinity, NaN throws an exception
        /// If the elements in RDD do not vary (max == min) always returns
        /// a single bucket.
        /// 
        /// It will return an tuple of buckets and histogram.
        /// 
        /// >>> rdd = sc.parallelize(range(51))
        /// >>> rdd.histogram(2)
        /// ([0, 25, 50], [25, 26])
        /// >>> rdd.histogram([0, 5, 25, 50])
        /// ([0, 5, 25, 50], [5, 20, 26])
        /// >>> rdd.histogram([0, 15, 30, 45, 60])  # evenly spaced buckets
        /// ([0, 15, 30, 45, 60], [15, 15, 15, 6])
        /// >>> rdd = sc.parallelize(["ab", "ac", "b", "bd", "ef"])
        /// >>> rdd.histogram(("a", "b", "c"))
        /// (('a', 'b', 'c'), [2, 2])
        /// 
        /// </summary>
        /// <param name="self"></param>
        /// <param name="bucketCount"></param>
        /// <returns></returns>
        public static Tuple<double[], long[]> Histogram(this RDD<double> self, int bucketCount)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Compute the mean of this RDD's elements.
        /// sc.Parallelize(new double[]{1, 2, 3}).Mean()
        /// 2.0
        /// </summary>
        /// <param name="self"></param>
        /// <returns></returns>
        public static double Mean(this RDD<double> self)
        {
            return self.Stats().Mean;
        }

        /// <summary>
        /// Compute the variance of this RDD's elements.
        /// sc.Parallelize(new double[]{1, 2, 3}).Variance()
        /// 0.666...
        /// </summary>
        /// <param name="self"></param>
        /// <returns></returns>
        public static double Variance(this RDD<double> self)
        {
            return self.Stats().Variance;
        }

        /// <summary>
        /// Compute the standard deviation of this RDD's elements.
        /// sc.Parallelize(new double[]{1, 2, 3}).Stdev()
        /// 0.816...
        /// </summary>
        /// <param name="self"></param>
        /// <returns></returns>
        public static double Stdev(this RDD<double> self)
        {
            return self.Stats().Stdev;
        }

        /// <summary>
        /// Compute the sample standard deviation of this RDD's elements (which
        /// corrects for bias in estimating the standard deviation by dividing by
        /// N-1 instead of N).
        /// 
        /// sc.Parallelize(new double[]{1, 2, 3}).SampleStdev()
        /// 1.0
        /// 
        /// </summary>
        /// <param name="self"></param>
        /// <returns></returns>
        public static double SampleStdev(this RDD<double> self)
        {
            return self.Stats().SampleStdev;
        }

        /// <summary>
        /// Compute the sample variance of this RDD's elements (which corrects
        /// for bias in estimating the variance by dividing by N-1 instead of N).
        /// 
        /// sc.Parallelize(new double[]{1, 2, 3}).SampleVariance()
        /// 1.0
        /// 
        /// </summary>
        /// <param name="self"></param>
        /// <returns></returns>
        public static double SampleVariance(this RDD<double> self)
        {
            return self.Stats().SampleVariance;
        }
    }
}
