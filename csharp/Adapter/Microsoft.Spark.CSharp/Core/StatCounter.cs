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
    /// A class for tracking the statistics of a set of numbers (count, mean and variance) in a numerically
    /// robust way. Includes support for merging two StatCounters. Based on Welford and Chan's algorithms
    /// for running variance. 
    /// </summary>
    [Serializable]
    public class StatCounter
    {
        private long n = 0;     // Running count of our values
        private double mu = 0;  // Running mean of our values
        private double m2 = 0;  // Running variance numerator (sum of (x - mean)^2)
        private double maxValue = double.MinValue; // Running max of our values
        private double minValue = double.MaxValue; // Running min of our values

        /// <summary>
        /// Initializes the StatCounter with no values.
        /// </summary>
        public StatCounter()
        { }

        /// <summary>
        /// Initializes the StatCounter with the given values.
        /// </summary>
        /// <param name="values"></param>
        public StatCounter(IEnumerable<double> values)
        {
            Merge(values);
        }

        /// <summary>
        /// Add a value into this StatCounter, updating the internal statistics.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        internal StatCounter Merge(double value)
        {
            var delta = value - mu;
            n += 1;
            mu += delta / n;
            m2 += delta * (value - mu);
            maxValue = Math.Max(maxValue, value);
            minValue = Math.Min(minValue, value);

            return this;
        }

        /// <summary>
        /// Add multiple values into this StatCounter, updating the internal statistics.
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        internal StatCounter Merge(IEnumerable<double> values)
        {
            foreach (var value in values)
                Merge(value);
            return this;
        }

        /// <summary>
        /// Merge another StatCounter into this one, adding up the internal statistics.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        internal StatCounter Merge(StatCounter other)
        {
            if (other == this) 
            {
                return Merge(other.copy());  // Avoid overwriting fields in a weird order
            } 
            else 
            {
                if (n == 0) 
                {
                    mu = other.mu;
                    m2 = other.m2;
                    n = other.n;
                    maxValue = other.maxValue;
                    minValue = other.minValue;
                } 
                else if (other.n != 0) 
                {
                    var delta = other.mu - mu;
                    if (other.n * 10 < n) 
                    {
                        mu = mu + (delta * other.n) / (n + other.n);
                    } 
                    else if (n * 10 < other.n) 
                    {
                        mu = other.mu - (delta * n) / (n + other.n);
                    } 
                    else 
                    {
                        mu = (mu * n + other.mu * other.n) / (n + other.n);
                    }
                    m2 += other.m2 + (delta * delta * n * other.n) / (n + other.n);
                    n += other.n;
                    maxValue = Math.Max(maxValue, other.maxValue);
                    minValue = Math.Min(minValue, other.minValue);
                }
                return this;
            }
        }

        /// <summary>
        /// Clone this StatCounter
        /// </summary>
        /// <returns></returns>
        internal StatCounter copy()
        {
            var other = new StatCounter();
            other.n = n;
            other.mu = mu;
            other.m2 = m2;
            other.maxValue = maxValue;
            other.minValue = minValue;
            return other;
        }

        /// <summary>
        /// Gets the count number of this StatCounter
        /// </summary>
        public long Count { get { return n; } }

        /// <summary>
        /// Gets the average number of this StatCounter
        /// </summary>
        public double Mean { get { return mu; } }

        /// <summary>
        /// Gets the sum number of this StatCounter
        /// </summary>
        public double Sum { get { return n * mu; } }

        /// <summary>
        /// Gets the maximum number of this StatCounter
        /// </summary>
        public double Max { get { return maxValue; } }

        /// <summary>
        /// Gets the minimum number of this StatCounter
        /// </summary>
        public double Min { get { return minValue; } }
        
        /// <summary>
        /// Return the variance of the values.
        /// </summary>
        public double Variance { get { return n == 0 ? double.NaN : m2 / n; } }
        
        /// <summary>
        /// Return the sample variance, which corrects for bias in estimating the variance by dividing by N-1 instead of N.
        /// </summary>
        public double SampleVariance { get { return n <= 1 ? double.NaN : m2 / (n - 1); } }
        
        /// <summary>
        /// Return the standard deviation of the values.
        /// </summary>
        public double Stdev { get { return Math.Sqrt(Variance); } }
        
        /// <summary>
        /// Return the sample standard deviation of the values, which corrects for bias in estimating the variance by dividing by N-1 instead of N.
        /// </summary>
        public double SampleStdev { get { return Math.Sqrt(SampleVariance); } }

        /// <summary>
        /// Returns a string that represents this StatCounter.
        /// </summary>
        /// <returns>
        /// A string that represents this StatCounter.
        /// </returns>
        public override string ToString()
        {
            return string.Format("(count: {0}, mean: {1}, stdev: {2}, max: {3}, min: {4})", Count, Mean, Stdev, Max, Min);
        }
    }
}
