// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Spark.CSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Extra functions available on RDDs of (key, value) pairs where the key is sortable through
    /// a function to sort the key.
    /// </summary>
    public static class OrderedRDDFunctions
    {

        /// <summary>
        /// Sorts this RDD, which is assumed to consist of KeyValuePair pairs.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="ascending"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static RDD<KeyValuePair<K, V>> SortByKey<K, V>(this RDD<KeyValuePair<K, V>> self,
            bool ascending = true, int? numPartitions = null)
        {
            return SortByKey<K, V, K>(self, ascending, numPartitions, new DefaultSortKeyFuncHelper<K>().Execute);
        }
        /// <summary>
        /// Sorts this RDD, which is assumed to consist of KeyValuePairs. If key is type of string, case is sensitive.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <param name="self"></param>
        /// <param name="ascending"></param>
        /// <param name="numPartitions">Number of partitions. Each partition of the sorted RDD contains a sorted range of the elements.</param>
        /// <param name="keyFunc">RDD will sort by keyFunc(key) for every key in KeyValuePair. Must not be null.</param>
        /// <returns></returns>
        public static RDD<KeyValuePair<K, V>> SortByKey<K, V, U>(this RDD<KeyValuePair<K, V>> self,
            bool ascending, int? numPartitions, Func<K, U> keyFunc)
        {
            if (keyFunc == null)
            {
                throw new ArgumentNullException("keyFunc cannot be null.");
            }

            if (numPartitions == null)
            {
                numPartitions = self.GetDefaultPartitionNum();
            }

            if (numPartitions == 1)
            {
                if (self.GetNumPartitions() > 1)
                {
                    self = self.Coalesce(1);
                }
                return self.MapPartitionsWithIndex(new SortByKeyHelper<K, V, U>(keyFunc, ascending).Execute, true);
            }

            var rddSize = self.Count();
            if (rddSize == 0) return self; // empty RDD

            var maxSampleSize = numPartitions.Value * 20; // constant from Spark's RangePartitioner
            double fraction = Math.Min((double)maxSampleSize / Math.Max(rddSize, 1), 1.0);

            /* first compute the boundary of each part via sampling: we want to partition
             * the key-space into bins such that the bins have roughly the same
             * number of (key, value) pairs falling into them */
            U[] samples = self.Sample(false, fraction, 1).Map(kv => kv.Key).Collect().Select(k => keyFunc(k)).ToArray();
            Array.Sort(samples, StringComparer.Ordinal); // case sensitive if key type is string

            List<U> bounds = new List<U>();
            for (int i = 0; i < numPartitions - 1; i++)
            {
                bounds.Add(samples[(int)(samples.Length * (i + 1) / numPartitions)]);
            }

            return self.PartitionBy(numPartitions.Value, 
                new PairRDDFunctions.PartitionFuncDynamicTypeHelper<K>(
                    new RangePartitionerHelper<K, U>(numPartitions.Value, keyFunc, bounds, ascending).Execute)
                    .Execute)
                        .MapPartitionsWithIndex(new SortByKeyHelper<K, V, U>(keyFunc, ascending).Execute, true);
        }
        
        /// <summary>
        /// Repartition the RDD according to the given partitioner and, within each resulting partition,
        /// sort records by their keys.
        ///
        /// This is more efficient than calling `repartition` and then sorting within each partition
        /// because it can push the sorting down into the shuffle machinery.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="numPartitions"></param>
        /// <param name="partitionFunc"></param>
        /// <param name="ascending"></param>
        /// <returns></returns>
        public static RDD<KeyValuePair<K, V>> repartitionAndSortWithinPartitions<K, V>(
            this RDD<KeyValuePair<K, V>> self, 
            int? numPartitions = null, 
            Func<K, int> partitionFunc = null, 
            bool ascending = true)
        {
            return self.MapPartitionsWithIndex<KeyValuePair<K, V>>((pid, iter) => ascending ? iter.OrderBy(kv => kv.Key) : iter.OrderByDescending(kv => kv.Key));
        }

        [Serializable]
        internal class SortByKeyHelper<K, V, U>
        {
            private readonly Func<K, U> func;
            private readonly bool ascending;
            public SortByKeyHelper(Func<K, U> f, bool ascending = true)
            {
                func = f;
                this.ascending = ascending;
            }

            public IEnumerable<KeyValuePair<K, V>> Execute(int pid, IEnumerable<KeyValuePair<K, V>> kvs)
            {
                IEnumerable<KeyValuePair<K, V>> ordered;
                if (ascending)
                {
                    if (typeof(K) == typeof(string))
                        ordered = kvs.OrderBy(k => func(k.Key).ToString(), StringComparer.Ordinal);
                    else
                        ordered = kvs.OrderBy(k => func(k.Key));
                }
                else
                {
                    if (typeof(K) == typeof(string))
                        ordered = kvs.OrderByDescending(k => func(k.Key).ToString(), StringComparer.Ordinal);
                    else
                        ordered = kvs.OrderByDescending(k => func(k.Key));
                }
                return ordered;
            }
        }

        [Serializable]
        internal class DefaultSortKeyFuncHelper<K>
        {
            public K Execute(K key) { return key; }
        }

        [Serializable]
        internal class RangePartitionerHelper<K, U>
        {
            private readonly int numPartitions;
            private readonly Func<K, U> keyFunc;
            private readonly List<U> bounds;
            private readonly bool ascending;
            public RangePartitionerHelper(int numPartitions, Func<K, U> keyFunc, List<U> bounds, bool ascending)
            {
                this.numPartitions = numPartitions;
                this.bounds = bounds;
                this.keyFunc = keyFunc;
                this.ascending = ascending;
            }

            public int Execute(K key)
            {
                // Binary search the insert position in the bounds. If key found, return the insert position; if not, a negative
                // number that is the bitwise complement of insert position is returned, so bitwise inversing it.
                var pos = bounds.BinarySearch(keyFunc(key));
                if (pos < 0) pos = ~pos;

                return ascending ? pos : numPartitions - 1 - pos;
            }
        }
    }
}
