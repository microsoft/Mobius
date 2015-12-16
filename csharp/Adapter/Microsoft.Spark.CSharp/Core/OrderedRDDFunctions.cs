// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Core
{
    public static class OrderedRDDFunctions
    {
        /// <summary>
        /// Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
        /// `collect` or `save` on the resulting RDD will return or output an ordered list of records
        /// (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
        /// order of the keys).
        /// 
        /// >>> tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
        /// >>> sc.parallelize(tmp).sortByKey().first()
        /// ('1', 3)
        /// >>> sc.parallelize(tmp).sortByKey(True, 1).collect()
        /// [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
        /// >>> sc.parallelize(tmp).sortByKey(True, 2).collect()
        /// [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
        /// >>> tmp2 = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
        /// >>> tmp2.extend([('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)])
        /// >>> sc.parallelize(tmp2).sortByKey(True, 3, keyfunc=lambda k: k.lower()).collect()
        /// [('a', 3), ('fleece', 7), ('had', 2), ('lamb', 5),...('white', 9), ('whose', 6)]
        /// 
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="ascending"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static RDD<KeyValuePair<K, V>> SortByKey<K, V>(
            this RDD<KeyValuePair<K, V>> self, 
            bool ascending = true, 
            int? numPartitions = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Repartition the RDD according to the given partitioner and, within each resulting partition,
        /// sort records by their keys.
        ///
        /// This is more efficient than calling `repartition` and then sorting within each partition
        /// because it can push the sorting down into the shuffle machinery.
        /// 
        /// >>> rdd = sc.parallelize([(0, 5), (3, 8), (2, 6), (0, 8), (3, 8), (1, 3)])
        /// >>> rdd2 = rdd.repartitionAndSortWithinPartitions(2, lambda x: x % 2, 2)
        /// >>> rdd2.glom().collect()
        /// [[(0, 5), (0, 8), (2, 6)], [(1, 3), (3, 8), (3, 8)]]
        /// 
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
    }
}
