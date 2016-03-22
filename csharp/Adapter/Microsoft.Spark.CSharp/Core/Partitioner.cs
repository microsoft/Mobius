// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// An object that defines how the elements in a key-value pair RDD are partitioned by key.
    /// Maps each key to a partition ID, from 0 to "numPartitions - 1".
    /// </summary>
    [Serializable]
    public class Partitioner
    {
        private readonly int numPartitions;
        private readonly Func<dynamic, int> partitionFunc;

        /// <summary>
        /// Create a <seealso cref="Partitioner"/> instance.
        /// </summary>
        /// <param name="numPartitions">Number of partitions.</param>
        /// <param name="partitionFunc">Defines how the elements in a key-value pair RDD are partitioned by key. Input of Func is key, output is partition index.
        /// Warning: diffrent Func instances are considered as different partitions which will cause repartition.</param>
        public Partitioner(int numPartitions, Func<dynamic, int> partitionFunc)
        {
            this.numPartitions = numPartitions;
            this.partitionFunc = partitionFunc;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;

            var otherPartitioner = obj as Partitioner;
            if (otherPartitioner != null)
            {
                return otherPartitioner.numPartitions == numPartitions && otherPartitioner.partitionFunc == partitionFunc;
            }

            return base.Equals(obj);
        }
    }
}
