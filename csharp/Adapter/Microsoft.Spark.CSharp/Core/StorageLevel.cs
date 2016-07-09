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
    /// Defines the type of storage levels
    /// </summary>
    public enum StorageLevelType
    {
        /// <summary>
        /// Not specified to use any storage
        /// </summary>
        NONE,
        /// <summary>
        /// Specifies to use disk only
        /// </summary>
        DISK_ONLY,
        /// <summary>
        /// Specifies to use disk only with 2 replicas
        /// </summary>
        DISK_ONLY_2,
        /// <summary>
        /// Specifies to use memory only
        /// </summary>
        MEMORY_ONLY,
        /// <summary>
        /// Specifies to use memory only 2 replicas
        /// </summary>
        MEMORY_ONLY_2,
        /// <summary>
        /// Specifies to use memory only in a serialized format
        /// </summary>
        MEMORY_ONLY_SER,
        /// <summary>
        /// Specifies to use memory only in a serialized format with 2 replicas
        /// </summary>
        MEMORY_ONLY_SER_2,
        /// <summary>
        /// Specifies to use disk and memory
        /// </summary>
        MEMORY_AND_DISK,
        /// <summary>
        /// Specifies to use disk and memory with 2 replicas
        /// </summary>
        MEMORY_AND_DISK_2,
        /// <summary>
        /// Specifies to use disk and memory in a serialized format
        /// </summary>
        MEMORY_AND_DISK_SER,
        /// <summary>
        /// Specifies to use disk and memory in a serialized format with 2 replicas
        /// </summary>
        MEMORY_AND_DISK_SER_2,
        /// <summary>
        /// Specifies to use off heap
        /// </summary>
        OFF_HEAP
    }

    /// <summary>
    /// Flags for controlling the storage of an RDD. Each StorageLevel records whether to use 
    /// memory, whether to drop the RDD to disk if it falls out of memory, whether to keep the 
    /// data in memory in a serialized format, and whether to replicate the RDD partitions 
    /// on multiple nodes.
    /// </summary>
    public class StorageLevel
    {
        internal static Dictionary<StorageLevelType, StorageLevel> storageLevel = new Dictionary<StorageLevelType, StorageLevel>
        {
            {StorageLevelType.NONE, new StorageLevel(false, false, false, false, 1)},
            {StorageLevelType.DISK_ONLY, new StorageLevel(true, false, false, false, 1)},
            {StorageLevelType.DISK_ONLY_2, new StorageLevel(true, false, false, false, 2)},
            {StorageLevelType.MEMORY_ONLY, new StorageLevel(false, true, false, true, 1)},
            {StorageLevelType.MEMORY_ONLY_2, new StorageLevel(false, true, false, true, 2)},
            {StorageLevelType.MEMORY_ONLY_SER, new StorageLevel(false, true, false, false, 1)},
            {StorageLevelType.MEMORY_ONLY_SER_2, new StorageLevel(false, true, false, false, 2)},
            {StorageLevelType.MEMORY_AND_DISK, new StorageLevel(true, true, false, true, 1)},
            {StorageLevelType.MEMORY_AND_DISK_2, new StorageLevel(true, true, false, true, 2)},
            {StorageLevelType.MEMORY_AND_DISK_SER, new StorageLevel(true, true, false, false, 1)},
            {StorageLevelType.MEMORY_AND_DISK_SER_2, new StorageLevel(true, true, false, false, 2)},
            {StorageLevelType.OFF_HEAP, new StorageLevel(false, false, true, false, 1)},
        };

        internal bool useDisk;
        internal bool useMemory;
        internal bool useOffHeap;
        internal bool deserialized;
        internal int replication;
        internal StorageLevel(bool useDisk, bool useMemory, bool useOffHeap, bool deserialized, int replication)
        {
            this.useDisk = useDisk;
            this.useMemory = useMemory;
            this.useOffHeap = useOffHeap;
            this.deserialized = deserialized;
            this.replication = replication;
        }

        /// <summary>
        /// Returns a readable string that represents the type
        /// </summary>
        /// <returns>A readable string</returns>
        public override string ToString()
        {
            return string.Format("{0}{1}{2}{3}{4} Replicated",
                useDisk ? "Disk " : null,
                useMemory ? "Memory " : null,
                useOffHeap ? "Tachyon " : null,
                deserialized ? "Deserialized " : "Serialized ",
                replication);
        }
    }
}
