// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Core
{
    public enum StorageLevelType
    {
        NONE,
        DISK_ONLY,
        DISK_ONLY_2,
        MEMORY_ONLY,
        MEMORY_ONLY_2,
        MEMORY_ONLY_SER,
        MEMORY_ONLY_SER_2,
        MEMORY_AND_DISK,
        MEMORY_AND_DISK_2,
        MEMORY_AND_DISK_SER,
        MEMORY_AND_DISK_SER_2,
        OFF_HEAP
    }
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
