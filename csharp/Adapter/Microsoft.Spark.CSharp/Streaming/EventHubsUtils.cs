// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.Streaming
{
    /// <summary>
    /// Utility for creating streams from 
    /// </summary>
    public class EventHubsUtils
    {
        /// <summary>
        /// Create a unioned EventHubs stream that receives data from Microsoft Azure Eventhubs
        /// The unioned stream will receive message from all partitions of the EventHubs
        /// </summary>
        /// <param name="ssc">Streaming context</param>
        /// <param name="eventhubsParams"> Parameters for EventHubs.
        ///  Required parameters are:
        ///  "eventhubs.policyname": EventHubs policy name
        ///  "eventhubs.policykey": EventHubs policy key
        ///  "eventhubs.namespace": EventHubs namespace
        ///  "eventhubs.name": EventHubs name
        ///  "eventhubs.partition.count": Number of partitions
        ///  "eventhubs.checkpoint.dir": checkpoint directory on HDFS
        ///
        ///  Optional parameters are:
        ///  "eventhubs.consumergroup": EventHubs consumer group name, default to "\$default"
        ///  "eventhubs.filter.offset": Starting offset of EventHubs, default to "-1"
        ///  "eventhubs.filter.enqueuetime": Unix time, millisecond since epoch, default to "0"
        ///  "eventhubs.default.credits": default AMQP credits, default to -1 (which is 1024)
        ///  "eventhubs.checkpoint.interval": checkpoint interval in second, default to 10
        /// </param>
        /// <param name="storageLevelType">Storage level, by default it is MEMORY_ONLY</param>
        /// <returns>DStream with byte[] representing events from EventHub</returns>
        public static DStream<byte[]> CreateUnionStream(StreamingContext ssc, Dictionary<string, string> eventhubsParams, StorageLevelType storageLevelType = StorageLevelType.MEMORY_ONLY)
        {
            return new DStream<byte[]>(ssc.streamingContextProxy.EventHubsUnionStream(eventhubsParams, storageLevelType), ssc, SerializedMode.None);
        }
    }
}
