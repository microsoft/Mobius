// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.Streaming
{
    public class KafkaUtils
    {
        /// <summary>
        /// Create an input stream that pulls messages from a Kafka Broker.
        /// </summary>
        /// <param name="ssc">Spark Streaming Context</param>
        /// <param name="zkQuorum">Zookeeper quorum (hostname:port,hostname:port,..).</param>
        /// <param name="groupId">The group id for this consumer.</param>
        /// <param name="topics">Dict of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread.</param>
        /// <param name="kafkaParams">Additional params for Kafka</param>
        /// <returns>A DStream object</returns>
        public static DStream<KeyValuePair<byte[], byte[]>> CreateStream(StreamingContext ssc, string zkQuorum, string groupId, Dictionary<string, int> topics, Dictionary<string, string> kafkaParams)
        {
            return CreateStream(ssc, zkQuorum, groupId, topics, kafkaParams, StorageLevelType.MEMORY_AND_DISK_SER_2);
        }

        /// <summary>
        /// Create an input stream that pulls messages from a Kafka Broker.
        /// </summary>
        /// <param name="zkQuorum">Zookeeper quorum (hostname:port,hostname:port,..).</param>
        /// <param name="groupId">The group id for this consumer.</param>
        /// <param name="topics">Dict of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread.</param>
        /// <param name="kafkaParams">Additional params for Kafka</param>
        /// <param name="storageLevelType">RDD storage level.</param>
        /// <returns>A DStream object</returns>
        public static DStream<KeyValuePair<byte[], byte[]>> CreateStream(StreamingContext ssc, string zkQuorum, string groupId, Dictionary<string, int> topics, Dictionary<string, string> kafkaParams, StorageLevelType storageLevelType)
        {
            if (kafkaParams == null)
                kafkaParams = new Dictionary<string, string>();

            if (!string.IsNullOrEmpty(zkQuorum))
                kafkaParams["zookeeper.connect"] = zkQuorum;
            if (groupId != null)
                kafkaParams["group.id"] = groupId;
            if (kafkaParams.ContainsKey("zookeeper.connection.timeout.ms"))
                kafkaParams["zookeeper.connection.timeout.ms"] = "10000";

            return new DStream<KeyValuePair<byte[], byte[]>>(ssc.streamingContextProxy.KafkaStream(topics, kafkaParams, storageLevelType), ssc);
        }

        /// <summary>
        /// Create an input stream that directly pulls messages from a Kafka Broker and specific offset.
        /// 
        /// This is not a receiver based Kafka input stream, it directly pulls the message from Kafka
        /// in each batch duration and processed without storing.
        /// 
        /// This does not use Zookeeper to store offsets. The consumed offsets are tracked
        /// by the stream itself. For interoperability with Kafka monitoring tools that depend on
        /// Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
        /// You can access the offsets used in each batch from the generated RDDs (see
        /// [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
        /// To recover from driver failures, you have to enable checkpointing in the StreamingContext.
        /// The information on consumed offset can be recovered from the checkpoint.
        /// See the programming guide for details (constraints, etc.).
        /// 
        /// </summary>
        /// <param name="ssc">Spark Streaming Context</param>
        /// <param name="topics">list of topic_name to consume.</param>
        /// <param name="kafkaParams">
        ///     Additional params for Kafka. Requires "metadata.broker.list" or "bootstrap.servers" to be set
        ///     with Kafka broker(s) (NOT zookeeper servers), specified in host1:port1,host2:port2 form.        
        /// </param>
        /// <param name="fromOffsets">Per-topic/partition Kafka offsets defining the (inclusive) starting point of the stream.</param>
        /// <returns>A DStream object</returns>
        public static DStream<KeyValuePair<byte[], byte[]>> CreateDirectStream(StreamingContext ssc, List<string> topics, Dictionary<string, string> kafkaParams, Dictionary<string, long> fromOffsets)
        {
            return new DStream<KeyValuePair<byte[], byte[]>>(ssc.streamingContextProxy.DirectKafkaStream(topics, kafkaParams, fromOffsets), ssc, SerializedMode.Pair);
        }
    }
}
