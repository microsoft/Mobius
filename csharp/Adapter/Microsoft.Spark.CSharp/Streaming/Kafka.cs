// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Streaming
{
    /// <summary>
    /// Utils for Kafka input stream.
    /// </summary>
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
        public static DStream<Tuple<byte[], byte[]>> CreateStream(StreamingContext ssc, string zkQuorum, string groupId, IEnumerable<Tuple<string, int>> topics, IEnumerable<Tuple<string, string>> kafkaParams)
        {
            return CreateStream(ssc, zkQuorum, groupId, topics, kafkaParams, StorageLevelType.MEMORY_AND_DISK_SER_2);
        }

        /// <summary>
        /// Create an input stream that pulls messages from a Kafka Broker.
        /// </summary>
        /// <param name="ssc">Spark Streaming Context</param>
        /// <param name="zkQuorum">Zookeeper quorum (hostname:port,hostname:port,..).</param>
        /// <param name="groupId">The group id for this consumer.</param>
        /// <param name="topics">Dict of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread.</param>
        /// <param name="kafkaParams">Additional params for Kafka</param>
        /// <param name="storageLevelType">RDD storage level.</param>
        /// <returns>A DStream object</returns>
        public static DStream<Tuple<byte[], byte[]>> CreateStream(StreamingContext ssc, string zkQuorum, string groupId, IEnumerable<Tuple<string, int>> topics, IEnumerable<Tuple<string, string>> kafkaParams, StorageLevelType storageLevelType)
        {
            if (kafkaParams == null)
                kafkaParams = new List<Tuple<string, string>>();

            var kafkaParamsMap = kafkaParams.ToDictionary(x => x.Item1, x => x.Item2);

            if (!string.IsNullOrEmpty(zkQuorum))
                kafkaParamsMap["zookeeper.connect"] = zkQuorum;
            if (groupId != null)
                kafkaParamsMap["group.id"] = groupId;
            if (kafkaParamsMap.ContainsKey("zookeeper.connection.timeout.ms"))
                kafkaParamsMap["zookeeper.connection.timeout.ms"] = "10000";

            return new DStream<Tuple<byte[], byte[]>>(ssc.streamingContextProxy.KafkaStream(topics, kafkaParamsMap.Select(x => Tuple.Create(x.Key, x.Value)), storageLevelType), ssc);
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
        public static DStream<Tuple<byte[], byte[]>> CreateDirectStream(StreamingContext ssc, List<string> topics, IEnumerable<Tuple<string, string>> kafkaParams, IEnumerable<Tuple<string, long>> fromOffsets)
        {        
            int numPartitions = GetNumPartitionsFromConfig(ssc, topics, kafkaParams);
            if (numPartitions >= 0 ||
                ssc.SparkContext.SparkConf.SparkConfProxy.Get("spark.mobius.streaming.kafka.CSharpReader.enabled", "false").ToLower() == "true" ||
                ssc.SparkContext.SparkConf.SparkConfProxy.GetInt("spark.mobius.streaming.kafka.numReceivers", 0) > 0 ||
                topics.Any(topic => ssc.SparkContext.SparkConf.SparkConfProxy.GetInt("spark.mobius.streaming.kafka.maxMessagesPerTask." + topic, 0) > 0))
            {
                return new DStream<Tuple<byte[], byte[]>>(ssc.streamingContextProxy.DirectKafkaStreamWithRepartition(topics, kafkaParams, fromOffsets, numPartitions, null, null), ssc, SerializedMode.Pair);
            }
            return new DStream<Tuple<byte[], byte[]>>(ssc.streamingContextProxy.DirectKafkaStream(topics, kafkaParams, fromOffsets), ssc, SerializedMode.Pair);
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
        /// <param name="readFunc">user function to process the kafka data.</param>
        /// <returns>A DStream object</returns>
        public static DStream<T> CreateDirectStream<T>(StreamingContext ssc, List<string> topics, IEnumerable<Tuple<string, string>> kafkaParams, IEnumerable<Tuple<string, long>> fromOffsets, Func<int, IEnumerable<Tuple<byte[], byte[]>>, IEnumerable<T>> readFunc)
        {
            int numPartitions = GetNumPartitionsFromConfig(ssc, topics, kafkaParams);
            if (ssc.SparkContext.SparkConf.SparkConfProxy.GetInt("spark.mobius.streaming.kafka.numReceivers", 0) <= 0)
            {
                var dstream = new DStream<Tuple<byte[], byte[]>>(ssc.streamingContextProxy.DirectKafkaStreamWithRepartition(topics, kafkaParams, fromOffsets, numPartitions, null, null), ssc, SerializedMode.Pair);
                return dstream.MapPartitionsWithIndex(readFunc, true);
            }

            var mapPartitionsWithIndexHelper = new MapPartitionsWithIndexHelper<Tuple<byte[], byte[]>, T>(readFunc, true); 
            var transformHelper = new TransformHelper<Tuple<byte[], byte[]>, T>(mapPartitionsWithIndexHelper.Execute);
            var transformDynamicHelper = new TransformDynamicHelper<Tuple<byte[], byte[]>, T>(transformHelper.Execute);
            Func<double, RDD<dynamic>, RDD<dynamic>> func = transformDynamicHelper.Execute;
            var formatter = new BinaryFormatter();
            var stream = new MemoryStream();
            formatter.Serialize(stream, func);
            byte[] readFuncBytes = stream.ToArray();
            string serializationMode = SerializedMode.Pair.ToString();
            return new DStream<T>(ssc.streamingContextProxy.DirectKafkaStreamWithRepartition(topics, kafkaParams, fromOffsets, numPartitions, readFuncBytes, serializationMode), ssc);
        }

        /// <summary>
        /// create offset range from kafka messages when CSharpReader is enabled
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static OffsetRange GetOffsetRange(IEnumerable<Tuple<byte[], byte[]>> input)
        {
            int count = 2;
            int i = 0;
            var offsetRange = new Tuple<byte[], byte[]>[count];
            foreach (var message in input)
            {
                offsetRange[i++ % count] = message;
                if (i > count)
                    break;
            }

            if (i != count)
            {
                throw new ArgumentException("Expecting kafka OffsetRange metadata.");
            }

            var topicAndClusterId = SerDe.ToString(offsetRange[0].Item1);
            var topic = topicAndClusterId.Split(',')[0];
            var clusterId = topicAndClusterId.Split(',')[1];
            var partition = SerDe.ToInt(offsetRange[0].Item2);
            var fromOffset = SerDe.ReadLong(new MemoryStream(offsetRange[1].Item1));
            var untilOffset = SerDe.ReadLong(new MemoryStream(offsetRange[1].Item2));

            return new OffsetRange(topic, clusterId, partition, fromOffset, untilOffset);
        }

        /// <summary>
        /// topics should contain only one topic if choose to repartitions to a configured numPartitions
        /// TODO: move to scala and merge into DynamicPartitionKafkaRDD.getPartitions to remove above limitation
        /// </summary>
        /// <param name="ssc"></param>
        /// <param name="topics"></param>
        /// <param name="kafkaParams"></param>
        /// <returns></returns>
        private static int GetNumPartitionsFromConfig(StreamingContext ssc, List<string> topics, IEnumerable<Tuple<string, string>> kafkaParams)
        {
            if (topics == null || topics.Count == 0)
                return -1;

            var kafkaParamsMap = kafkaParams.ToDictionary(x => x.Item1, x => x.Item2);
            string clusterId = kafkaParamsMap.ContainsKey("cluster.id") ? "." + kafkaParamsMap["cluster.id"] : null;
            return ssc.SparkContext.SparkConf.SparkConfProxy.GetInt("spark.mobius.streaming.kafka.numPartitions." + topics[0] + clusterId, -1);
        }
    }
    
    /// <summary>
    /// Kafka offset range
    /// </summary>
    public class OffsetRange
    {
        private readonly string topic;
        private readonly string clusterId;
        private readonly int partition;
        private readonly long fromOffset;
        private readonly long untilOffset;

        /// <summary>
        /// Topic
        /// </summary>
        public string Topic { get { return topic; } }
        /// <summary>
        /// ClusterId
        /// </summary>
        public string ClusterId { get { return clusterId; } }
        /// <summary>
        /// Partition
        /// </summary>
        public int Partition { get { return partition; } }
        /// <summary>
        /// FromOffset
        /// </summary>
        public long FromOffset { get { return fromOffset; } }
        /// <summary>
        /// Until Offset
        /// </summary>
        public long UntilOffset { get { return untilOffset; } }

        internal OffsetRange(string topic, string clusterId, int partition, long fromOffset, long untilOffset)
        {
            this.topic = topic;
            this.clusterId = clusterId;
            this.partition = partition;
            this.fromOffset = fromOffset;
            this.untilOffset = untilOffset;
        }

        /// <summary>
        /// OffsetRange string format
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("Kafka OffsetRange: topic {0} cluster {1} partition {2} from {3} until {4}", topic, clusterId, partition, fromOffset, untilOffset);
        }
    }
}
