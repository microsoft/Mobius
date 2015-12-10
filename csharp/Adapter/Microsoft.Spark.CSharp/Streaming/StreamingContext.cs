// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Streaming
{
    /**
     * Main entry point for Spark Streaming functionality. It provides methods used to create
     * [[org.apache.spark.streaming.dstream.DStream]]s from various input sources. It can be either
     * created by providing a Spark master URL and an appName, or from a org.apache.spark.SparkConf
     * configuration (see core Spark documentation), or from an existing org.apache.spark.SparkContext.
     * The associated SparkContext can be accessed using `context.sparkContext`. After
     * creating and transforming DStreams, the streaming computation can be started and stopped
     * using `context.start()` and `context.stop()`, respectively.
     * `context.awaitTermination()` allows the current thread to wait for the termination
     * of the context by `stop()` or by an exception.
     */
    public class StreamingContext
    {
        internal readonly IStreamingContextProxy streamingContextProxy;
        private SparkContext sparkContext;
        internal SparkContext SparkContext
        {
            get
            {
                if (sparkContext == null)
                {
                    sparkContext = streamingContextProxy.SparkContext;
                }
                return sparkContext;
            }
        }

        /// <summary>
        /// when created from checkpoint
        /// </summary>
        /// <param name="streamingContextProxy"></param>
        private StreamingContext(IStreamingContextProxy streamingContextProxy)
        {
            this.streamingContextProxy = streamingContextProxy;
        }

        public StreamingContext(SparkContext sparkContext, long durationMs)
        {
            this.sparkContext = sparkContext;
            this.streamingContextProxy = SparkCLREnvironment.SparkCLRProxy.CreateStreamingContext(sparkContext, durationMs);
        }

        /// <summary>
        /// Either recreate a StreamingContext from checkpoint data or create a new StreamingContext.
        /// If checkpoint data exists in the provided `checkpointPath`, then StreamingContext will be
        /// recreated from the checkpoint data. If the data does not exist, then the provided setupFunc
        /// will be used to create a JavaStreamingContext.
        /// </summary>
        /// <param name="checkpointPath">Checkpoint directory used in an earlier JavaStreamingContext program</param>
        /// <param name="creatingFunc">Function to create a new JavaStreamingContext and setup DStreams</param>
        /// <returns></returns>
        public static StreamingContext GetOrCreate(string checkpointPath, Func<StreamingContext> creatingFunc)
        {
            if (!SparkCLREnvironment.SparkCLRProxy.CheckpointExists(checkpointPath))
            {
                var ssc = creatingFunc();
                ssc.Checkpoint(checkpointPath);
                return ssc;
            }
            
            return new StreamingContext(SparkCLREnvironment.SparkCLRProxy.CreateStreamingContext(checkpointPath));
        }

        public void Start()
        {
            this.streamingContextProxy.Start();
        }

        public void Stop()
        {
            this.streamingContextProxy.Stop();
        }

        /// <summary>
        /// Set each DStreams in this context to remember RDDs it generated in the last given duration.
        /// DStreams remember RDDs only for a limited duration of time and releases them for garbage
        /// collection. This method allows the developer to specify how long to remember the RDDs (
        /// if the developer wishes to query old data outside the DStream computation).
        /// </summary>
        /// <param name="durationMs">Minimum duration that each DStream should remember its RDDs</param>
        public void Remember(long durationMs)
        {
            this.streamingContextProxy.Remember(durationMs);
        }

        /// <summary>
        /// Set the context to periodically checkpoint the DStream operations for driver
        /// fault-tolerance.
        /// </summary>
        /// <param name="directory">
        ///     HDFS-compatible directory where the checkpoint data will be reliably stored. 
        ///     Note that this must be a fault-tolerant file system like HDFS for
        /// </param>
        public void Checkpoint(string directory)
        {
            this.streamingContextProxy.Checkpoint(directory);
        }

        /// <summary>
        /// Create an input from TCP source hostname:port. Data is received using
        /// a TCP socket and receive byte is interpreted as UTF8 encoded ``\\n`` delimited
        /// lines.
        /// </summary>
        /// <param name="hostname">Hostname to connect to for receiving data</param>
        /// <param name="port">Port to connect to for receiving data</param>
        /// <param name="storageLevelType">Storage level to use for storing the received objects</param>
        /// <returns></returns>
        public DStream<string> SocketTextStream(string hostname, int port, StorageLevelType storageLevelType = StorageLevelType.MEMORY_AND_DISK_SER_2)
        {
            return new DStream<string>(streamingContextProxy.SocketTextStream(hostname, port, storageLevelType), this, SerializedMode.String);
        }

        /// <summary>
        /// Create an input stream that monitors a Hadoop-compatible file system
        /// for new files and reads them as text files. Files must be wrriten to the
        /// monitored directory by "moving" them from another location within the same
        /// file system. File names starting with . are ignored.
        /// </summary>
        /// <param name="directory"></param>
        /// <returns></returns>
        public DStream<string> TextFileStream(string directory)
        {
            return new DStream<string>(streamingContextProxy.TextFileStream(directory), this, SerializedMode.String);
        }

        /// <summary>
        /// Create an input stream that pulls messages from a Kafka Broker.
        /// </summary>
        /// <param name="zkQuorum">Zookeeper quorum (hostname:port,hostname:port,..).</param>
        /// <param name="groupId">The group id for this consumer.</param>
        /// <param name="topics">Dict of (topic_name -> numPartitions) to consume. Each partition is consumed in its own thread.</param>
        /// <param name="kafkaParams">Additional params for Kafka</param>
        /// <returns>A DStream object</returns>
        public DStream<KeyValuePair<byte[], byte[]>> KafkaStream(string zkQuorum, string groupId, Dictionary<string, int> topics, Dictionary<string, string> kafkaParams)
        {
            return this.KafkaStream(zkQuorum, groupId, topics, kafkaParams, StorageLevelType.MEMORY_AND_DISK_SER_2);
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
        public DStream<KeyValuePair<byte[], byte[]>> KafkaStream(string zkQuorum, string groupId, Dictionary<string, int> topics, Dictionary<string, string> kafkaParams, StorageLevelType storageLevelType)
        {
            if (kafkaParams == null)
                kafkaParams = new Dictionary<string, string>();

            if (!string.IsNullOrEmpty(zkQuorum))
                kafkaParams["zookeeper.connect"] = zkQuorum;
            if (groupId != null)
                kafkaParams["group.id"] = groupId;
            if (kafkaParams.ContainsKey("zookeeper.connection.timeout.ms"))
                kafkaParams["zookeeper.connection.timeout.ms"] = "10000";

            return new DStream<KeyValuePair<byte[], byte[]>>(this.streamingContextProxy.KafkaStream(topics, kafkaParams, storageLevelType), this);
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
        /// 
        /// To recover from driver failures, you have to enable checkpointing in the StreamingContext.
        /// The information on consumed offset can be recovered from the checkpoint.
        /// See the programming guide for details (constraints, etc.).
        /// 
        /// </summary>
        /// <param name="topics">list of topic_name to consume.</param>
        /// <param name="kafkaParams">
        ///     Additional params for Kafka. Requires "metadata.broker.list" or "bootstrap.servers" to be set
        ///     with Kafka broker(s) (NOT zookeeper servers), specified in host1:port1,host2:port2 form.        
        /// </param>
        /// <param name="fromOffsets">Per-topic/partition Kafka offsets defining the (inclusive) starting point of the stream.</param>
        /// <returns>A DStream object</returns>
        public DStream<KeyValuePair<byte[], byte[]>> DirectKafkaStream(List<string> topics, Dictionary<string, string> kafkaParams, Dictionary<string, long> fromOffsets)
        {
            return new DStream<KeyValuePair<byte[], byte[]>>(this.streamingContextProxy.DirectKafkaStream(topics, kafkaParams, fromOffsets), this, SerializedMode.Pair);
        }

        /// <summary>
        /// Wait for the execution to stop.
        /// </summary>
        public void AwaitTermination()
        {
            this.streamingContextProxy.AwaitTermination();
        }

        /// <summary>
        /// Wait for the execution to stop.
        /// </summary>
        /// <param name="timeout">time to wait in seconds</param>
        public void AwaitTermination(int timeout)
        {
            this.streamingContextProxy.AwaitTermination(timeout);
        }

        /// <summary>
        /// Create a new DStream in which each RDD is generated by applying
        /// a function on RDDs of the DStreams. The order of the JavaRDDs in
        /// the transform function parameter will be the same as the order
        /// of corresponding DStreams in the list.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dstreams"></param>
        /// <param name="transformFunc"></param>
        /// <returns></returns>
        public DStream<T> Transform<T>(IEnumerable<DStream<T>> dstreams, Func<IEnumerable<RDD<T>>, long, RDD<T>> transformFunc)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Create a unified DStream from multiple DStreams of the same
        /// type and same slide duration.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dstreams"></param>
        /// <returns></returns>
        public DStream<T> Union<T>(params DStream<T>[] dstreams)
        {
            if (dstreams == null || dstreams.Length == 0)
                throw new ArgumentException("should have at least one DStream to union");
            
            if (dstreams.Length == 1)
                return dstreams[0];
            
            if (dstreams.Select(x => x.serializedMode).Distinct().Count() > 1)
                throw new ArgumentException("All DStreams should have same serializer");
            
            if (dstreams.Select(x => x.SlideDuration).Distinct().Count() > 1)
                throw new ArgumentException("All DStreams should have same slide duration");
            
            var first = dstreams.First();
            var rest = dstreams.Skip(1);
            
            return new DStream<T>(streamingContextProxy.Union(first.dstreamProxy, rest.Select(x => x.dstreamProxy).ToArray()), this, first.serializedMode);
        }
    }
}
