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
            streamingContextProxy = SparkCLREnvironment.SparkCLRProxy.CreateStreamingContext(sparkContext, durationMs);
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
            streamingContextProxy.Start();
        }

        public void Stop()
        {
            streamingContextProxy.Stop();
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
            streamingContextProxy.Remember(durationMs);
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
            streamingContextProxy.Checkpoint(directory);
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
        /// Wait for the execution to stop.
        /// </summary>
        public void AwaitTermination()
        {
            streamingContextProxy.AwaitTermination();
        }

        /// <summary>
        /// Wait for the execution to stop.
        /// </summary>
        /// <param name="timeout">time to wait in seconds</param>
        public void AwaitTermination(int timeout)
        {
            streamingContextProxy.AwaitTermination(timeout);
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
