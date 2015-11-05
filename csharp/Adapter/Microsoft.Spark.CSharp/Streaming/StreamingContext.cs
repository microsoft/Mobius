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
    /// <summary>
    /// Main entry point for Spark Streaming functionality. 
    /// It provides methods used to create <see cref="DStream{T}"/>s from various input sources. 
    /// It can be either created by providing a Spark master URL and an appName, 
    /// or from a org.apache.spark.SparkConf configuration (see core Spark documentation), 
    /// or from an existing org.apache.spark.SparkContext.
    /// The associated SparkContext can be accessed using `context.sparkContext`. 
    /// After creating and transforming DStreams, the streaming computation can be started 
    /// and stopped using `context.start()` and `context.stop()`, respectively.
    /// Methods `context.awaitTermination()` allows the current thread to wait 
    /// for the termination of the context by `stop()` or by an exception.
    /// </summary>
    /// <seealso cref="DStream{T}"/>
    public class StreamingContext
    {
        public StreamingContext(SparkContext sparkContext, long durationMS)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Set each DStreams in this context to remember RDDs it generated in the last given duration.
        /// DStreams remember RDDs only for a limited duration of time and releases them for garbage
        /// collection. This method allows the developer to specify how long to remember the RDDs (
        /// if the developer wishes to query old data outside the DStream computation).
        /// </summary>
        /// <param name="durationMS">Minimum duration that each DStream should remember its RDDs</param>
        public void Remember(long durationMS)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Set the context to periodically checkpoint the DStream operations for driver
        /// fault-tolerance.
        /// </summary>
        /// <param name="directory">HDFS-compatible directory where the checkpoint data will be reliably stored.
        /// Note that this must be a fault-tolerant file system like HDFS.</param>
        public void Checkpoint(string directory)
        {
            throw new NotImplementedException();
        }

        public DStream<string> TextFileStream(string directory)
        {
            throw new NotImplementedException();
        }
    }
}
