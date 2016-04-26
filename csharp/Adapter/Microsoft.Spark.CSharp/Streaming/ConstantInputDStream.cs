// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy.Ipc;

namespace Microsoft.Spark.CSharp.Streaming
{
    /// <summary>
    /// An input stream that always returns the same RDD on each timestep. Useful for testing.
    /// </summary>
    public class ConstantInputDStream<T> : DStream<T>
    {
        /// <summary>
        /// Construct a ConstantInputDStream instance.
        /// </summary>
        public ConstantInputDStream(RDD<T> rdd, StreamingContext ssc)
        {
            if (rdd == null)
            {
                throw new ArgumentNullException("Parameter rdd null is illegal, which will lead to NPE in the following transformation");
            }

            dstreamProxy = ssc.streamingContextProxy.CreateConstantInputDStream(rdd.RddProxy);
            streamingContext = ssc;
            serializedMode = SerializedMode.Byte;
        }
    }
}
