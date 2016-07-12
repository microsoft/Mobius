// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Collections.Generic;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy.Ipc;

namespace Microsoft.Spark.CSharp.Streaming
{
    /// <summary>
    /// Utils for csharp input stream.
    /// </summary>
    public class CSharpInputDStreamUtils
    {
        /// <summary>
        /// Create an input stream that user can control the data injection by C# code
        /// </summary>
        /// <param name="ssc">Spark Streaming Context</param>
        /// <param name="func">
        /// function provided by user to inject data to the DStream.
        /// it should return a RDD for each batch interval
        /// </param>
        /// <returns>A DStream object</returns>
        public static DStream<T> CreateStream<T>(StreamingContext ssc, Func<double, RDD<T>> func)
        {
            Func<double, RDD<dynamic>, RDD<dynamic>> csharpFunc = new CSharpInputDStreamTransformRDDHelper<T>(func).Execute;
            var formatter = new BinaryFormatter();
            var stream = new MemoryStream();
            formatter.Serialize(stream, csharpFunc);

            var dstreamProxy = ssc.streamingContextProxy.CreateCSharpInputDStream(stream.ToArray(), SerializedMode.Byte.ToString());
            return new DStream<T>(dstreamProxy, ssc, SerializedMode.Byte);
        }

        /// <summary>
        /// Create an input stream that user can control the data injection by C# code
        /// </summary>
        /// <param name="ssc">Spark Streaming Context</param>
        /// <param name="numPartitions">number of partitions</param>
        /// <param name="func">
        /// function provided by user to inject data to the DStream.
        /// it has two input parameters: time and partitionIndex
        /// it should return IEnumerable of injected data
        /// </param>
        /// <returns>A DStream object</returns>
        public static DStream<T> CreateStream<T>(StreamingContext ssc, int numPartitions, Func<double, int, IEnumerable<T>> func)
        {
            Func<double, RDD<T>> generateRDDFunc = new CSharpInputDStreamGenerateRDDHelper<T>(numPartitions, func).Execute;
            return CreateStream<T>(ssc, generateRDDFunc);
        }
    }

    /// <summary>
    /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
    /// on the serializability of compiler generated types
    /// </summary>
    [Serializable]
    internal class CSharpInputDStreamTransformRDDHelper<T>
    {
        private Func<double, RDD<T>> func;

        public CSharpInputDStreamTransformRDDHelper(Func<double, RDD<T>> func)
        {
            this.func = func;
        }

        internal RDD<dynamic> Execute(double t, RDD<dynamic> rdd)
        {
            return func(t).ConvertTo<dynamic>();
        }
    }

    /// <summary>
    /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
    /// on the serializability of compiler generated types
    /// </summary>
    [Serializable]
    internal class CSharpInputDStreamMapPartitionWithIndexHelper<T>
    {
        Func<double, int, IEnumerable <T>> func;
        double time;

        public CSharpInputDStreamMapPartitionWithIndexHelper(double time, Func<double, int, IEnumerable<T>> func)
        {
            this.time = time;
            this.func = func;
        }

        internal IEnumerable<T> Execute(int partitionIndex, IEnumerable<int> input)
        {
            return func(time, partitionIndex);
        }
    }

    /// <summary>
    /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
    /// on the serializability of compiler generated types
    /// </summary>
    [Serializable]
    internal class CSharpInputDStreamGenerateRDDHelper<T>
    {
        private Func<double, int, IEnumerable<T>> func;
        private int numPartitions;

        public CSharpInputDStreamGenerateRDDHelper(int numPartitions, Func<double, int, IEnumerable<T>> func)
        {
            this.numPartitions = numPartitions;
            this.func = func;
        }

        internal RDD<T> Execute(double t)
        {
            var sc = SparkContext.GetActiveSparkContext();
            int[] array = new int[numPartitions];
            var initialRdd = sc.Parallelize(array.AsEnumerable(), numPartitions);
            return initialRdd.MapPartitionsWithIndex<T>(new CSharpInputDStreamMapPartitionWithIndexHelper<T>(t, func).Execute, true);
        }
    }
}
