// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Core
{

    /// <summary>
    /// Wraps C#-based transformations that can be executed within a stage. It helps avoid unnecessary Ser/De of data between
    /// JVM & CLR to execute C# transformations and pipelines them
    /// </summary>
    /// <typeparam name="U"></typeparam>
    [Serializable]
    public class PipelinedRDD<U> : RDD<U>
    {
        internal Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> func; //using dynamic types to keep deserialization simple in worker side
        internal bool preservesPartitioning;

        //TODO - give generic types a better id
        public override RDD<U1> MapPartitionsWithIndex<U1>(Func<int, IEnumerable<U>, IEnumerable<U1>> newFunc, bool preservesPartitioningParam = false)
        {
            if (IsPipelinable())
            {
                var pipelinedRDD = new PipelinedRDD<U1>
                {
                    func = new MapPartitionsWithIndexHelper(new NewFuncWrapper<U, U1>(newFunc).Execute, func).Execute,
                    preservesPartitioning = preservesPartitioning && preservesPartitioningParam,
                    previousRddProxy = previousRddProxy,
                    prevSerializedMode = prevSerializedMode,

                    sparkContext = sparkContext,
                    rddProxy = null,
                    serializedMode = SerializedMode.Byte
                };
                return pipelinedRDD;
            }

            return base.MapPartitionsWithIndex(newFunc, preservesPartitioningParam);
        }

        [Serializable]
        private class NewFuncWrapper<I, O>
        {
            private Func<int, IEnumerable<I>, IEnumerable<O>> func;
            internal NewFuncWrapper(Func<int, IEnumerable<I>, IEnumerable<O>> f)
            {
                func = f;
            }

            internal IEnumerable<dynamic> Execute(int val, IEnumerable<dynamic> input)
            {
                return func(val, input.Cast<I>()).Cast<dynamic>();
            }
        }

        /// <summary>
        /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
        /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
        /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
        /// on the serializability of compiler generated types
        /// </summary>
        [Serializable]
        private class MapPartitionsWithIndexHelper
        {
            private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> newFunc;
            private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> prevFunc;
            internal MapPartitionsWithIndexHelper(Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> nFunc, Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> pFunc)
            {
                prevFunc = pFunc;
                newFunc = nFunc;
            }

            internal IEnumerable<dynamic> Execute(int split, IEnumerable<dynamic> input)
            {
                return newFunc(split, prevFunc(split, input));
            }
        }

        private bool IsPipelinable()
        {
            return !(isCached || isCheckpointed);
        }

        internal override IRDDProxy RddProxy
        {
            get
            {
                if (rddProxy == null)
                {
                    rddProxy = sparkContext.SparkContextProxy.CreateCSharpRdd(previousRddProxy,
                        SparkContext.BuildCommand(func, prevSerializedMode, bypassSerializer ? SerializedMode.None : serializedMode),
                        null, null, preservesPartitioning, sparkContext.broadcastVars, null);
                }
                return rddProxy;
            }
        }
    }
}
