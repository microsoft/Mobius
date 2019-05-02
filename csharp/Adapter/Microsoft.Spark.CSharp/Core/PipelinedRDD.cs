// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Diagnostics;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;
using System.Linq.Expressions;
using SerializationHelpers.Data;
using SerializationHelpers.Extensions;

namespace Microsoft.Spark.CSharp.Core
{

    /// <summary>
    /// Wraps C#-based transformations that can be executed within a stage. It helps avoid unnecessary Ser/De of data between
    /// JVM and CLR to execute C# transformations and pipelines them
    /// </summary>
    /// <typeparam name="U"></typeparam>
    [Serializable]
    [DataContract]
    public class PipelinedRDD<U> : RDD<U>
    {
        [DataMember]
        internal CSharpWorkerFunc workerFunc;
        [DataMember]
        internal bool preservesPartitioning;

        public PipelinedRDD() { }
        //TODO - give generic types a better id
        /// <summary>
        /// Return a new RDD by applying a function to each partition of this RDD,
        /// while tracking the index of the original partition.
        /// </summary>
        /// <typeparam name="U1">The element type</typeparam>
        /// <param name="newFunc">The function to be applied to each partition</param>
        /// <param name="preservesPartitioningParam">Indicates if it preserves partition parameters</param>
        /// <returns>A new RDD</returns>
        public override RDD<U1> MapPartitionsWithIndex<U1>(Expression<Func<int, IEnumerable<U>, IEnumerable<U1>>> newFunc, bool preservesPartitioningParam = false)
        {
            if (IsPipelinable())
            {
                //var newExpressionData = newFunc.ToExpressionData();
                //var workerExpressionData = workerFunc.ExpressionData;
                var mapPartition = new MapPartitionsWithIndexHelper<U, U1>(newFunc, workerFunc.Expr);
                CSharpWorkerFunc newWorkerFunc = new CSharpWorkerFunc((mapPartionsX, mapPartionsY) =>
                    mapPartition.Execute(mapPartionsX, mapPartionsY), workerFunc.StackTrace);

                var pipelinedRDD = new PipelinedRDD<U1>
                {
                    workerFunc = newWorkerFunc,
                    preservesPartitioning = preservesPartitioning && preservesPartitioningParam,
                    previousRddProxy = this.previousRddProxy,
                    prevSerializedMode = this.prevSerializedMode,
                    sparkContext = this.sparkContext,
                    rddProxy = null,
                    serializedMode = SerializedMode.Byte,
                    partitioner = preservesPartitioning ? partitioner : null
                };
                return pipelinedRDD;
            }

            return base.MapPartitionsWithIndex(newFunc, preservesPartitioningParam);
        }

        /// <summary>
        /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
        /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
        /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
        /// on the serializability of compiler generated types
        /// </summary>
        [Serializable]
        [DataContract]
        private class MapPartitionsWithIndexHelper<I, O>
        {
            //private readonly Func<int, IEnumerable<I>, IEnumerable<O>> newFunc;
            //private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> prevFunc;
            [DataMember]
            private LinqExpressionData newFuncExpressionData;
            [DataMember]
            private LinqExpressionData prevFuncExpressionData;
            public MapPartitionsWithIndexHelper() { }
            internal MapPartitionsWithIndexHelper(Expression<Func<int, IEnumerable<I>, IEnumerable<O>>> nFunc, Expression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>> pFunc)
            {
                prevFuncExpressionData = pFunc.ToExpressionData();
                newFuncExpressionData = nFunc.ToExpressionData();
            }

            //internal MapPartitionsWithIndexHelper(LinqExpressionData newFuncExpressionData, LinqExpressionData prevFuncExpressionData)
            //{
            //    this.prevFuncExpressionData = prevFuncExpressionData;
            //    this.newFuncExpressionData = newFuncExpressionData;
            //}

            internal IEnumerable<dynamic> Execute(int split, IEnumerable<dynamic> input)
            {
                var newFunc = newFuncExpressionData.ToFunc<Func<int, IEnumerable<I>, IEnumerable<O>>>();
                var prevFunc = prevFuncExpressionData.ToFunc<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>>();
                return newFunc(split, prevFunc(split, input).Cast<I>()).Cast<dynamic>();
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
                        SparkContext.BuildCommand(workerFunc, prevSerializedMode, bypassSerializer ? SerializedMode.None : serializedMode),
                        null, null, preservesPartitioning, null, null);
                }
                return rddProxy;
            }
        }
    }
}
