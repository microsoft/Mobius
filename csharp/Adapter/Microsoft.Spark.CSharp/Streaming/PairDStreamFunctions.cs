// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Streaming
{
    /// <summary>
    /// operations only available to Tuple RDD
    /// </summary>
    public static class PairDStreamFunctions
    {
        /// <summary>
        /// Return a new DStream by applying ReduceByKey to each RDD.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="reduceFunc"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, V>> ReduceByKey<K, V>(this DStream<Tuple<K, V>> self, Func<V, V, V> reduceFunc, int numPartitions = 0)
        {
            var locallyCombined = self.MapPartitionsWithIndex(new GroupByMergeHelper<K, V>(reduceFunc).Execute, true);

            var shuffled = locallyCombined.PartitionBy(numPartitions);

            return shuffled.MapPartitionsWithIndex(new GroupByMergeHelper<K, V>(reduceFunc).Execute, true);
        }

        /// <summary>
        /// Return a new DStream by applying combineByKey to each RDD.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="C"></typeparam>
        /// <param name="self"></param>
        /// <param name="createCombiner"></param>
        /// <param name="mergeValue"></param>
        /// <param name="mergeCombiners"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, C>> CombineByKey<K, V, C>(
            this DStream<Tuple<K, V>> self,
            Func<C> createCombiner,
            Func<C, V, C> mergeValue,
            Func<C, C, C> mergeCombiners,
            int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.Transform<Tuple<K, C>>(new CombineByKeyHelper<K, V, C>(createCombiner, mergeValue, mergeCombiners, numPartitions).Execute);
        }

        /// <summary>
        /// Return a new DStream in which each RDD are partitioned by numPartitions.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, V>> PartitionBy<K, V>(this DStream<Tuple<K, V>> self, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.Transform<Tuple<K, V>>(new PartitionByHelper<K, V>(numPartitions).Execute);
        }

        /// <summary>
        /// Return a new DStream by applying a map function to the value of
        /// each key-value pairs in this DStream without changing the key.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <param name="self"></param>
        /// <param name="func"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, U>> MapValues<K, V, U>(this DStream<Tuple<K, V>> self, Func<V, U> func)
        {
            return self.Map(new MapValuesHelper<K, V, U>(func).Execute, true);
        }

        /// <summary>
        /// Return a new DStream by applying a flatmap function to the value
        /// of each key-value pairs in this DStream without changing the key.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <param name="self"></param>
        /// <param name="func"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, U>> FlatMapValues<K, V, U>(this DStream<Tuple<K, V>> self, Func<V, IEnumerable<U>> func)
        {
            return self.FlatMap(new FlatMapValuesHelper<K, V, U>(func).Execute, true);
        }

        /// <summary>
        /// Return a new DStream by applying groupByKey on each RDD.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, List<V>>> GroupByKey<K, V>(this DStream<Tuple<K, V>> self, int numPartitions = 0)
        {
            return self.Transform<Tuple<K, List<V>>>(new GroupByKeyHelper<K, V>(numPartitions).Execute);
        }

        /// <summary>
        /// Return a new DStream by applying 'cogroup' between RDDs of this DStream and `other` DStream.
        /// Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="W"></typeparam>
        /// <param name="self"></param>
        /// <param name="other"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, Tuple<List<V>, List<W>>>> GroupWith<K, V, W>(this DStream<Tuple<K, V>> self, DStream<Tuple<K, W>> other, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.TransformWith<Tuple<K, W>, Tuple<K, Tuple<List<V>, List<W>>>>(new GroupWithHelper<K, V, W>(numPartitions).Execute, other);
        }

        /// <summary>
        /// Return a new DStream by applying 'join' between RDDs of this DStream and `other` DStream.
        /// Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="W"></typeparam>
        /// <param name="self"></param>
        /// <param name="other"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, Tuple<V, W>>> Join<K, V, W>(this DStream<Tuple<K, V>> self, DStream<Tuple<K, W>> other, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.TransformWith<Tuple<K, W>, Tuple<K, Tuple<V, W>>>(new JoinHelper<K, V, W>(numPartitions).Execute, other);
        }

        /// <summary>
        /// Return a new DStream by applying 'left outer join' between RDDs of this DStream and `other` DStream.
        /// Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="W"></typeparam>
        /// <param name="self"></param>
        /// <param name="other"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, Tuple<V, Option<W>>>> LeftOuterJoin<K, V, W>(this DStream<Tuple<K, V>> self, DStream<Tuple<K, W>> other, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.TransformWith<Tuple<K, W>, Tuple<K, Tuple<V, Option<W>>>>(new LeftOuterJoinHelper<K, V, W>(numPartitions).Execute, other);
        }

        /// <summary>
        /// Return a new DStream by applying 'right outer join' between RDDs of this DStream and `other` DStream.
        /// Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="W"></typeparam>
        /// <param name="self"></param>
        /// <param name="other"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, Tuple<Option<V>, W>>> RightOuterJoin<K, V, W>(this DStream<Tuple<K, V>> self, DStream<Tuple<K, W>> other, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.TransformWith<Tuple<K, W>, Tuple<K, Tuple<Option<V>, W>>>(new RightOuterJoinHelper<K, V, W>(numPartitions).Execute, other);
        }

        /// <summary>
        /// Return a new DStream by applying 'full outer join' between RDDs of this DStream and `other` DStream.
        /// Hash partitioning is used to generate the RDDs with `numPartitions` partitions.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="W"></typeparam>
        /// <param name="self"></param>
        /// <param name="other"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, Tuple<Option<V>, Option<W>>>> FullOuterJoin<K, V, W>(this DStream<Tuple<K, V>> self, DStream<Tuple<K, W>> other, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.TransformWith<Tuple<K, W>, Tuple<K, Tuple<Option<V>, Option<W>>>>(new FullOuterJoinHelper<K, V, W>(numPartitions).Execute, other);
        }

        /// <summary>
        /// Return a new DStream by applying `GroupByKey` over a sliding window.
        /// Similar to `DStream.GroupByKey()`, but applies it over a sliding window.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="windowSeconds">width of the window; must be a multiple of this DStream's batching interval</param>
        /// <param name="slideSeconds">
        ///     sliding interval of the window (i.e., the interval after which
        ///     the new DStream will generate RDDs); must be a multiple of this
        ///     DStream's batching interval
        /// </param>
        /// <param name="numPartitions">Number of partitions of each RDD in the new DStream.</param>
        /// <returns></returns>
        public static DStream<Tuple<K, IEnumerable<V>>> GroupByKeyAndWindow<K, V>(this DStream<Tuple<K, V>> self,
            int windowSeconds, int slideSeconds, int numPartitions = 0)
        {
            var ls = self.MapValues(x => new List<V> { x });
            
            var grouped = ls.ReduceByKeyAndWindow(
                    (a, b) => { a.AddRange(b); return a; },
                    (a, b) => { a.RemoveRange(0, b.Count); return a; },
                    windowSeconds, slideSeconds, numPartitions);

            return grouped.MapValues(x => x.AsEnumerable());
        }

        /// <summary>
        /// Return a new DStream by applying incremental `reduceByKey` over a sliding window.
        ///
        /// The reduced value of over a new window is calculated using the old window's reduce value :
        ///  1. reduce the new values that entered the window (e.g., adding new counts)
        ///  2. "inverse reduce" the old values that left the window (e.g., subtracting old counts)
        ///
        /// `invFunc` can be None, then it will reduce all the RDDs in window, could be slower than having `invFunc`.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="reduceFunc">associative reduce function</param>
        /// <param name="invReduceFunc">inverse function of `reduceFunc`</param>
        /// <param name="windowSeconds">width of the window; must be a multiple of this DStream's batching interval</param>
        /// <param name="slideSeconds">sliding interval of the window (i.e., the interval after which the new DStream will generate RDDs); must be a multiple of this DStream's batching interval</param>
        /// <param name="numPartitions">number of partitions of each RDD in the new DStream.</param>
        /// <param name="filterFunc">function to filter expired key-value pairs; only pairs that satisfy the function are retained set this to null if you do not want to filter</param>
        /// <returns></returns>
        public static DStream<Tuple<K, V>> ReduceByKeyAndWindow<K, V>(this DStream<Tuple<K, V>> self,
            Func<V, V, V> reduceFunc,
            Func<V, V, V> invReduceFunc,
            int windowSeconds,
            int slideSeconds = 0,
            int numPartitions = 0,
            Func<Tuple<K, V>, bool> filterFunc = null)
        {
            self.ValidateWindowParam(windowSeconds, slideSeconds);

            if (slideSeconds <= 0)
                slideSeconds = self.SlideDuration;

            // dstream to be transformed by substracting old RDDs and adding new RDDs based on the window
            var reduced = self.ReduceByKey(reduceFunc, numPartitions);
            reduced.Cache();
                                                                                                                    
            var helper = new ReduceByKeyAndWindowHelper<K, V>(reduceFunc, invReduceFunc, numPartitions, filterFunc);
            // function to reduce the new values that entered the window (e.g., adding new counts)
            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> reduceF = helper.Reduce;

            MemoryStream stream = new MemoryStream();
            var formatter = new BinaryFormatter();
            formatter.Serialize(stream, reduceF);

            // function to "inverse reduce" the old values that left the window (e.g., subtracting old counts)
            MemoryStream invStream = null;
            if (invReduceFunc != null)
            {
                Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> invReduceF = helper.InvReduce;

                invStream = new MemoryStream();
                formatter.Serialize(invStream, invReduceF);
            }

            return new DStream<Tuple<K, V>>(
                SparkCLREnvironment.SparkCLRProxy.StreamingContextProxy.CreateCSharpReducedWindowedDStream(
                    reduced.DStreamProxy, 
                    stream.ToArray(),
                    invStream == null ? null : invStream.ToArray(),
                    windowSeconds,
                    slideSeconds,
                    reduced.serializedMode.ToString()), 
                self.streamingContext
            );
        }

        /// <summary>
        /// Return a new "state" DStream where the state for each key is updated by applying
        /// the given function on the previous state of the key and the new values of the key.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="self"></param>
        /// <param name="updateFunc">
        ///     State update function - (newValues, oldState) => newState
        ///     If this function returns None, then corresponding state key-value pair will be eliminated.
        /// </param>
        /// <param name="initialState">Initial state value of each key</param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>                                                          
        public static DStream<Tuple<K, S>> UpdateStateByKey<K, V, S>(this DStream<Tuple<K, V>> self,
            Func<IEnumerable<V>, S, S> updateFunc, RDD<Tuple<K, S>> initialState = null, 
            int numPartitions = 0)
        {
            return UpdateStateByKey<K, V, S>(self, new UpdateStateByKeyHelper<K, V, S>(updateFunc).Execute, initialState, numPartitions);
        }
        
        /// <summary>
        /// Return a new "state" DStream where the state for each key is updated by applying
        /// the given function on the previous state of the key and the new values of the key.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="self"></param>
        /// <param name="updateFunc">State update function - IEnumerable[K, [newValues, oldState]] => IEnumerable[K, newState]</param>
        /// <param name="initialState">Initial state value of each key</param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>   
        public static DStream<Tuple<K, S>> UpdateStateByKey<K, V, S>(this DStream<Tuple<K, V>> self,
            Func<IEnumerable<Tuple<K, Tuple<IEnumerable<V>, S>>>, IEnumerable<Tuple<K, S>>> updateFunc, RDD<Tuple<K, S>> initialState = null,
            int numPartitions = 0)
        {
            return UpdateStateByKey<K, V, S>(self, new MapPartitionsHelper<Tuple<K, Tuple<IEnumerable<V>, S>>, Tuple<K, S>>(updateFunc).Execute, initialState, numPartitions);
        }
        
        /// <summary>
        /// Return a new "state" DStream where the state for each key is updated by applying
        /// the given function on the previous state of the key and the new values of the key.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="self"></param>
        /// <param name="updateFunc">State update function - (pid, IEnumerable[K, [newValues, oldState]]) => IEnumerable[K, newState]</param>
        /// <param name="initialState">Initial state value of each key</param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<Tuple<K, S>> UpdateStateByKey<K, V, S>(this DStream<Tuple<K, V>> self,
            Func<int, IEnumerable<Tuple<K, Tuple<IEnumerable<V>, S>>>, IEnumerable<Tuple<K, S>>> updateFunc,
            RDD<Tuple<K, S>> initialState = null, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            // completes pipelinable dstream by adding the last pipelinable operation
            // before transforming to CSharpStateDStream so that UpdateStateByKey's 
            // parallel job covers all pipelinable operations before shuffling
            var ds = self.Transform(new AddShuffleKeyHelper<K, V>(numPartitions).Execute);

            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> func = new UpdateStateByKeysHelper<K, V, S>(updateFunc, initialState, numPartitions).Execute;

            var formatter = new BinaryFormatter();
            var stream = new MemoryStream();
            formatter.Serialize(stream, func);

            return new DStream<Tuple<K, S>>(SparkCLREnvironment.SparkCLRProxy.StreamingContextProxy.CreateCSharpStateDStream(
                    ds.DStreamProxy,
                    stream.ToArray(),
                    "CSharpStateDStream",
                    ds.serializedMode.ToString(),
                    ds.serializedMode.ToString()),
                self.streamingContext);
        }

        /// <summary>
        /// Return a new "state" DStream where the state for each key is updated by applying
        /// the given function on the previous state of the key and the new values of the key.
        /// </summary>
        public static MapWithStateDStream<K, V, S, M> MapWithState<K, V, S, M>(this DStream<Tuple<K, V>> self, StateSpec<K, V, S, M> stateSpec)
        {
            if (stateSpec.numPartitions <= 0)
            {
                stateSpec = stateSpec.NumPartitions(self.streamingContext.SparkContext.DefaultParallelism);
            }

            Func<double, RDD<dynamic>, RDD<dynamic>> prevFunc = self.Piplinable ? (self as TransformedDStream<Tuple<K, V>>).func : null;

            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> func = new MapWithStateHelper<K, V, S, M>(prevFunc, stateSpec).Execute;

            var formatter = new BinaryFormatter();
            var stream = new MemoryStream();
            formatter.Serialize(stream, func);

            var mapWithStateDStream = new DStream<MapWithStateRDDRecord<K, S, M>>(SparkCLREnvironment.SparkCLRProxy.StreamingContextProxy.CreateCSharpStateDStream(
                    self.Piplinable ? self.prevDStreamProxy : self.DStreamProxy,
                    stream.ToArray(),
                    "CSharpStateDStream",
                    self.serializedMode.ToString(),
                    (self.Piplinable ? self.prevSerializedMode : self.serializedMode).ToString()),
                self.streamingContext);

            DStream<M> mappedDataDStream = mapWithStateDStream.FlatMap(r => r.mappedData);
            DStream<Tuple<K, S>> snapshotsDStream = mapWithStateDStream.FlatMap(
                r => r.stateMap.Select(entry => new Tuple<K, S>(entry.Key, entry.Value.state)));

            return new MapWithStateDStream<K, V, S, M>(mappedDataDStream, snapshotsDStream);
        }
    }

    /// <summary>
    /// Following classes are defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not marked serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. These classes are to work around the limitation
    /// on the serializability of compiler generated types
    /// </summary>
    [Serializable]
    internal class GroupByMergeHelper<K, C>
    {
        private readonly Func<C, C, C> mergeCombiners;
        public GroupByMergeHelper(Func<C, C, C> mc)
        {
            mergeCombiners = mc;
        }

        public IEnumerable<Tuple<K, C>> Execute(int pid, IEnumerable<Tuple<K, C>> input)
        {
            return input.GroupBy(
                kvp => kvp.Item1,
                kvp => kvp.Item2,
                (k, v) => new Tuple<K, C>(k, v.Aggregate(mergeCombiners))
                );
        }
    }

    [Serializable]
    internal class CombineByKeyHelper<K, V, C>
    {
        private readonly Func<C> createCombiner;
        private readonly Func<C, V, C> mergeValue;
        private readonly Func<C, C, C> mergeCombiners;
        private readonly int numPartitions = 0;
        internal CombineByKeyHelper(Func<C> createCombiner, Func<C, V, C> mergeValue, Func<C, C, C> mergeCombiners, int numPartitions = 0)
        {
            this.createCombiner = createCombiner;
            this.mergeValue = mergeValue;
            this.mergeCombiners = mergeCombiners;
            this.numPartitions = numPartitions;
        }

        internal RDD<Tuple<K, C>> Execute(RDD<Tuple<K, V>> rdd)
        {
            return rdd.CombineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions);
        }
    }

    [Serializable]
    internal class PartitionByHelper<K, V>
    {
        private readonly int numPartitions = 0;
        internal PartitionByHelper(int numPartitions = 0)
        {
            this.numPartitions = numPartitions;
        }

        internal RDD<Tuple<K, V>> Execute(RDD<Tuple<K, V>> rdd)
        {
            return rdd.PartitionBy(numPartitions);
        }
    }

    [Serializable]
    internal class AddShuffleKeyHelper<K, V>
    {
        private readonly int numPartitions;
        internal AddShuffleKeyHelper(int numPartitions)
        {
            this.numPartitions = numPartitions;
        }

        internal RDD<byte[]> Execute(RDD<Tuple<K, V>> rdd)
        {
            var keyed = rdd.MapPartitionsWithIndex(new PairRDDFunctions.AddShuffleKeyHelper<K, V>(numPartitions).Execute, true);
            keyed.bypassSerializer = true;
            keyed.rddProxy = keyed.RddProxy;

            return keyed;
        }
    }

    [Serializable]
    internal class MapValuesHelper<K, V, U>
    {
        private readonly Func<V, U> func;
        internal MapValuesHelper(Func<V, U> f)
        {
            func = f;
        }

        internal Tuple<K, U> Execute(Tuple<K, V> kvp)
        {
            return new Tuple<K, U>(kvp.Item1, func(kvp.Item2));
        }
    }

    [Serializable]
    internal class FlatMapValuesHelper<K, V, U>
    {
        private readonly Func<V, IEnumerable<U>> func;
        internal FlatMapValuesHelper(Func<V, IEnumerable<U>> f)
        {
            func = f;
        }

        internal IEnumerable<Tuple<K, U>> Execute(Tuple<K, V> kvp)
        {
            return func(kvp.Item2).Select(v => new Tuple<K, U>(kvp.Item1, v));
        }
    }
    
    [Serializable]
    internal class GroupByKeyHelper<K, V>
    {
        private readonly int numPartitions = 0;
        internal GroupByKeyHelper(int numPartitions = 0)
        {
            this.numPartitions = numPartitions;
        }

        internal RDD<Tuple<K, List<V>>> Execute(RDD<Tuple<K, V>> rdd)
        {
            return rdd.GroupByKey(numPartitions);
        }
    }

    [Serializable]
    internal class GroupWithHelper<K, V, W>
    {
        private readonly int numPartitions;
        internal GroupWithHelper(int numPartitions)
        {
            this.numPartitions = numPartitions;
        }

        internal RDD<Tuple<K, Tuple<List<V>, List<W>>>> Execute(RDD<Tuple<K, V>> l, RDD<Tuple<K, W>> r)
        {
            return l.GroupWith<K, V, W>(r, numPartitions);
        }
    }

    [Serializable]
    internal class JoinHelper<K, V, W>
    {
        private readonly int numPartitions;
        internal JoinHelper(int numPartitions)
        {
            this.numPartitions = numPartitions;
        }

        internal RDD<Tuple<K, Tuple<V, W>>> Execute(RDD<Tuple<K, V>> l, RDD<Tuple<K, W>> r)
        {
            return l.Join<K, V, W>(r, numPartitions);
        }
    }

    [Serializable]
    internal class LeftOuterJoinHelper<K, V, W>
    {
        private readonly int numPartitions;
        internal LeftOuterJoinHelper(int numPartitions)
        {
            this.numPartitions = numPartitions;
        }

        internal RDD<Tuple<K, Tuple<V, Option<W>>>> Execute(RDD<Tuple<K, V>> l, RDD<Tuple<K, W>> r)
        {
            return l.LeftOuterJoin<K, V, W>(r, numPartitions);
        }
    }

    [Serializable]
    internal class RightOuterJoinHelper<K, V, W>
    {
        private readonly int numPartitions;
        internal RightOuterJoinHelper(int numPartitions)
        {
            this.numPartitions = numPartitions;
        }

        internal RDD<Tuple<K, Tuple<Option<V>, W>>> Execute(RDD<Tuple<K, V>> l, RDD<Tuple<K, W>> r)
        {
            return l.RightOuterJoin<K, V, W>(r, numPartitions);
        }
    }

    [Serializable]
    internal class FullOuterJoinHelper<K, V, W>
    {
        private readonly int numPartitions;
        internal FullOuterJoinHelper(int numPartitions)
        {
            this.numPartitions = numPartitions;
        }

        internal RDD<Tuple<K, Tuple<Option<V>, Option<W>>>> Execute(RDD<Tuple<K, V>> l, RDD<Tuple<K, W>> r)
        {
            return l.FullOuterJoin<K, V, W>(r, numPartitions);
        }
    }

    [Serializable]
    internal class ReduceByKeyAndWindowHelper<K, V>
    {
        private readonly Func<V, V, V> reduceFunc;
        private readonly Func<V, V, V> invReduceFunc;
        private readonly int numPartitions;
        private readonly Func<Tuple<K, V>, bool> filterFunc;

        internal ReduceByKeyAndWindowHelper(Func<V, V, V> reduceF, 
            Func<V, V, V> invReduceF, 
            int numPartitions, 
            Func<Tuple<K, V>, bool> filterF)
        {
            reduceFunc = reduceF;
            invReduceFunc = invReduceF;
            this.numPartitions = numPartitions;
            filterFunc = filterF;
        }

        internal RDD<dynamic> Reduce(double t, RDD<dynamic> a, RDD<dynamic> b)
        {
            b.partitioner = new Partitioner(numPartitions, null);
            var r = b.ConvertTo<Tuple<K, V>>();
            if (a != null)
            {
                a.partitioner = b.partitioner;
                r = a.ConvertTo<Tuple<K, V>>().Union(r);
            }
            r = r.ReduceByKey<K, V>(reduceFunc, numPartitions);
            if (filterFunc != null)
                r.Filter(filterFunc);
            return r.ConvertTo<dynamic>();
        }

        internal RDD<dynamic> InvReduce(double t, RDD<dynamic> a, RDD<dynamic> b)
        {     
            a.partitioner = b.partitioner = new Partitioner(numPartitions, null);
            var rddb = b.ConvertTo<Tuple<K, V>>().ReduceByKey<K, V>(reduceFunc, numPartitions);
            var rdda = a.ConvertTo<Tuple<K, V>>();
            var joined = rdda.Join<K, V, V>(rddb, numPartitions);
            var r = joined.MapValues<K, Tuple<V, V>, V>(kv => kv.Item2 != null ? invReduceFunc(kv.Item1, kv.Item2) : kv.Item1);
            return r.ConvertTo<dynamic>();
        }
    }
    
    [Serializable]
    internal class UpdateStateByKeyHelper<K, V, S>
    {
        private readonly Func<IEnumerable<V>, S, S> func;

        internal UpdateStateByKeyHelper(Func<IEnumerable<V>, S, S> f)
        {
            func = f;
        }

        internal IEnumerable<Tuple<K, S>> Execute(IEnumerable<Tuple<K, Tuple<IEnumerable<V>, S>>> input)
        {
            return input.Select(x => new Tuple<K, S>(x.Item1, func(x.Item2.Item1, x.Item2.Item2)));
        }
    }

    [Serializable]
    internal class UpdateStateByKeysHelper<K, V, S>
    {           
        private readonly Func<int, IEnumerable<Tuple<K, Tuple<IEnumerable<V>, S>>>, IEnumerable<Tuple<K, S>>> func;
        private readonly RDD<Tuple<K, S>> initialState;
        private readonly int numPartitions;
        internal UpdateStateByKeysHelper(
            Func<int, IEnumerable<Tuple<K, Tuple<IEnumerable<V>, S>>>, IEnumerable<Tuple<K, S>>> f, 
            RDD<Tuple<K, S>> initialState, int numPartitions)
        {
            func = f;
            this.initialState = initialState;
            this.numPartitions = numPartitions;
        }

        internal RDD<dynamic> Execute(double t, RDD<dynamic> stateRDD, RDD<dynamic> valuesRDD)
        {
            RDD<Tuple<K, S>> state = null;
            RDD<Tuple<K, Tuple<IEnumerable<V>, S>>> g = null;

            // call into scala side partitionBy directly since AddShuffleKey already applied
            var values = new RDD<Tuple<K, V>>(valuesRDD.sparkContext.SparkContextProxy.CreatePairwiseRDD(valuesRDD.rddProxy, numPartitions, 0), valuesRDD.sparkContext);
            values.partitioner = new Partitioner(numPartitions, null);

            if (stateRDD == null)
            {
                if (initialState != null)
                {
                    if (initialState.sparkContext == null)
                    {
                        initialState.sparkContext = valuesRDD.sparkContext;
                    }
                    stateRDD = initialState.ConvertTo<dynamic>();
                }
            }

            if (stateRDD == null)
            {
                g = values.GroupByKey(numPartitions).MapValues(x => new Tuple<IEnumerable<V>, S>(new List<V>(x), default(S)));
            }
            else
            {                         
                state = stateRDD.ConvertTo<Tuple<K, S>>();
                state.partitioner = values.partitioner;
                g = state.GroupWith(values, numPartitions).MapValues(x => new Tuple<IEnumerable<V>, S>(new List<V>(x.Item2), x.Item1.Count > 0 ? x.Item1[0] : default(S)));
            }

            state = g.MapPartitionsWithIndex((pid, iter) => func(pid, iter), true).Filter(x => x.Item2 != null);

            return state.ConvertTo<dynamic>();
        }
    }
}
