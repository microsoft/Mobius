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
    /// operations only available to KeyValuePair RDD
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
        public static DStream<KeyValuePair<K, V>> ReduceByKey<K, V>(this DStream<KeyValuePair<K, V>> self, Func<V, V, V> reduceFunc, int numPartitions = 0)
        {
            return self.CombineByKey(() => default(V), reduceFunc, reduceFunc, numPartitions);
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
        public static DStream<KeyValuePair<K, C>> CombineByKey<K, V, C>(
            this DStream<KeyValuePair<K, V>> self,
            Func<C> createCombiner,
            Func<C, V, C> mergeValue,
            Func<C, C, C> mergeCombiners,
            int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.Transform<KeyValuePair<K, C>>(new CombineByKeyHelper<K, V, C>(createCombiner, mergeValue, mergeCombiners, numPartitions).Execute);
        }

        /// <summary>
        /// Return a new DStream in which each RDD are partitioned by numPartitions.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <param name="self"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<KeyValuePair<K, V>> PartitionBy<K, V>(this DStream<KeyValuePair<K, V>> self, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.Transform<KeyValuePair<K, V>>(new PartitionByHelper<K, V>(numPartitions).Execute);
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
        public static DStream<KeyValuePair<K, U>> MapValues<K, V, U>(this DStream<KeyValuePair<K, V>> self, Func<V, U> func)
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
        public static DStream<KeyValuePair<K, U>> FlatMapValues<K, V, U>(this DStream<KeyValuePair<K, V>> self, Func<V, IEnumerable<U>> func)
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
        public static DStream<KeyValuePair<K, List<V>>> GroupByKey<K, V>(this DStream<KeyValuePair<K, V>> self, int numPartitions = 0)
        {
            return self.Transform<KeyValuePair<K, List<V>>>(new GroupByKeyHelper<K, V>(numPartitions).Execute);
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
        public static DStream<KeyValuePair<K, Tuple<List<V>, List<W>>>> GroupWith<K, V, W>(this DStream<KeyValuePair<K, V>> self, DStream<KeyValuePair<K, W>> other, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.TransformWith<KeyValuePair<K, W>, KeyValuePair<K, Tuple<List<V>, List<W>>>>(new GroupWithHelper<K, V, W>(numPartitions).Execute, other);
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
        public static DStream<KeyValuePair<K, Tuple<V, W>>> Join<K, V, W>(this DStream<KeyValuePair<K, V>> self, DStream<KeyValuePair<K, W>> other, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.TransformWith<KeyValuePair<K, W>, KeyValuePair<K, Tuple<V, W>>>(new JoinHelper<K, V, W>(numPartitions).Execute, other);
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
        public static DStream<KeyValuePair<K, Tuple<V, W>>> LeftOuterJoin<K, V, W>(this DStream<KeyValuePair<K, V>> self, DStream<KeyValuePair<K, W>> other, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.TransformWith<KeyValuePair<K, W>, KeyValuePair<K, Tuple<V, W>>>(new LeftOuterJoinHelper<K, V, W>(numPartitions).Execute, other);
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
        public static DStream<KeyValuePair<K, Tuple<V, W>>> RightOuterJoin<K, V, W>(this DStream<KeyValuePair<K, V>> self, DStream<KeyValuePair<K, W>> other, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.TransformWith<KeyValuePair<K, W>, KeyValuePair<K, Tuple<V, W>>>(new RightOuterJoinHelper<K, V, W>(numPartitions).Execute, other);
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
        public static DStream<KeyValuePair<K, Tuple<V, W>>> FullOuterJoin<K, V, W>(this DStream<KeyValuePair<K, V>> self, DStream<KeyValuePair<K, W>> other, int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            return self.TransformWith<KeyValuePair<K, W>, KeyValuePair<K, Tuple<V, W>>>(new FullOuterJoinHelper<K, V, W>(numPartitions).Execute, other);
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
        public static DStream<KeyValuePair<K, IEnumerable<V>>> GroupByKeyAndWindow<K, V>(this DStream<KeyValuePair<K, V>> self,
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
        public static DStream<KeyValuePair<K, V>> ReduceByKeyAndWindow<K, V>(this DStream<KeyValuePair<K, V>> self,
            Func<V, V, V> reduceFunc,
            Func<V, V, V> invReduceFunc,
            int windowSeconds,
            int slideSeconds = 0,
            int numPartitions = 0,
            Func<KeyValuePair<K, V>, bool> filterFunc = null)
        {
            self.ValidatWindowParam(windowSeconds, slideSeconds);

            if (slideSeconds <= 0)
                slideSeconds = self.SlideDuration;

            // dstream to be transformed by substracting old RDDs and adding new RDDs based on the window
            var reduced = self.ReduceByKey(reduceFunc, numPartitions);

            Func<double, RDD<dynamic>, RDD<dynamic>> prevFunc = reduced.Piplinable ? (reduced as TransformedDStream<KeyValuePair<K, V>>).func : null;

            var helper = new ReduceByKeyAndWindowHelper<K, V>(reduceFunc, invReduceFunc, numPartitions, filterFunc, prevFunc);
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
                formatter.Serialize(stream, invReduceF);
            }

            return new DStream<KeyValuePair<K, V>>(
                SparkCLREnvironment.SparkCLRProxy.StreamingContextProxy.CreateCSharpReducedWindowedDStream(
                    reduced.Piplinable ? reduced.prevDStreamProxy : reduced.DStreamProxy, 
                    stream.ToArray(),
                    invStream == null ? null : invStream.ToArray(),
                    windowSeconds,
                    slideSeconds,
                    (reduced.Piplinable ? reduced.prevSerializedMode : reduced.serializedMode).ToString()), 
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
        /// <param name="updateFunc"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<KeyValuePair<K, S>> UpdateStateByKey<K, V, S>(this DStream<KeyValuePair<K, V>> self,
            Func<IEnumerable<V>, S, S> updateFunc,
            int numPartitions = 0)
        {
            return UpdateStateByKey<K, V, S>(self, new UpdateStateByKeyHelper<K, V, S>(updateFunc).Execute, numPartitions);
        }

        /// <summary>
        /// Return a new "state" DStream where the state for each key is updated by applying
        /// the given function on the previous state of the key and the new values of the key.
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <typeparam name="V"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="self"></param>
        /// <param name="updateFunc">State update function. If this function returns None, then corresponding state key-value pair will be eliminated.</param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public static DStream<KeyValuePair<K, S>> UpdateStateByKey<K, V, S>(this DStream<KeyValuePair<K, V>> self,
            Func<IEnumerable<KeyValuePair<K, Tuple<IEnumerable<V>, S>>>, IEnumerable<KeyValuePair<K, S>>> updateFunc,
            int numPartitions = 0)
        {
            if (numPartitions <= 0)
                numPartitions = self.streamingContext.SparkContext.DefaultParallelism;

            Func<double, RDD<dynamic>, RDD<dynamic>> prevFunc = self.Piplinable ? (self as TransformedDStream<KeyValuePair<K, V>>).func : null;

            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> func = new UpdateStateByKeysHelper<K, V, S>(updateFunc, prevFunc, numPartitions).Execute;

            var formatter = new BinaryFormatter();
            var stream = new MemoryStream();
            formatter.Serialize(stream, func);

            return new DStream<KeyValuePair<K, S>>(SparkCLREnvironment.SparkCLRProxy.StreamingContextProxy.CreateCSharpStateDStream(
                    self.Piplinable ? self.prevDStreamProxy : self.DStreamProxy,
                    stream.ToArray(),
                    self.serializedMode.ToString(),
                    (self.Piplinable ? self.prevSerializedMode : self.serializedMode).ToString()),
                self.streamingContext);
        }
    }

    /// <summary>
    /// Following classes are defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not marked serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. These classes are to work around the limitation
    /// on the serializability of compiler generated types
    /// </summary>

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

        internal RDD<KeyValuePair<K, C>> Execute(RDD<KeyValuePair<K, V>> rdd)
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

        internal RDD<KeyValuePair<K, V>> Execute(RDD<KeyValuePair<K, V>> rdd)
        {
            return rdd.PartitionBy(numPartitions);
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

        internal KeyValuePair<K, U> Execute(KeyValuePair<K, V> kvp)
        {
            return new KeyValuePair<K, U>(kvp.Key, func(kvp.Value));
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

        internal IEnumerable<KeyValuePair<K, U>> Execute(KeyValuePair<K, V> kvp)
        {
            return func(kvp.Value).Select(v => new KeyValuePair<K, U>(kvp.Key, v));
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

        internal RDD<KeyValuePair<K, List<V>>> Execute(RDD<KeyValuePair<K, V>> rdd)
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

        internal RDD<KeyValuePair<K, Tuple<List<V>, List<W>>>> Execute(RDD<KeyValuePair<K, V>> l, RDD<KeyValuePair<K, W>> r)
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

        internal RDD<KeyValuePair<K, Tuple<V, W>>> Execute(RDD<KeyValuePair<K, V>> l, RDD<KeyValuePair<K, W>> r)
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

        internal RDD<KeyValuePair<K, Tuple<V, W>>> Execute(RDD<KeyValuePair<K, V>> l, RDD<KeyValuePair<K, W>> r)
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

        internal RDD<KeyValuePair<K, Tuple<V, W>>> Execute(RDD<KeyValuePair<K, V>> l, RDD<KeyValuePair<K, W>> r)
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

        internal RDD<KeyValuePair<K, Tuple<V, W>>> Execute(RDD<KeyValuePair<K, V>> l, RDD<KeyValuePair<K, W>> r)
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
        private readonly Func<KeyValuePair<K, V>, bool> filterFunc;
        private readonly Func<double, RDD<dynamic>, RDD<dynamic>> prevFunc;

        internal ReduceByKeyAndWindowHelper(Func<V, V, V> reduceF, 
            Func<V, V, V> invReduceF, 
            int numPartitions, 
            Func<KeyValuePair<K, V>, bool> filterF, 
            Func<double, RDD<dynamic>, RDD<dynamic>> prevF)
        {
            reduceFunc = reduceF;
            invReduceFunc = invReduceF;
            this.numPartitions = numPartitions;
            filterFunc = filterF;
            prevFunc = prevF;
        }

        internal RDD<dynamic> Reduce(double t, RDD<dynamic> a, RDD<dynamic> b)
        {
            if (prevFunc != null)
                b = prevFunc(t, b);

            var r = b.ConvertTo<KeyValuePair<K, V>>().ReduceByKey<K, V>(reduceFunc);
            if (a != null)
            {
                if (prevFunc != null)
                    a = prevFunc(t, a);
                
                r = a.ConvertTo<KeyValuePair<K, V>>().Union(r).ReduceByKey<K, V>(reduceFunc);
            }
            if (filterFunc != null)
                r.Filter(filterFunc);
            return r.ConvertTo<dynamic>();
        }

        internal RDD<dynamic> InvReduce(double t, RDD<dynamic> a, RDD<dynamic> b)
        {
            if (prevFunc != null)
            {
                a = prevFunc(t, a);
                b = prevFunc(t, b);
            }

            var rddb = b.ConvertTo<KeyValuePair<K, V>>().ReduceByKey<K, V>(reduceFunc);
            var rdda = a.ConvertTo<KeyValuePair<K, V>>();
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

        internal IEnumerable<KeyValuePair<K, S>> Execute(IEnumerable<KeyValuePair<K, Tuple<IEnumerable<V>, S>>> input)
        {
            return input.Select(x => new KeyValuePair<K, S>(x.Key, func(x.Value.Item1, x.Value.Item2)));
        }
    }

    [Serializable]
    internal class UpdateStateByKeysHelper<K, V, S>
    {
        private readonly Func<IEnumerable<KeyValuePair<K, Tuple<IEnumerable<V>, S>>>, IEnumerable<KeyValuePair<K, S>>> func;
        private readonly Func<double, RDD<dynamic>, RDD<dynamic>> prevFunc;
        private readonly int numPartitions;
        internal UpdateStateByKeysHelper(
            Func<IEnumerable<KeyValuePair<K, Tuple<IEnumerable<V>, S>>>, IEnumerable<KeyValuePair<K, S>>> f, 
            Func<double, RDD<dynamic>, RDD<dynamic>> prevF, int numPartitions)
        {
            func = f;
            prevFunc = prevF;
            this.numPartitions = numPartitions;
        }

        internal RDD<dynamic> Execute(double t, RDD<dynamic> stateRDD, RDD<dynamic> valuesRDD)
        {
            RDD<KeyValuePair<K, S>> state = null;
            RDD<KeyValuePair<K, Tuple<IEnumerable<V>, S>>> g = null;

            if (prevFunc != null)
                valuesRDD = prevFunc(t, valuesRDD);

            var values = valuesRDD.ConvertTo<KeyValuePair<K, V>>();

            if (stateRDD == null)
            {
                g = values.GroupByKey(numPartitions).MapValues(x => new Tuple<IEnumerable<V>, S>(new List<V>(x), default(S)));
            }
            else
            {
                state = stateRDD.ConvertTo<KeyValuePair<K, S>>();
                values = values.PartitionBy(numPartitions);
                state.partitioner = values.partitioner;
                g = state.GroupWith(values, numPartitions).MapValues(x => new Tuple<IEnumerable<V>, S>(new List<V>(x.Item2), x.Item1.Count > 0 ? x.Item1[0] : default(S)));
            }

            state = g.MapPartitionsWithIndex((pid, iter) => func(iter), true).Filter(x => x.Value != null);

            return state.ConvertTo<dynamic>();
        }
    }
}
