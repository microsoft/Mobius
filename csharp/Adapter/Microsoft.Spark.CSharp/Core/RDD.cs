// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Reflection;
using System.IO;
using System.Net.Sockets;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Sql;
using Razorvine.Pickle;
using Razorvine.Pickle.Objects;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Represents a Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable, 
    /// partitioned collection of elements that can be operated on in parallel
    /// 
    /// See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD
    /// </summary>
    /// <typeparam name="T">Type of the RDD</typeparam>
    [Serializable]
    public class RDD<T>
    {
        internal IRDDProxy rddProxy;
        internal IRDDProxy previousRddProxy;
        internal SparkContext sparkContext;
        internal SerializedMode serializedMode; //used for deserializing data before processing in C# worker
        internal SerializedMode prevSerializedMode;

        protected bool isCached;
        protected bool isCheckpointed;
        internal bool bypassSerializer;
        internal int? partitioner;

        internal virtual IRDDProxy RddProxy
        {
            get
            {
                return rddProxy;
            }
        }

        /// <summary>
        /// Return whether this RDD has been cached or not
        /// </summary>
        public bool IsCached
        {
            get
            {
                return isCached;
            }
        }

        /// <summary>
        /// Return whether this RDD has been checkpointed or not
        /// </summary>
        public bool IsCheckpointed
        {
            get
            {
                return RddProxy.IsCheckpointed;
            }
        }

        /// <summary>
        /// Return the name of this RDD.
        /// </summary>
        public string Name
        {
            get
            {
                return RddProxy.Name;
            }
        }

        internal int DefaultReducePartitions
        {
            get
            {
                return GetNumPartitions();
            }
        }

        internal RDD() { }

        internal RDD(IRDDProxy rddProxy, SparkContext sparkContext, SerializedMode serializedMode = SerializedMode.Byte)
        {
            this.rddProxy = rddProxy;
            this.sparkContext = sparkContext;
            this.serializedMode = serializedMode;
        }

        internal RDD<U> ConvertTo<U>()
        {
            RDD<U> r = (this is PipelinedRDD<T>) ? new PipelinedRDD<U>() : new RDD<U>();

            r.rddProxy = rddProxy;
            r.sparkContext = sparkContext;
            r.previousRddProxy = previousRddProxy;
            r.prevSerializedMode = prevSerializedMode;
            r.serializedMode = serializedMode;
            r.partitioner = partitioner;

            if (this is PipelinedRDD<T>)
            {
                CSharpWorkerFunc oldWorkerFunc = (this as PipelinedRDD<T>).workerFunc;
                CSharpWorkerFunc newWorkerFunc = new CSharpWorkerFunc(oldWorkerFunc.Func, oldWorkerFunc.StackTrace);
                (r as PipelinedRDD<U>).workerFunc = newWorkerFunc;
                (r as PipelinedRDD<U>).preservesPartitioning = (this as PipelinedRDD<T>).preservesPartitioning;
            }

            return r;
        }

        /// <summary>
        /// Persist this RDD with the default storage level (C{MEMORY_ONLY_SER}).
        /// </summary>
        /// <returns></returns>
        public RDD<T> Cache()
        {
            isCached = true;
            RddProxy.Cache();
            return this;
        }

        /// <summary>
        /// Set this RDD's storage level to persist its values across operations
        /// after the first time it is computed. This can only be used to assign
        /// a new storage level if the RDD does not have a storage level set yet.
        /// If no storage level is specified defaults to (C{MEMORY_ONLY_SER}).
        /// 
        /// sc.Parallelize(new string[] {"b", "a", "c").Persist().isCached
        /// True
        /// 
        /// </summary>
        /// <param name="storageLevelType"></param>
        /// <returns></returns>
        public RDD<T> Persist(StorageLevelType storageLevelType)
        {
            isCached = true;
            RddProxy.Persist(storageLevelType);
            return this;
        }
        /// <summary>
        /// Mark the RDD as non-persistent, and remove all blocks for it from
        /// memory and disk.
        /// </summary>
        /// <returns></returns>
        public RDD<T> Unpersist()
        {
            if (isCached)
            {
                isCached = false;
                RddProxy.Unpersist();
            }
            return this;
        }

        /// <summary>
        /// Mark this RDD for checkpointing. It will be saved to a file inside the
        /// checkpoint directory set with L{SparkContext.setCheckpointDir()} and
        /// all references to its parent RDDs will be removed. This function must
        /// be called before any job has been executed on this RDD. It is strongly
        /// recommended that this RDD is persisted in memory, otherwise saving it
        /// on a file will require recomputation.
        /// </summary>
        public void Checkpoint()
        {
            isCheckpointed = true;
            RddProxy.Checkpoint();
        }

        internal int GetNumPartitions()
        {
            return RddProxy.GetNumPartitions();
        }

        /// <summary>
        /// Return a new RDD by applying a function to each element of this RDD.
        /// 
        /// sc.Parallelize(new string[]{"b", "a", "c"}, 1).Map(x => new KeyValuePair&lt;string, int>(x, 1)).Collect()
        /// [('a', 1), ('b', 1), ('c', 1)]
        /// 
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <param name="f"></param>
        /// <param name="preservesPartitioning"></param>
        /// <returns></returns>
        public RDD<U> Map<U>(Func<T, U> f, bool preservesPartitioning = false)
        {
            return MapPartitionsWithIndex(new MapHelper<T, U>(f).Execute, preservesPartitioning);
        }

        /// <summary>
        /// Return a new RDD by first applying a function to all elements of this
        /// RDD, and then flattening the results.
        /// 
        /// sc.Parallelize(new int[] {2, 3, 4}, 1).FlatMap(x => Enumerable.Range(1, x - 1)).Collect()
        /// [1, 1, 1, 2, 2, 3]
        /// 
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <param name="f"></param>
        /// <param name="preservesPartitioning"></param>
        /// <returns></returns>
        public RDD<U> FlatMap<U>(Func<T, IEnumerable<U>> f, bool preservesPartitioning = false)
        {
            return MapPartitionsWithIndex(new FlatMapHelper<T, U>(f).Execute, preservesPartitioning);
        }

        /// <summary>
        /// Return a new RDD by applying a function to each partition of this RDD.
        /// 
        /// sc.Parallelize(new int[] {1, 2, 3, 4}, 2).MapPartitions(iter => new[]{iter.Sum(x => (x as decimal?))}).Collect()
        /// [3, 7]
        /// 
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <param name="f"></param>
        /// <param name="preservesPartitioning"></param>
        /// <returns></returns>
        public RDD<U> MapPartitions<U>(Func<IEnumerable<T>, IEnumerable<U>> f, bool preservesPartitioning = false)
        {
            return MapPartitionsWithIndex(new MapPartitionsHelper<T, U>(f).Execute, preservesPartitioning);
        }

        /// <summary>
        /// Return a new RDD by applying a function to each partition of this RDD,
        /// while tracking the index of the original partition.
        /// 
        /// sc.Parallelize(new int[]{1, 2, 3, 4}, 4).MapPartitionsWithIndex&lt;double>((pid, iter) => (double)pid).Sum()
        /// 6
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <param name="f"></param>
        /// <param name="preservesPartitioningParam"></param>
        /// <returns></returns>
        public virtual RDD<U> MapPartitionsWithIndex<U>(Func<int, IEnumerable<T>, IEnumerable<U>> f, bool preservesPartitioningParam = false)
        {
            CSharpWorkerFunc csharpWorkerFunc = new CSharpWorkerFunc(new DynamicTypingWrapper<T, U>(f).Execute);
            var pipelinedRDD = new PipelinedRDD<U>
            {
                workerFunc = csharpWorkerFunc,
                preservesPartitioning = preservesPartitioningParam,
                previousRddProxy = rddProxy,
                prevSerializedMode = serializedMode,

                sparkContext = sparkContext,
                rddProxy = null,
                serializedMode = SerializedMode.Byte,
                partitioner = preservesPartitioningParam ? partitioner : null
            };
            return pipelinedRDD;
        }

        /// <summary>
        /// Return a new RDD containing only the elements that satisfy a predicate.
        /// 
        /// sc.Parallelize(new int[]{1, 2, 3, 4, 5}, 1).Filter(x => x % 2 == 0).Collect()
        /// [2, 4]
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public RDD<T> Filter(Func<T, bool> f)
        {
            return MapPartitionsWithIndex(new FilterHelper<T>(f).Execute, true);
        }

        /// <summary>
        /// Return a new RDD containing the distinct elements in this RDD.
        /// 
        /// >>> sc.Parallelize(new int[] {1, 1, 2, 3}, 1).Distinct().Collect()
        /// [1, 2, 3]
        /// </summary>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public RDD<T> Distinct(int numPartitions = 0)
        {
            return Map(x => new KeyValuePair<T, int>(x, 0)).ReduceByKey((x, y) => x, numPartitions).Map<T>(x => x.Key);
        }

        /// <summary>
        /// Return a sampled subset of this RDD.
        ///
        /// var rdd = sc.Parallelize(Enumerable.Range(0, 100), 4)
        /// 6 &lt;= rdd.Sample(False, 0.1, 81).count() &lt;= 14
        /// true
        /// 
        /// </summary>
        /// <param name="withReplacement">can elements be sampled multiple times (replaced when sampled out)</param>
        /// <param name="fraction">expected size of the sample as a fraction of this RDD's size
        ///    without replacement: probability that each element is chosen; fraction must be [0, 1]
        ///    with replacement: expected number of times each element is chosen; fraction must be >= 0</param>
        /// <param name="seed">seed for the random number generator</param>
        /// <returns></returns>
        public RDD<T> Sample(bool withReplacement, double fraction, long seed)
        {
            return new RDD<T>(RddProxy.Sample(withReplacement, fraction, seed), sparkContext);
        }

        /// <summary>
        /// Randomly splits this RDD with the provided weights.
        ///
        /// var rdd = sc.Parallelize(Enumerable.Range(0, 500), 1)
        /// var rdds = rdd.RandomSplit(new double[] {2, 3}, 17)
        /// 150 &lt; rdds[0].Count() &lt; 250
        /// 250 &lt; rdds[1].Count() &lt; 350
        /// 
        /// </summary>
        /// <param name="weights">weights for splits, will be normalized if they don't sum to 1</param>
        /// <param name="seed">random seed</param>
        /// <returns>split RDDs in a list</returns>
        public RDD<T>[] RandomSplit(double[] weights, long seed)
        {
            return RddProxy.RandomSplit(weights, seed).Select(r => new RDD<T>(r, sparkContext)).ToArray();
        }

        /// <summary>
        /// Return a fixed-size sampled subset of this RDD.
        /// 
        /// var rdd = sc.Parallelize(Enumerable.Range(0, 10), 2)
        /// rdd.TakeSample(true, 20, 1).Length
        /// 20
        /// rdd.TakeSample(false, 5, 2).Length
        /// 5
        /// rdd.TakeSample(false, 15, 3).Length
        /// 10
        /// 
        /// </summary>
        /// <param name="withReplacement"></param>
        /// <param name="num"></param>
        /// <param name="seed"></param>
        /// <returns></returns>
        public T[] TakeSample(bool withReplacement, int num, int seed)
        {
            const double numStDev = 10.0;

            if (num < 0)
                throw new ArgumentException("Sample size cannot be negative.");
            else if (num == 0)
                return new T[0];

            int initialCount = (int)Count();
            if (initialCount == 0)
                return new T[0];

            var rand = new Random(seed);

            if (!withReplacement && num >= initialCount)
            {
                // shuffle current RDD and return
                return Collect().OrderBy(x => rand.Next()).ToArray();
            }

            var maxSampleSize = int.MaxValue - (int)(numStDev * Math.Sqrt(int.MaxValue));
            if (num > maxSampleSize)
                throw new ArgumentException(string.Format("Sample size cannot be greater than {0}.", maxSampleSize));

            var fraction = ComputeFractionForSampleSize(num, initialCount, withReplacement);
            var samples = Sample(withReplacement, fraction, seed).Collect();

            // If the first sample didn't turn out large enough, keep trying to take samples;
            // this shouldn't happen often because we use a big multiplier for their initial size.
            // See: scala/spark/RDD.scala
            var numIters = 0;
            while (samples.Length < num)
            {
                // logWarning(string.Format("Needed to re-sample due to insufficient sample size. Repeat {0}", numIters));
                samples = Sample(withReplacement, fraction, rand.Next()).Collect();
                numIters += 1;
            }

            return samples.OrderBy(x => rand.Next()).Take(num).ToArray();
        }

        /// <summary>
        /// Returns a sampling rate that guarantees a sample of
        /// size >= sampleSizeLowerBound 99.99% of the time.
        /// 
        /// How the sampling rate is determined:
        /// Let p = num / total, where num is the sample size and total is the
        /// total number of data points in the RDD. We're trying to compute
        /// q > p such that
        ///   - when sampling with replacement, we're drawing each data point
        ///     with prob_i ~ Pois(q), where we want to guarantee
        ///     Pr[s &lt; num] &lt; 0.0001 for s = sum(prob_i for i from 0 to
        ///     total), i.e. the failure rate of not having a sufficiently large
        ///     sample &lt; 0.0001. Setting q = p + 5 * sqrt(p/total) is sufficient
        ///     to guarantee 0.9999 success rate for num > 12, but we need a
        ///     slightly larger q (9 empirically determined).
        ///   - when sampling without replacement, we're drawing each data point
        ///     with prob_i ~ Binomial(total, fraction) and our choice of q
        ///     guarantees 1-delta, or 0.9999 success rate, where success rate is
        ///     defined the same as in sampling with replacement.
        /// </summary>
        /// <param name="sampleSizeLowerBound"></param>
        /// <param name="total"></param>
        /// <param name="withReplacement"></param>
        /// <returns></returns>
        private static double ComputeFractionForSampleSize(int sampleSizeLowerBound, int total, bool withReplacement)
        {
            var fraction = (float)sampleSizeLowerBound / total;
            if (withReplacement)
            {
                int numStDev = 5;
                if (sampleSizeLowerBound < 12)
                    numStDev = 9;
                return fraction + numStDev * Math.Sqrt(fraction / total);
            }
            else
            {
                const double delta = 0.00005;
                var gamma = -Math.Log(delta) / total;
                return Math.Min(1, fraction + gamma + Math.Sqrt(gamma * gamma + 2 * gamma * fraction));
            }
        }

        /// <summary>
        /// Return the union of this RDD and another one.
        /// 
        /// var rdd = sc.Parallelize(new int[] { 1, 1, 2, 3 }, 1)
        /// rdd.union(rdd).collect()
        /// [1, 1, 2, 3, 1, 1, 2, 3]
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public RDD<T> Union(RDD<T> other)
        {
            var rdd = new RDD<T>(RddProxy.Union(other.RddProxy), sparkContext);
            if (partitioner == other.partitioner && RddProxy.PartitionLength() == rdd.RddProxy.PartitionLength())
                rdd.partitioner = partitioner;
            return rdd;
        }

        /// <summary>
        /// Return the intersection of this RDD and another one. The output will
        /// not contain any duplicate elements, even if the input RDDs did.
        /// 
        /// Note that this method performs a shuffle internally.
        /// 
        /// var rdd1 = sc.Parallelize(new int[] { 1, 10, 2, 3, 4, 5 }, 1)
        /// var rdd2 = sc.Parallelize(new int[] { 1, 6, 2, 3, 7, 8 }, 1)
        /// var rdd1.Intersection(rdd2).Collect()
        /// [1, 2, 3]
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public RDD<T> Intersection(RDD<T> other)
        {
            return Map(v => new KeyValuePair<T, int>(v, 0))
                .GroupWith(other.Map(v => new KeyValuePair<T, int>(v, 0)))
                .Filter(kv => kv.Value.Item1.Count > 0 && kv.Value.Item2.Count > 0)
                .Keys();
        }

        // /// <summary>
        // /// Sorts this RDD by the given keyfunc
        // /// 
        // /// >>> tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
        // /// >>> sc.Parallelize(tmp).sortBy(lambda x: x[0]).collect()
        // /// [('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
        // /// >>> sc.Parallelize(tmp).sortBy(lambda x: x[1]).collect()
        // /// [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
        // /// 
        // /// </summary>
        // /// <typeparam name="K"></typeparam>
        // /// <param name="keyfunc"></param>
        // /// <param name="ascending"></param>
        // /// <param name="numPartitions"></param>
        // /// <returns></returns>
        //public RDD<T> SortBy<K>(Func<T, K> keyfunc, bool ascending = true, int? numPartitions = null)
        //{
        //    return KeyBy<K>(keyfunc).SortByKey<K, T>(ascending, numPartitions).Map<T>(kv => kv.Value);
        //}

        /// <summary>
        /// Return an RDD created by coalescing all elements within each partition into a list.
        ///
        /// var rdd = sc.Parallelize(new int[] { 1, 2, 3, 4 }, 2)
        /// rdd.Glom().Collect()
        /// [[1, 2], [3, 4]]
        /// 
        /// </summary>
        /// <returns></returns>
        public RDD<T[]> Glom()
        {
            return MapPartitionsWithIndex<T[]>(new GlomHelper<T>().Execute);
        }

        /// <summary>
        /// Return the Cartesian product of this RDD and another one, that is, the
        /// RDD of all pairs of elements C{(a, b)} where C{a} is in C{self} and C{b} is in C{other}.
        /// 
        /// rdd = sc.Parallelize(new int[] { 1, 2 }, 1)
        /// rdd.Cartesian(rdd).Collect()
        /// [(1, 1), (1, 2), (2, 1), (2, 2)]
        /// 
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <param name="other"></param>
        /// <returns></returns>
        public RDD<Tuple<T, U>> Cartesian<U>(RDD<U> other)
        {
            return new RDD<Tuple<T, U>>(RddProxy.Cartesian(other.RddProxy), sparkContext, SerializedMode.Pair);
        }

        /// <summary>
        /// Return an RDD of grouped items. Each group consists of a key and a sequence of elements
        /// mapping to that key. The ordering of elements within each group is not guaranteed, and
        /// may even differ each time the resulting RDD is evaluated.
        ///
        /// Note: This operation may be very expensive. If you are grouping in order to perform an
        /// aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
        /// or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
        /// 
        /// >>> rdd = sc.Parallelize(new int[] { 1, 1, 2, 3, 5, 8 }, 1)
        /// >>> result = rdd.GroupBy(lambda x: x % 2).Collect()
        /// [(0, [2, 8]), (1, [1, 1, 3, 5])]
        /// 
        /// </summary>
        /// <returns></returns>
        public RDD<KeyValuePair<K, List<T>>> GroupBy<K>(Func<T, K> f, int numPartitions = 0)
        {
            return KeyBy(f).GroupByKey(numPartitions);
        }

        /// <summary>
        /// Return an RDD created by piping elements to a forked external process.
        ///
        /// >>> sc.Parallelize(new char[] { '1', '2', '3', '4' }, 1).Pipe("cat").Collect()
        /// [u'1', u'2', u'3', u'4']
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        public RDD<string> Pipe(string command)
        {
            return new RDD<string>(RddProxy.Pipe(command), sparkContext);
        }

        /// <summary>
        /// Applies a function to all elements of this RDD.
        ///
        /// sc.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).Foreach(x => Console.Write(x))
        /// 
        /// </summary>
        /// <param name="f"></param>
        public void Foreach(Action<T> f)
        {
            MapPartitionsWithIndex<T>(new ForeachHelper<T>(f).Execute).Count(); // Force evaluation
        }

        /// <summary>
        /// Applies a function to each partition of this RDD.
        ///
        /// sc.parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).ForeachPartition(iter => { foreach (var x in iter) Console.Write(x + " "); })
        /// 
        /// </summary>
        /// <param name="f"></param>
        public void ForeachPartition(Action<IEnumerable<T>> f)
        {
            MapPartitionsWithIndex<T>(new ForeachPartitionHelper<T>(f).Execute).Count(); // Force evaluation
        }

        /// <summary>
        /// Return a list that contains all of the elements in this RDD.
        /// </summary>
        /// <returns></returns>
        public T[] Collect()
        {
            int port = RddProxy.CollectAndServe();
            return Collect(port).Cast<T>().ToArray();
        }

        internal IEnumerable<dynamic> Collect(int port)
        {
            return RddProxy.RDDCollector.Collect(port, serializedMode, typeof(T));
        }

        /// <summary>
        /// Reduces the elements of this RDD using the specified commutative and
        /// associative binary operator.
        /// 
        /// sc.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).Reduce((x, y) => x + y)
        /// 15
        /// 
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public T Reduce(Func<T, T, T> f)
        {
            Func<int, IEnumerable<T>, IEnumerable<T>> func = new ReduceHelper<T>(f).Execute;
            var vals = MapPartitionsWithIndex(func, true).Collect();

            if (vals == null)
            {
                throw new ArgumentException("Cannot reduce empty RDD");
            }

            return vals.Aggregate(f);
        }

        /// <summary>
        /// Reduces the elements of this RDD in a multi-level tree pattern.
        /// 
        /// >>> add = lambda x, y: x + y
        /// >>> rdd = sc.Parallelize(new int[] { -5, -4, -3, -2, -1, 1, 2, 3, 4 }, 10).TreeReduce((x, y) => x + y))
        /// >>> rdd.TreeReduce(add)
        /// -5
        /// >>> rdd.TreeReduce(add, 1)
        /// -5
        /// >>> rdd.TreeReduce(add, 2)
        /// -5
        /// >>> rdd.TreeReduce(add, 5)
        /// -5
        /// >>> rdd.TreeReduce(add, 10)
        /// -5
        /// 
        /// </summary>
        /// <param name="f"></param>
        /// <param name="depth">suggested depth of the tree (default: 2)</param>
        /// <returns></returns>
        public T TreeReduce(Func<T, T, T> f, int depth = 2)
        {
            if (depth < 1)
                throw new ArgumentException(string.Format("Depth cannot be smaller than 1 but got {0}.", depth));

            var zeroValue = new KeyValuePair<T, bool>(default(T), true);  // Use the second entry to indicate whether this is a dummy value.

            Func<KeyValuePair<T, bool>, KeyValuePair<T, bool>, KeyValuePair<T, bool>> op = new TreeReduceHelper<T>(f).Execute;

            var reduced = Map<KeyValuePair<T, bool>>(x => new KeyValuePair<T, bool>(x, false)).TreeAggregate(zeroValue, op, op, depth);
            if (reduced.Value)
                throw new ArgumentException("Cannot reduce empty RDD.");
            return reduced.Key;
        }

        /// <summary>
        /// Aggregate the elements of each partition, and then the results for all
        /// the partitions, using a given associative and commutative function and
        /// a neutral "zero value."
        /// 
        /// The function C{op(t1, t2)} is allowed to modify C{t1} and return it
        /// as its result value to avoid object allocation; however, it should not
        /// modify C{t2}.
        /// 
        /// This behaves somewhat differently from fold operations implemented
        /// for non-distributed collections in functional languages like Scala.
        /// This fold operation may be applied to partitions individually, and then
        /// fold those results into the final result, rather than apply the fold
        /// to each element sequentially in some defined ordering. For functions
        /// that are not commutative, the result may differ from that of a fold
        /// applied to a non-distributed collection.
        /// 
        /// >>> from operator import add
        /// >>> sc.parallelize([1, 2, 3, 4, 5]).fold(0, add)
        /// 15
        /// 
        /// </summary>
        /// <param name="zeroValue"></param>
        /// <param name="op"></param>
        /// <returns></returns>
        public T Fold(T zeroValue, Func<T, T, T> op)
        {
            T[] vals = MapPartitionsWithIndex<T>(new AggregateHelper<T, T>(zeroValue, op).Execute).Collect();
            return vals.Aggregate(zeroValue, op);
        }

        /// <summary>
        /// Aggregate the elements of each partition, and then the results for all
        /// the partitions, using a given combine functions and a neutral "zero
        /// value."
        /// 
        /// The functions C{op(t1, t2)} is allowed to modify C{t1} and return it
        /// as its result value to avoid object allocation; however, it should not
        /// modify C{t2}.
        /// 
        /// The first function (seqOp) can return a different result type, U, than
        /// the type of this RDD. Thus, we need one operation for merging a T into
        /// an U and one operation for merging two U
        /// 
        /// >>> sc.parallelize(new int[] { 1, 2, 3, 4 }, 1).Aggregate(0, (x, y) => x + y, (x, y) => x + y))
        /// 10
        /// 
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <param name="zeroValue"></param>
        /// <param name="seqOp"></param>
        /// <param name="combOp"></param>
        /// <returns></returns>
        public U Aggregate<U>(U zeroValue, Func<U, T, U> seqOp, Func<U, U, U> combOp)
        {
            return MapPartitionsWithIndex<U>(new AggregateHelper<U, T>(zeroValue, seqOp).Execute).Fold(zeroValue, combOp);
        }

        /// <summary>
        /// Aggregates the elements of this RDD in a multi-level tree pattern.
        /// 
        /// rdd = sc.Parallelize(new int[] { 1, 2, 3, 4 }, 1).TreeAggregate(0, (x, y) => x + y, (x, y) => x + y))
        /// 10
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <param name="zeroValue"></param>
        /// <param name="seqOp"></param>
        /// <param name="combOp"></param>
        /// <param name="depth">suggested depth of the tree (default: 2)</param>
        /// <returns></returns>
        public U TreeAggregate<U>(U zeroValue, Func<U, T, U> seqOp, Func<U, U, U> combOp, int depth = 2)
        {
            if (depth < 1)
                throw new ArgumentException(string.Format("Depth cannot be smaller than 1 but got {0}.", depth));

            if (GetNumPartitions() == 0)
                return zeroValue;

            var partiallyAggregated = MapPartitionsWithIndex(new AggregateHelper<U, T>(zeroValue, seqOp).Execute);
            int numPartitions = partiallyAggregated.GetNumPartitions();
            int scale = Math.Max((int)(Math.Ceiling(Math.Pow(numPartitions, 1.0 / depth))), 2);
            // If creating an extra level doesn't help reduce the wall-clock time, we stop the tree aggregation.
            while (numPartitions > scale + numPartitions / scale)
            {
                numPartitions /= scale;

                partiallyAggregated = partiallyAggregated
                    .MapPartitionsWithIndex<KeyValuePair<int, U>>(new TreeAggregateHelper<U>(numPartitions).Execute)
                    .ReduceByKey(combOp, numPartitions)
                    .Values();
            }

            return partiallyAggregated.Reduce(combOp);
        }

        /// <summary>
        /// Return the number of elements in this RDD.
        /// </summary>
        /// <returns></returns>
        public long Count()
        {
            return RddProxy.Count();
        }

        /// <summary>
        /// Return the count of each unique value in this RDD as a dictionary of
        /// (value, count) pairs.
        /// 
        /// sc.Parallelize(new int[] { 1, 2, 1, 2, 2 }, 2).CountByValue())
        /// [(1, 2), (2, 3)]
        /// 
        /// </summary>
        /// <returns></returns>
        public Dictionary<T, long> CountByValue()
        {
            return Map<KeyValuePair<T, T>>(v => new KeyValuePair<T, T>(v, default(T))).CountByKey();
        }

        /// <summary>
        /// Take the first num elements of the RDD.
        /// 
        /// It works by first scanning one partition, and use the results from
        /// that partition to estimate the number of additional partitions needed
        /// to satisfy the limit.
        /// 
        /// Translated from the Scala implementation in RDD#take().
        /// 
        /// sc.Parallelize(new int[] { 2, 3, 4, 5, 6 }, 2).Cache().Take(2)))
        /// [2, 3]
        /// sc.Parallelize(new int[] { 2, 3, 4, 5, 6 }, 2).Take(10)
        /// [2, 3, 4, 5, 6]
        /// sc.Parallelize(Enumerable.Range(0, 100), 100).Filter(x => x > 90).Take(3)
        /// [91, 92, 93]
        /// 
        /// </summary>
        /// <param name="num"></param>
        /// <returns></returns>
        public T[] Take(int num)
        {
            List<T> items = new List<T>();
            int totalParts = GetNumPartitions();
            int partsScanned = 0;

            while (items.Count < num && partsScanned < totalParts)
            {
                // The number of partitions to try in this iteration.
                // It is ok for this number to be greater than totalParts because
                // we actually cap it at totalParts in runJob.
                int numPartsToTry = 1;
                if (partsScanned > 0)
                {
                    // If we didn't find any rows after the previous iteration,
                    // quadruple and retry.  Otherwise, interpolate the number of
                    // partitions we need to try, but overestimate it by 50%.
                    // We also cap the estimation in the end.
                    if (items.Count == 0)
                        numPartsToTry = partsScanned * 4;
                    else
                    {
                        // the first paramter of max is >=1 whenever partsScanned >= 2
                        numPartsToTry = (int)(1.5 * num * partsScanned / items.Count) - partsScanned;
                        numPartsToTry = Math.Min(Math.Max(numPartsToTry, 1), partsScanned * 4);
                    }
                }

                int left = num - items.Count;
                IEnumerable<int> partitions = Enumerable.Range(partsScanned, Math.Min(numPartsToTry, totalParts - partsScanned));


                var mappedRDD = MapPartitionsWithIndex<T>(new TakeHelper<T>(left).Execute);
                int port = sparkContext.SparkContextProxy.RunJob(mappedRDD.RddProxy, partitions);

                IEnumerable<T> res = Collect(port).Cast<T>();

                items.AddRange(res);
                partsScanned += numPartsToTry;
            }

            return items.Take(num).ToArray();
        }

        /// <summary>
        /// Return the first element in this RDD.
        /// 
        /// >>> sc.Parallelize(new int[] { 2, 3, 4 }, 2).First()
        /// 2
        /// 
        /// </summary>
        /// <returns></returns>
        public T First()
        {
            return Take(1)[0];
        }

        /// <summary>
        /// Returns true if and only if the RDD contains no elements at all. Note that an RDD
        /// may be empty even when it has at least 1 partition.
        /// 
        /// sc.Parallelize(new int[0], 1).isEmpty()
        /// true
        /// sc.Parallelize(new int[] {1}).isEmpty()
        /// false
        /// </summary>
        /// <returns></returns>
        public bool IsEmpty()
        {
            return GetNumPartitions() == 0 || Take(1).Length == 0;
        }

        /// <summary>
        /// Return each value in C{self} that is not contained in C{other}.
        /// 
        /// var x = sc.Parallelize(new int[] { 1, 2, 3, 4 }, 1)
        /// var y = sc.Parallelize(new int[] { 3 }, 1)
        /// x.Subtract(y).Collect())
        /// [1, 2, 4]
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public RDD<T> Subtract(RDD<T> other, int numPartitions = 0)
        {
            return Map<KeyValuePair<T, T>>(v => new KeyValuePair<T, T>(v, default(T))).SubtractByKey
                (
                    other.Map<KeyValuePair<T, T>>(v => new KeyValuePair<T, T>(v, default(T))),
                    numPartitions
                )
                .Keys();
        }

        /// <summary>
        /// Creates tuples of the elements in this RDD by applying C{f}.
        /// 
        /// sc.Parallelize(new int[] { 1, 2, 3, 4 }, 1).KeyBy(x => x * x).Collect())
        /// (1, 1), (4, 2), (9, 3), (16, 4)
        /// 
        /// </summary>
        /// <typeparam name="K"></typeparam>
        /// <param name="f"></param>
        /// <returns></returns>
        public RDD<KeyValuePair<K, T>> KeyBy<K>(Func<T, K> f)
        {
            return Map<KeyValuePair<K, T>>(new KeyByHelper<K, T>(f).Execute);
        }

        /// <summary>
        /// Return a new RDD that has exactly numPartitions partitions.
        ///
        /// Can increase or decrease the level of parallelism in this RDD.
        /// Internally, this uses a shuffle to redistribute data.
        /// If you are decreasing the number of partitions in this RDD, consider
        /// using `Coalesce`, which can avoid performing a shuffle.
        ///
        /// var rdd = sc.Parallelize(new int[] { 1, 2, 3, 4, 5, 6, 7 }, 4)
        /// rdd.Glom().Collect().Length
        /// 4
        /// rdd.Repartition(2).Glom().Collect().Length
        /// 2
        /// 
        /// </summary>
        /// <param name="numPartitions"></param>
        /// <returns></returns>
        public RDD<T> Repartition(int numPartitions)
        {
            return new RDD<T>(RddProxy.Repartition(numPartitions), sparkContext);
        }

        /// <summary>
        /// Return a new RDD that is reduced into `numPartitions` partitions.
        ///
        /// sc.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 3).Glom().Collect().Length
        /// 3
        /// >>> sc.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 3).Coalesce(1).Glom().Collect().Length
        /// 1
        /// 
        /// </summary>
        /// <param name="numPartitions"></param>
        /// <param name="shuffle"></param>
        /// <returns></returns>
        public RDD<T> Coalesce(int numPartitions, bool shuffle = false)
        {
            return new RDD<T>(RddProxy.Coalesce(numPartitions, shuffle), sparkContext);
        }

        /// <summary>
        /// Zips this RDD with another one, returning key-value pairs with the
        /// first element in each RDD second element in each RDD, etc. Assumes
        /// that the two RDDs have the same number of partitions and the same
        /// number of elements in each partition (e.g. one was made through
        /// a map on the other).
        /// 
        /// var x = sc.parallelize(range(0,5))
        /// var y = sc.parallelize(range(1000, 1005))
        /// x.Zip(y).Collect()
        /// [(0, 1000), (1, 1001), (2, 1002), (3, 1003), (4, 1004)]
        /// 
        /// </summary>
        /// <typeparam name="U"></typeparam>
        /// <param name="other"></param>
        /// <returns></returns>
        public RDD<KeyValuePair<T, U>> Zip<U>(RDD<U> other)
        {
            return new RDD<KeyValuePair<T, U>>(RddProxy.Zip(other.RddProxy), sparkContext, SerializedMode.Pair);
        }

        /// <summary>
        /// Zips this RDD with its element indices.
        /// 
        /// The ordering is first based on the partition index and then the
        /// ordering of items within each partition. So the first item in
        /// the first partition gets index 0, and the last item in the last
        /// partition receives the largest index.
        /// 
        /// This method needs to trigger a spark job when this RDD contains
        /// more than one partitions.
        /// 
        /// sc.Parallelize(new string[] { "a", "b", "c", "d" }, 3).ZipWithIndex().Collect()
        /// [('a', 0), ('b', 1), ('c', 2), ('d', 3)]
        /// 
        /// </summary>
        /// <returns></returns>
        public RDD<KeyValuePair<T, long>> ZipWithIndex()
        {
            int num = GetNumPartitions();
            int[] starts = new int[num];
            if (num > 1)
            {
                var nums = MapPartitionsWithIndex<int>((pid, iter) => new[] { iter.Count() }).Collect();
                for (int i = 0; i < nums.Length - 1; i++)
                    starts[i + 1] = starts[i] + nums[i];
            }
            return MapPartitionsWithIndex<KeyValuePair<T, long>>(new ZipWithIndexHelper<T>(starts).Execute);
        }

        /// <summary>
        /// Zips this RDD with generated unique Long ids.
        /// 
        /// Items in the kth partition will get ids k, n+k, 2*n+k, ..., where
        /// n is the number of partitions. So there may exist gaps, but this
        /// method won't trigger a spark job, which is different from L{zipWithIndex}
        /// 
        /// >>> sc.Parallelize(new string[] { "a", "b", "c", "d" }, 1).ZipWithIndex().Collect()
        /// [('a', 0), ('b', 1), ('c', 4), ('d', 2), ('e', 5)]
        /// 
        /// </summary>
        /// <returns></returns>
        public RDD<KeyValuePair<T, long>> ZipWithUniqueId()
        {
            int num = GetNumPartitions();
            return MapPartitionsWithIndex<KeyValuePair<T, long>>(new ZipWithUniqueIdHelper<T>(num).Execute);
        }

        /// <summary>
        /// Assign a name to this RDD.
        /// 
        /// >>> rdd1 = sc.parallelize([1, 2])
        /// >>> rdd1.setName('RDD1').name()
        /// u'RDD1'
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public RDD<T> SetName(string name)
        {
            RddProxy.SetName(name);
            return this;
        }

        /// <summary>
        /// A description of this RDD and its recursive dependencies for debugging.
        /// </summary>
        /// <returns></returns>
        public string ToDebugString()
        {
            return RddProxy.ToDebugString();
        }

        /// <summary>
        /// Get the RDD's current storage level.
        /// 
        /// >>> rdd1 = sc.parallelize([1,2])
        /// >>> rdd1.getStorageLevel()
        /// StorageLevel(False, False, False, False, 1)
        /// >>> print(rdd1.getStorageLevel())
        /// Serialized 1x Replicated
        /// </summary>
        /// <returns></returns>
        public StorageLevel GetStorageLevel()
        {
            return RddProxy.GetStorageLevel();
        }

        /// <summary>
        /// Return an iterator that contains all of the elements in this RDD.
        /// The iterator will consume as much memory as the largest partition in this RDD.
        /// sc.Parallelize(Enumerable.Range(0, 10), 1).ToLocalIterator()
        /// [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        /// </summary>
        /// <returns></returns>
        public IEnumerable<T> ToLocalIterator()
        {
            foreach (int partition in Enumerable.Range(0, GetNumPartitions()))
            {
                var mappedRDD = MapPartitionsWithIndex<T>((pid, iter) => iter);
                int port = sparkContext.SparkContextProxy.RunJob(mappedRDD.RddProxy, Enumerable.Range(partition, 1));
                foreach (T row in Collect(port))
                    yield return row;
            }
        }

        /// <summary>
        /// Internal method exposed for Random Splits in DataFrames. Samples an RDD given a probability range.
        /// </summary>
        /// <param name="lb">lower bound to use for the Bernoulli sampler</param>
        /// <param name="ub">upper bound to use for the Bernoulli sampler</param>
        /// <param name="seed">the seed for the Random number generator</param>
        /// <returns>A random sub-sample of the RDD without replacement.</returns>
        internal RDD<T> RandomSampleWithRange(double lb, double ub, long seed)
        {
            return new RDD<T>(RddProxy.RandomSampleWithRange(lb, ub, seed), sparkContext);
        }
    }

    /// <summary>
    /// Some useful utility functions for <c>RDD{string}</c>
    /// </summary>
    public static class StringRDDFunctions
    {
        /// <summary>
        /// Save this RDD as a text file, using string representations of elements.
        /// </summary>
        /// <param name="self"></param>
        /// <param name="path">path to text file</param>
        /// <param name="compressionCodecClass">(None by default) string i.e. "org.apache.hadoop.io.compress.GzipCodec"</param>
        public static void SaveAsTextFile(this RDD<string> self, string path, string compressionCodecClass = null)
        {
            var keyed = self.MapPartitionsWithIndex((pid, iter) => iter.Select(x => x));
            keyed.serializedMode = SerializedMode.String;
            keyed.RddProxy.SaveAsTextFile(path, compressionCodecClass);
        }
    }

    /// <summary>
    /// Some useful utility functions for RDD's containing IComparable values.
    /// </summary>
    public static class ComparableRDDFunctions
    {
        /// <summary>
        /// Find the maximum item in this RDD.
        /// 
        /// sc.Parallelize(new double[] { 1.0, 5.0, 43.0, 10.0 }, 2).Max()
        /// 43.0
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <returns></returns>
        public static T Max<T>(this RDD<T> self) where T : IComparable<T>
        {
            return self.MapPartitionsWithIndex<T>((pid, iter) => { return new List<T> { iter.Max() }; }).Collect().Max();
        }

        /// <summary>
        /// Find the minimum item in this RDD.
        /// 
        /// sc.Parallelize(new double[] { 2.0, 5.0, 43.0, 10.0 }, 2).Min()
        /// >>> rdd.min()
        /// 2.0
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <returns></returns>
        public static T Min<T>(this RDD<T> self) where T : IComparable<T>
        {
            return self.MapPartitionsWithIndex<T>((pid, iter) => { return new List<T> { iter.Min() }; }).Collect().Min();
        }

        /// <summary>
        /// Get the N elements from a RDD ordered in ascending order or as
        /// specified by the optional key function.
        /// 
        /// sc.Parallelize(new int[] { 10, 1, 2, 9, 3, 4, 5, 6, 7 }, 2).TakeOrdered(6)
        /// [1, 2, 3, 4, 5, 6]
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="num"></param>
        /// <returns></returns>
        public static T[] TakeOrdered<T>(this RDD<T> self, int num) where T : IComparable<T>
        {
            return self.MapPartitionsWithIndex<T>(new TakeOrderedHelper<T>(num).Execute).Collect().OrderBy(x => x).Take(num).ToArray();
        }

        /// <summary>
        /// Get the top N elements from a RDD.
        /// 
        /// Note: It returns the list sorted in descending order.
        /// 
        /// sc.Parallelize(new int[] { 2, 3, 4, 5, 6 }, 2).Top(3)
        /// [6, 5, 4]
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="num"></param>
        /// <returns></returns>
        public static T[] Top<T>(this RDD<T> self, int num) where T : IComparable<T>
        {
            return self.MapPartitionsWithIndex<T>(new TopHelper<T>(num).Execute).Collect().OrderByDescending(x => x).Take(num).ToArray();
        }
    }

    /// <summary>
    /// This class is used to wrap Func of specific parameter types into Func of dynamic parameter types.  The wrapping is done to use dynamic types 
    /// in PipelinedRDD which helps keeping the deserialization of Func simple at the worker side.
    /// 
    /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
    /// on the serializability of compiler generated types 
    /// </summary>
    [Serializable]
    public class DynamicTypingWrapper<I, O>
    {
        private readonly Func<int, IEnumerable<I>, IEnumerable<O>> func;
        internal DynamicTypingWrapper(Func<int, IEnumerable<I>, IEnumerable<O>> f)
        {
            func = f;
        }

        internal IEnumerable<dynamic> Execute(int val, IEnumerable<dynamic> inputValues)
        {
            return func(val, inputValues.Cast<I>()).Cast<dynamic>();
        }
    }

    /// <summary>
    /// This class is used to wrap Func of specific parameter types into Func of dynamic parameter types.  The wrapping is done to use dynamic types 
    /// in PipelinedRDD which helps keeping the deserialization of Func simple at the worker side.
    /// 
    /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
    /// on the serializability of compiler generated types 
    /// </summary>
    [Serializable]
    public class DynamicTypingWrapper<K, V, W1, W2, W3>
    {
        internal IEnumerable<dynamic> Execute(int val, IEnumerable<dynamic> inputValues)
        {
            // constructor and property reflection is much slower than 'new' operator
            return inputValues.Select(x => 
                {
                    K key;
                    dynamic value;
                    if (x is KeyValuePair<K, V>)
                    {
                        key = ((KeyValuePair<K, V>)x).Key;
                        value = ((KeyValuePair<K, V>)x).Value;
                    }
                    else if (x is KeyValuePair<K, W1>)
                    {
                        key = ((KeyValuePair<K, W1>)x).Key;
                        value = ((KeyValuePair<K, W1>)x).Value;
                    }
                    else if (x is KeyValuePair<K, W2>)
                    {
                        key = ((KeyValuePair<K, W2>)x).Key;
                        value = ((KeyValuePair<K, W2>)x).Value;
                    }
                    else
                    {
                        key = ((KeyValuePair<K, W3>)x).Key;
                        value = ((KeyValuePair<K, W3>)x).Value;
                    }
                    return new KeyValuePair<K, dynamic>(key, value);
                })
                .Cast<dynamic>();
        }
    }

    /// <summary>
    /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
    /// on the serializability of compiler generated types
    /// </summary>
    [Serializable]
    internal class FilterHelper<I>
    {
        private readonly Func<I, bool> func;
        internal FilterHelper(Func<I, bool> f)
        {
            func = f;
        }

        internal IEnumerable<I> Execute(int pid, IEnumerable<I> input)
        {
            return input.Where(func);
        }
    }

    /// <summary>
    /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
    /// on the serializability of compiler generated types
    /// </summary>
    [Serializable]
    internal class MapHelper<I, O>
    {
        private readonly Func<I, O> func;
        internal MapHelper(Func<I, O> f)
        {
            func = f;
        }

        internal IEnumerable<O> Execute(int pid, IEnumerable<I> input)
        {
            return input.Select(func);
        }
    }

    /// <summary>
    /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
    /// on the serializability of compiler generated types
    /// </summary>
    [Serializable]
    internal class FlatMapHelper<I, O>
    {
        private readonly Func<I, IEnumerable<O>> func;
        internal FlatMapHelper(Func<I, IEnumerable<O>> f)
        {
            func = f;
        }

        internal IEnumerable<O> Execute(int pid, IEnumerable<I> input)
        {
            return input.SelectMany(func);
        }
    }

    /// <summary>
    /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
    /// on the serializability of compiler generated types
    /// </summary>
    [Serializable]
    internal class MapPartitionsHelper<T, U>
    {
        private readonly Func<IEnumerable<T>, IEnumerable<U>> func;
        internal MapPartitionsHelper(Func<IEnumerable<T>, IEnumerable<U>> f)
        {
            func = f;
        }

        internal IEnumerable<U> Execute(int pid, IEnumerable<T> input)
        {
            return func(input);
        }
    }

    /// <summary>
    /// This class is defined explicitly instead of using anonymous method as delegate to prevent C# compiler from generating
    /// private anonymous type that is not serializable. Since the delegate has to be serialized and sent to the Spark workers
    /// for execution, it is necessary to have the type marked [Serializable]. This class is to work around the limitation
    /// on the serializability of compiler generated types
    /// </summary>
    [Serializable]
    internal class ReduceHelper<T1>
    {
        private readonly Func<T1, T1, T1> func;
        internal ReduceHelper(Func<T1, T1, T1> f)
        {
            func = f;
        }

        internal IEnumerable<T1> Execute(int pid, IEnumerable<T1> input)
        {
            yield return input.DefaultIfEmpty().Aggregate(func);
        }
    }
    [Serializable]
    internal class GlomHelper<T>
    {
        internal IEnumerable<T[]> Execute(int pid, IEnumerable<T> input)
        {
            yield return input.ToArray();
        }
    }
    [Serializable]
    internal class ForeachHelper<T>
    {
        private readonly Action<T> func;
        internal ForeachHelper(Action<T> f)
        {
            func = f;
        }

        internal IEnumerable<T> Execute(int pid, IEnumerable<T> input)
        {
            foreach (var item in input)
            {
                func(item);
                yield return default(T);
            }
        }
    }
    [Serializable]
    internal class ForeachPartitionHelper<T>
    {
        private readonly Action<IEnumerable<T>> func;
        internal ForeachPartitionHelper(Action<IEnumerable<T>> f)
        {
            func = f;
        }

        internal IEnumerable<T> Execute(int pid, IEnumerable<T> input)
        {
            func(input);
            yield return default(T);
        }
    }
    [Serializable]
    internal class KeyByHelper<K, T>
    {
        private readonly Func<T, K> func;
        internal KeyByHelper(Func<T, K> f)
        {
            func = f;
        }

        internal KeyValuePair<K, T> Execute(T input)
        {
            return new KeyValuePair<K, T>(func(input), input);
        }
    }
    [Serializable]
    internal class AggregateHelper<U, T>
    {
        private readonly U zeroValue;
        private readonly Func<U, T, U> op;
        internal AggregateHelper(U zeroValue, Func<U, T, U> op)
        {
            this.zeroValue = zeroValue;
            this.op = op;
        }

        internal IEnumerable<U> Execute(int pid, IEnumerable<T> input)
        {
            yield return input.Aggregate(zeroValue, op);
        }
    }
    [Serializable]
    internal class TreeAggregateHelper<U>
    {
        private readonly int numPartitions;
        internal TreeAggregateHelper(int numPartitions)
        {
            this.numPartitions = numPartitions;
        }
        internal IEnumerable<KeyValuePair<int, U>> Execute(int pid, IEnumerable<U> input)
        {
            return input.Select(x => new KeyValuePair<int, U>(pid % numPartitions, x));
        }
    }
    [Serializable]
    internal class TreeReduceHelper<T>
    {
        private readonly Func<T, T, T> func;
        internal TreeReduceHelper(Func<T, T, T> func)
        {
            this.func = func;
        }
        internal KeyValuePair<T, bool> Execute(KeyValuePair<T, bool> x, KeyValuePair<T, bool> y)
        {
            if (x.Value)
                return y;
            else if (y.Value)
                return x;
            else
                return new KeyValuePair<T, bool>(func(x.Key, y.Key), false);
        }
    }
    [Serializable]
    internal class TakeHelper<T>
    {
        private readonly int num;
        internal TakeHelper(int num)
        {
            this.num = num;
        }
        internal IEnumerable<T> Execute(int pid, IEnumerable<T> input)
        {
            return input.Take(num);
        }
    }
    [Serializable]
    internal class TakeOrderedHelper<T>
    {
        private readonly int num;
        internal TakeOrderedHelper(int num)
        {
            this.num = num;
        }
        internal IEnumerable<T> Execute(int pid, IEnumerable<T> input)
        {
            return input.OrderBy(x => x).Take(num);
        }
    }
    [Serializable]
    internal class TopHelper<T>
    {
        private readonly int num;
        internal TopHelper(int num)
        {
            this.num = num;
        }
        internal IEnumerable<T> Execute(int pid, IEnumerable<T> input)
        {
            return input.OrderByDescending(x => x).Take(num);
        }
    }
    [Serializable]
    internal class ZipWithUniqueIdHelper<T>
    {
        private readonly int num;
        internal ZipWithUniqueIdHelper(int num)
        {
            this.num = num;
        }
        internal IEnumerable<KeyValuePair<T, long>> Execute(int pid, IEnumerable<T> input)
        {
            long l = 0;
            foreach (var item in input)
            {
                yield return new KeyValuePair<T, long>(item, (l++) * num + pid);
            }
        }
    }
    [Serializable]
    internal class ZipWithIndexHelper<T>
    {
        private readonly int[] starts;
        internal ZipWithIndexHelper(int[] starts)
        {
            this.starts = starts;
        }
        internal IEnumerable<KeyValuePair<T, long>> Execute(int pid, IEnumerable<T> input)
        {
            long l = 0;
            foreach (var item in input)
            {
                yield return new KeyValuePair<T, long>(item, (l++) + starts[pid]);
            }
        }
    }
    internal enum SerializedMode
    {
        None,
        String,
        Byte,
        Pair,
        Row
    }

}
