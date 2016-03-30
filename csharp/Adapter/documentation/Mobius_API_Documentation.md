<?xml version="1.0" encoding="utf-8"?>
##<center><H1><font color="darkorchid4">Mobius API Documentation</font></H1></center>
###<font color="#68228B">Microsoft.Spark.CSharp.Core.Accumulator</font>
####Summary
  
            
            A shared variable that can be accumulated, i.e., has a commutative and associative "add"
            operation. Worker tasks on a Spark cluster can add values to an Accumulator with the +=
            operator, but only the driver program is allowed to access its value, using Value.
            Updates from the workers get propagated automatically to the driver program.
            
            While  supports accumulators for primitive data types like int and
            float, users can also define accumulators for custom types by providing a custom
             object. Refer to the doctest of this module for an example.
            
            See python implementation in accumulators.py, worker.py, PythonRDD.scala
            
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue"></font></td><td>Adds a term to this accumulator's value</td></tr><tr><td><font color="blue"></font></td><td>The += operator; adds a term to this accumulator's value</td></tr><tr><td><font color="blue"></font></td><td>Creates and returns a string representation of the current accumulator</td></tr><tr><td><font color="blue"></font></td><td>Provide a "zero value" for the type</td></tr><tr><td><font color="blue"></font></td><td>Add two values of the accumulator's data type, returning a new value;</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.Accumulator`1</font>
####Summary
  
            
            A generic version of  where the element type is specified by the driver program.
            
            The type of element in the accumulator.
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Add</font></td><td>Adds a term to this accumulator's value</td></tr><tr><td><font color="blue">op_Addition</font></td><td>The += operator; adds a term to this accumulator's value</td></tr><tr><td><font color="blue">ToString</font></td><td>Creates and returns a string representation of the current accumulator</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.AccumulatorParam`1</font>
####Summary
  
            
            An AccumulatorParam that uses the + operators to add values. Designed for simple types
            such as integers, floats, and lists. Requires the zero value for the underlying type
            as a parameter.
            
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Zero</font></td><td>Provide a "zero value" for the type</td></tr><tr><td><font color="blue">AddInPlace</font></td><td>Add two values of the accumulator's data type, returning a new value;</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.AccumulatorServer</font>
####Summary
  
            
            A simple TCP server that intercepts shutdown() in order to interrupt
            our continuous polling on the handler.
            
        
###<font color="#68228B">Microsoft.Spark.CSharp.Core.Broadcast</font>
####Summary
  
            
            A broadcast variable created with SparkContext.Broadcast().
            Access its value through Value.
            
            var b = sc.Broadcast(new int[] {1, 2, 3, 4, 5})
            b.Value
            [1, 2, 3, 4, 5]
            sc.Parallelize(new in[] {0, 0}).FlatMap(x: b.Value).Collect()
            [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
            b.Unpersist()
            
            See python implementation in broadcast.py, worker.py, PythonRDD.scala
            
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue"></font></td><td>Delete cached copies of this broadcast on the executors.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.Broadcast`1</font>
####Summary
  
            
            A generic version of  where the element can be specified.
            
            The type of element in Broadcast
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Unpersist</font></td><td>Delete cached copies of this broadcast on the executors.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.Option`1</font>
####Summary
  
            
            Container for an optional value of type T. If the value of type T is present, the Option.IsDefined is TRUE and GetValue() return the value. 
            If the value is absent, the Option.IsDefined is FALSE, exception will be thrown when calling GetValue().
            
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">GetValue</font></td><td>Returns the value of the option if Option.IsDefined is TRUE; otherwise, throws an .</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.Partitioner</font>
####Summary
  
            
            An object that defines how the elements in a key-value pair RDD are partitioned by key.
            Maps each key to a partition ID, from 0 to "numPartitions - 1".
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.RDDCollector</font>
####Summary
  
            
            Used for collect operation on RDD
            
        
###<font color="#68228B">Microsoft.Spark.CSharp.Core.IRDDCollector</font>
####Summary
  
            
            Interface for collect operation on RDD
            
        
###<font color="#68228B">Microsoft.Spark.CSharp.Core.DoubleRDDFunctions</font>
####Summary
  
            
            Extra functions available on RDDs of Doubles through an implicit conversion. 
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Sum</font></td><td>Add up the elements in this RDD. sc.Parallelize(new double[] {1.0, 2.0, 3.0}).Sum() 6.0</td></tr><tr><td><font color="blue">Stats</font></td><td>Return a object that captures the mean, variance and count of the RDD's elements in one operation.</td></tr><tr><td><font color="blue">Histogram</font></td><td>Compute a histogram using the provided buckets. The buckets are all open to the right except for the last which is closed. e.g. [1,10,20,50] means the buckets are [1,10) [10,20) [20,50], which means 1&lt;=x&lt;10, 10&lt;=x&lt;20, 20&lt;=x&lt;=50. And on the input of 1 and 50 we would have a histogram of 1,0,1. If your histogram is evenly spaced (e.g. [0, 10, 20, 30]), this can be switched from an O(log n) inseration to O(1) per element(where n = # buckets). Buckets must be sorted and not contain any duplicates, must be at least two elements. If `buckets` is a number, it will generates buckets which are evenly spaced between the minimum and maximum of the RDD. For example, if the min value is 0 and the max is 100, given buckets as 2, the resulting buckets will be [0,50) [50,100]. buckets must be at least 1 If the RDD contains infinity, NaN throws an exception If the elements in RDD do not vary (max == min) always returns a single bucket. It will return an tuple of buckets and histogram. &gt;&gt;&gt; rdd = sc.parallelize(range(51)) &gt;&gt;&gt; rdd.histogram(2) ([0, 25, 50], [25, 26]) &gt;&gt;&gt; rdd.histogram([0, 5, 25, 50]) ([0, 5, 25, 50], [5, 20, 26]) &gt;&gt;&gt; rdd.histogram([0, 15, 30, 45, 60]) # evenly spaced buckets ([0, 15, 30, 45, 60], [15, 15, 15, 6]) &gt;&gt;&gt; rdd = sc.parallelize(["ab", "ac", "b", "bd", "ef"]) &gt;&gt;&gt; rdd.histogram(("a", "b", "c")) (('a', 'b', 'c'), [2, 2])</td></tr><tr><td><font color="blue">Mean</font></td><td>Compute the mean of this RDD's elements. sc.Parallelize(new double[]{1, 2, 3}).Mean() 2.0</td></tr><tr><td><font color="blue">Variance</font></td><td>Compute the variance of this RDD's elements. sc.Parallelize(new double[]{1, 2, 3}).Variance() 0.666...</td></tr><tr><td><font color="blue">Stdev</font></td><td>Compute the standard deviation of this RDD's elements. sc.Parallelize(new double[]{1, 2, 3}).Stdev() 0.816...</td></tr><tr><td><font color="blue">SampleStdev</font></td><td>Compute the sample standard deviation of this RDD's elements (which corrects for bias in estimating the standard deviation by dividing by N-1 instead of N). sc.Parallelize(new double[]{1, 2, 3}).SampleStdev() 1.0</td></tr><tr><td><font color="blue">SampleVariance</font></td><td>Compute the sample variance of this RDD's elements (which corrects for bias in estimating the variance by dividing by N-1 instead of N). sc.Parallelize(new double[]{1, 2, 3}).SampleVariance() 1.0</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.OrderedRDDFunctions</font>
####Summary
  
            
            Extra functions available on RDDs of (key, value) pairs where the key is sortable through
            a function to sort the key.
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">SortByKey``2</font></td><td>Sorts this RDD, which is assumed to consist of KeyValuePair pairs.</td></tr><tr><td><font color="blue">SortByKey``3</font></td><td>Sorts this RDD, which is assumed to consist of KeyValuePairs. If key is type of string, case is sensitive.</td></tr><tr><td><font color="blue">repartitionAndSortWithinPartitions``2</font></td><td>Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys. This is more efficient than calling `repartition` and then sorting within each partition because it can push the sorting down into the shuffle machinery.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.PairRDDFunctions</font>
####Summary
  
            
            operations only available to KeyValuePair RDD
            
            See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">CollectAsMap``2</font></td><td>Return the key-value pairs in this RDD to the master as a dictionary. var m = sc.Parallelize(new[] { new (1, 2), new (3, 4) }, 1).CollectAsMap() m[1] 2 m[3] 4</td></tr><tr><td><font color="blue">Keys``2</font></td><td>Return an RDD with the keys of each tuple. &gt;&gt;&gt; m = sc.Parallelize(new[] { new (1, 2), new (3, 4) }, 1).Keys().Collect() [1, 3]</td></tr><tr><td><font color="blue">Values``2</font></td><td>Return an RDD with the values of each tuple. &gt;&gt;&gt; m = sc.Parallelize(new[] { new (1, 2), new (3, 4) }, 1).Values().Collect() [2, 4]</td></tr><tr><td><font color="blue">ReduceByKey``2</font></td><td>Merge the values for each key using an associative reduce function. This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a "combiner" in MapReduce. Output will be hash-partitioned with partitions, or the default parallelism level if is not specified. sc.Parallelize(new[] { new ("a", 1), new ("b", 1), new ("a", 1) }, 2) .ReduceByKey((x, y) =&gt; x + y).Collect() [('a', 2), ('b', 1)]</td></tr><tr><td><font color="blue">ReduceByKeyLocally``2</font></td><td>Merge the values for each key using an associative reduce function, but return the results immediately to the master as a dictionary. This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a "combiner" in MapReduce. sc.Parallelize(new[] { new ("a", 1), new ("b", 1), new ("a", 1) }, 2) .ReduceByKeyLocally((x, y) =&gt; x + y).Collect() [('a', 2), ('b', 1)]</td></tr><tr><td><font color="blue">CountByKey``2</font></td><td>Count the number of elements for each key, and return the result to the master as a dictionary. sc.Parallelize(new[] { new ("a", 1), new ("b", 1), new ("a", 1) }, 2) .CountByKey((x, y) =&gt; x + y).Collect() [('a', 2), ('b', 1)]</td></tr><tr><td><font color="blue">Join``3</font></td><td>Return an RDD containing all pairs of elements with matching keys in this RDD and . Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in this RDD and (k, v2) is in . Performs a hash join across the cluster. var l = sc.Parallelize( new[] { new ("a", 1), new ("b", 4) }, 1); var r = sc.Parallelize( new[] { new ("a", 2), new ("a", 3) }, 1); var m = l.Join(r, 2).Collect(); [('a', (1, 2)), ('a', (1, 3))]</td></tr><tr><td><font color="blue">LeftOuterJoin``3</font></td><td>Perform a left outer join of this RDD and . For each element (k, v) in this RDD, the resulting RDD will either contain all pairs (k, (v, Option)) for w in , where Option.IsDefined is TRUE, or the pair (k, (v, Option)) if no elements in have key k, where Option.IsDefined is FALSE. Hash-partitions the resulting RDD into the given number of partitions. var l = sc.Parallelize( new[] { new ("a", 1), new ("b", 4) }, 1); var r = sc.Parallelize( new[] { new ("a", 2) }, 1); var m = l.LeftOuterJoin(r).Collect(); [('a', (1, 2)), ('b', (4, Option))] * Option.IsDefined = FALSE</td></tr><tr><td><font color="blue">RightOuterJoin``3</font></td><td>Perform a right outer join of this RDD and . For each element (k, w) in , the resulting RDD will either contain all pairs (k, (Option, w)) for v in this, where Option.IsDefined is TRUE, or the pair (k, (Option, w)) if no elements in this RDD have key k, where Option.IsDefined is FALSE. Hash-partitions the resulting RDD into the given number of partitions. var l = sc.Parallelize( new[] { new ("a", 2) }, 1); var r = sc.Parallelize( new[] { new ("a", 1), new ("b", 4) }, 1); var m = l.RightOuterJoin(r).Collect(); [('a', (2, 1)), ('b', (Option, 4))] * Option.IsDefined = FALSE</td></tr><tr><td><font color="blue">FullOuterJoin``3</font></td><td>Perform a full outer join of this RDD and . For each element (k, v) in this RDD, the resulting RDD will either contain all pairs (k, (v, w)) for w in , or the pair (k, (v, None)) if no elements in have key k. Similarly, for each element (k, w) in , the resulting RDD will either contain all pairs (k, (v, w)) for v in this RDD, or the pair (k, (None, w)) if no elements in this RDD have key k. Hash-partitions the resulting RDD into the given number of partitions. var l = sc.Parallelize( new[] { new ("a", 1), ("b", 4) }, 1); var r = sc.Parallelize( new[] { new ("a", 2), new ("c", 8) }, 1); var m = l.FullOuterJoin(r).Collect(); [('a', (1, 2)), ('b', (4, None)), ('c', (None, 8))]</td></tr><tr><td><font color="blue">PartitionBy``2</font></td><td>Return a copy of the RDD partitioned using the specified partitioner. sc.Parallelize(new[] { 1, 2, 3, 4, 2, 4, 1 }, 1).Map(x =&gt; new (x, x)).PartitionBy(3).Glom().Collect()</td></tr><tr><td><font color="blue">CombineByKey``3</font></td><td># TODO: add control over map-side aggregation Generic function to combine the elements for each key using a custom set of aggregation functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C. Note that V and C can be different -- for example, one might group an RDD of type (Int, Int) into an RDD of type (Int, List[Int]). Users provide three functions: - , which turns a V into a C (e.g., creates a one-element list) - , to merge a V into a C (e.g., adds it to the end of a list) - , to combine two C's into a single one. In addition, users can control the partitioning of the output RDD. sc.Parallelize( new[] { new ("a", 1), new ("b", 1), new ("a", 1) }, 2) .CombineByKey(() =&gt; string.Empty, (x, y) =&gt; x + y.ToString(), (x, y) =&gt; x + y).Collect() [('a', '11'), ('b', '1')]</td></tr><tr><td><font color="blue">AggregateByKey``3</font></td><td>Aggregate the values of each key, using given combine functions and a neutral "zero value". This function can return a different result type, U, than the type of the values in this RDD, V. Thus, we need one operation for merging a V into a U and one operation for merging two U's, The former operation is used for merging values within a partition, and the latter is used for merging values between partitions. To avoid memory allocation, both of these functions are allowed to modify and return their first argument instead of creating a new U. sc.Parallelize( new[] { new ("a", 1), new ("b", 1), new ("a", 1) }, 2) .CombineByKey(() =&gt; string.Empty, (x, y) =&gt; x + y.ToString(), (x, y) =&gt; x + y).Collect() [('a', 2), ('b', 1)]</td></tr><tr><td><font color="blue">FoldByKey``2</font></td><td>Merge the values for each key using an associative function "func" and a neutral "zeroValue" which may be added to the result an arbitrary number of times, and must not change the result (e.g., 0 for addition, or 1 for multiplication.). sc.Parallelize( new[] { new ("a", 1), new ("b", 1), new ("a", 1) }, 2) .CombineByKey(() =&gt; string.Empty, (x, y) =&gt; x + y.ToString(), (x, y) =&gt; x + y).Collect() [('a', 2), ('b', 1)]</td></tr><tr><td><font color="blue">GroupByKey``2</font></td><td>Group the values for each key in the RDD into a single sequence. Hash-partitions the resulting RDD with numPartitions partitions. Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will provide much better performance. sc.Parallelize( new[] { new ("a", 1), new ("b", 1), new ("a", 1) }, 2) .GroupByKey().MapValues(l =&gt; string.Join(" ", l)).Collect() [('a', [1, 1]), ('b', [1])]</td></tr><tr><td><font color="blue">MapValues``3</font></td><td>Pass each value in the key-value pair RDD through a map function without changing the keys; this also retains the original RDD's partitioning. sc.Parallelize( new[] { new ("a", new[]{"apple", "banana", "lemon"}), new ("b", new[]{"grapes"}) }, 2) .MapValues(x =&gt; x.Length).Collect() [('a', 3), ('b', 1)]</td></tr><tr><td><font color="blue">FlatMapValues``3</font></td><td>Pass each value in the key-value pair RDD through a flatMap function without changing the keys; this also retains the original RDD's partitioning. x = sc.Parallelize( new[] { new ("a", new[]{"x", "y", "z"}), new ("b", new[]{"p", "r"}) }, 2) .FlatMapValues(x =&gt; x).Collect() [('a', 'x'), ('a', 'y'), ('a', 'z'), ('b', 'p'), ('b', 'r')]</td></tr><tr><td><font color="blue">GroupWith``3</font></td><td>For each key k in this RDD or , return a resulting RDD that contains a tuple with the list of values for that key in this RDD as well as . var x = sc.Parallelize(new[] { new ("a", 1), new ("b", 4) }, 2); var y = sc.Parallelize(new[] { new ("a", 2) }, 1); x.GroupWith(y).Collect(); [('a', ([1], [2])), ('b', ([4], []))]</td></tr><tr><td><font color="blue">GroupWith``4</font></td><td>var x = sc.Parallelize(new[] { new ("a", 5), new ("b", 6) }, 2); var y = sc.Parallelize(new[] { new ("a", 1), new ("b", 4) }, 2); var z = sc.Parallelize(new[] { new ("a", 2) }, 1); x.GroupWith(y, z).Collect();</td></tr><tr><td><font color="blue">GroupWith``5</font></td><td>var x = sc.Parallelize(new[] { new ("a", 5), new ("b", 6) }, 2); var y = sc.Parallelize(new[] { new ("a", 1), new ("b", 4) }, 2); var z = sc.Parallelize(new[] { new ("a", 2) }, 1); var w = sc.Parallelize(new[] { new ("b", 42) }, 1); var m = x.GroupWith(y, z, w).MapValues(l =&gt; string.Join(" ", l.Item1) + " : " + string.Join(" ", l.Item2) + " : " + string.Join(" ", l.Item3) + " : " + string.Join(" ", l.Item4)).Collect();</td></tr><tr><td><font color="blue">SubtractByKey``3</font></td><td>Return each (key, value) pair in this RDD that has no pair with matching key in . var x = sc.Parallelize(new[] { new ("a", 1), new ("b", 4), new ("b", 5), new ("a", 2) }, 2); var y = sc.Parallelize(new[] { new ("a", 3), new ("c", null) }, 2); x.SubtractByKey(y).Collect(); [('b', 4), ('b', 5)]</td></tr><tr><td><font color="blue">Lookup``2</font></td><td>Return the list of values in the RDD for key `key`. This operation is done efficiently if the RDD has a known partitioner by only searching the partition that the key maps to. &gt;&gt;&gt; l = range(1000) &gt;&gt;&gt; rdd = sc.Parallelize(Enumerable.Range(0, 1000).Zip(Enumerable.Range(0, 1000), (x, y) =&gt; new (x, y)), 10) &gt;&gt;&gt; rdd.lookup(42) [42]</td></tr><tr><td><font color="blue">SaveAsNewAPIHadoopDataset``2</font></td><td>Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the new Hadoop OutputFormat API (mapreduce package). Keys/values are converted for output using either user specified converters or, by default, org.apache.spark.api.python.JavaToWritableConverter.</td></tr><tr><td><font color="blue">SaveAsNewAPIHadoopFile``2</font></td><td></td></tr><tr><td><font color="blue">SaveAsHadoopDataset``2</font></td><td>Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the old Hadoop OutputFormat API (mapred package). Keys/values are converted for output using either user specified converters or, by default, org.apache.spark.api.python.JavaToWritableConverter.</td></tr><tr><td><font color="blue">SaveAsHadoopFile``2</font></td><td>Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the old Hadoop OutputFormat API (mapred package). Key and value types will be inferred if not specified. Keys and values are converted for output using either user specified converters or org.apache.spark.api.python.JavaToWritableConverter. The is applied on top of the base Hadoop conf associated with the SparkContext of this RDD to create a merged Hadoop MapReduce job configuration for saving the data.</td></tr><tr><td><font color="blue">SaveAsSequenceFile``2</font></td><td>Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the org.apache.hadoop.io.Writable types that we convert from the RDD's key and value types. The mechanism is as follows: 1. Pyrolite is used to convert pickled Python RDD into RDD of Java objects. 2. Keys and values of this Java RDD are converted to Writables and written out.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.RDD`1</font>
####Summary
  
            
            Represents a Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable, 
            partitioned collection of elements that can be operated on in parallel
            
            See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD
            
            Type of the RDD
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Cache</font></td><td>Persist this RDD with the default storage level .</td></tr><tr><td><font color="blue">Persist</font></td><td>Set this RDD's storage level to persist its values across operations after the first time it is computed. This can only be used to assign a new storage level if the RDD does not have a storage level set yet. If no storage level is specified defaults to . sc.Parallelize(new string[] {"b", "a", "c").Persist().isCached True</td></tr><tr><td><font color="blue">Unpersist</font></td><td>Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.</td></tr><tr><td><font color="blue">Checkpoint</font></td><td>Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint directory set with ) and all references to its parent RDDs will be removed. This function must be called before any job has been executed on this RDD. It is strongly recommended that this RDD is persisted in memory, otherwise saving it on a file will require recomputation.</td></tr><tr><td><font color="blue">Map``1</font></td><td>Return a new RDD by applying a function to each element of this RDD. sc.Parallelize(new string[]{"b", "a", "c"}, 1).Map(x =&gt; new (x, 1)).Collect() [('a', 1), ('b', 1), ('c', 1)]</td></tr><tr><td><font color="blue">FlatMap``1</font></td><td>Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results. sc.Parallelize(new int[] {2, 3, 4}, 1).FlatMap(x =&gt; Enumerable.Range(1, x - 1)).Collect() [1, 1, 1, 2, 2, 3]</td></tr><tr><td><font color="blue">MapPartitions``1</font></td><td>Return a new RDD by applying a function to each partition of this RDD. sc.Parallelize(new int[] {1, 2, 3, 4}, 2).MapPartitions(iter =&gt; new[]{iter.Sum(x =&gt; (x as decimal?))}).Collect() [3, 7]</td></tr><tr><td><font color="blue">MapPartitionsWithIndex``1</font></td><td>Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition. ((pid, iter) =&gt; (double)pid).Sum() 6</td></tr><tr><td><font color="blue">Filter</font></td><td>Return a new RDD containing only the elements that satisfy a predicate. sc.Parallelize(new int[]{1, 2, 3, 4, 5}, 1).Filter(x =&gt; x % 2 == 0).Collect() [2, 4]</td></tr><tr><td><font color="blue">Distinct</font></td><td>Return a new RDD containing the distinct elements in this RDD. &gt;&gt;&gt; sc.Parallelize(new int[] {1, 1, 2, 3}, 1).Distinct().Collect() [1, 2, 3]</td></tr><tr><td><font color="blue">Sample</font></td><td>Return a sampled subset of this RDD. var rdd = sc.Parallelize(Enumerable.Range(0, 100), 4) 6 &lt;= rdd.Sample(False, 0.1, 81).count() &lt;= 14 true</td></tr><tr><td><font color="blue">RandomSplit</font></td><td>Randomly splits this RDD with the provided weights. var rdd = sc.Parallelize(Enumerable.Range(0, 500), 1) var rdds = rdd.RandomSplit(new double[] {2, 3}, 17) 150 &lt; rdds[0].Count() &lt; 250 250 &lt; rdds[1].Count() &lt; 350</td></tr><tr><td><font color="blue">TakeSample</font></td><td>Return a fixed-size sampled subset of this RDD. var rdd = sc.Parallelize(Enumerable.Range(0, 10), 2) rdd.TakeSample(true, 20, 1).Length 20 rdd.TakeSample(false, 5, 2).Length 5 rdd.TakeSample(false, 15, 3).Length 10</td></tr><tr><td><font color="blue">ComputeFractionForSampleSize</font></td><td>Returns a sampling rate that guarantees a sample of size &gt;= sampleSizeLowerBound 99.99% of the time. How the sampling rate is determined: Let p = num / total, where num is the sample size and total is the total number of data points in the RDD. We're trying to compute q &gt; p such that - when sampling with replacement, we're drawing each data point with prob_i ~ Pois(q), where we want to guarantee Pr[s &lt; num] &lt; 0.0001 for s = sum(prob_i for i from 0 to total), i.e. the failure rate of not having a sufficiently large sample &lt; 0.0001. Setting q = p + 5 * sqrt(p/total) is sufficient to guarantee 0.9999 success rate for num &gt; 12, but we need a slightly larger q (9 empirically determined). - when sampling without replacement, we're drawing each data point with prob_i ~ Binomial(total, fraction) and our choice of q guarantees 1-delta, or 0.9999 success rate, where success rate is defined the same as in sampling with replacement.</td></tr><tr><td><font color="blue">Union</font></td><td>Return the union of this RDD and another one. var rdd = sc.Parallelize(new int[] { 1, 1, 2, 3 }, 1) rdd.union(rdd).collect() [1, 1, 2, 3, 1, 1, 2, 3]</td></tr><tr><td><font color="blue">Intersection</font></td><td>Return the intersection of this RDD and another one. The output will not contain any duplicate elements, even if the input RDDs did. Note that this method performs a shuffle internally. var rdd1 = sc.Parallelize(new int[] { 1, 10, 2, 3, 4, 5 }, 1) var rdd2 = sc.Parallelize(new int[] { 1, 6, 2, 3, 7, 8 }, 1) var rdd1.Intersection(rdd2).Collect() [1, 2, 3]</td></tr><tr><td><font color="blue">Glom</font></td><td>Return an RDD created by coalescing all elements within each partition into a list. var rdd = sc.Parallelize(new int[] { 1, 2, 3, 4 }, 2) rdd.Glom().Collect() [[1, 2], [3, 4]]</td></tr><tr><td><font color="blue">Cartesian``1</font></td><td>Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of elements (a, b) where a is in self and b is in other. rdd = sc.Parallelize(new int[] { 1, 2 }, 1) rdd.Cartesian(rdd).Collect() [(1, 1), (1, 2), (2, 1), (2, 2)]</td></tr><tr><td><font color="blue">GroupBy``1</font></td><td>Return an RDD of grouped items. Each group consists of a key and a sequence of elements mapping to that key. The ordering of elements within each group is not guaranteed, and may even differ each time the resulting RDD is evaluated. Note: This operation may be very expensive. If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]] or [[PairRDDFunctions.reduceByKey]] will provide much better performance. &gt;&gt;&gt; rdd = sc.Parallelize(new int[] { 1, 1, 2, 3, 5, 8 }, 1) &gt;&gt;&gt; result = rdd.GroupBy(lambda x: x % 2).Collect() [(0, [2, 8]), (1, [1, 1, 3, 5])]</td></tr><tr><td><font color="blue">Pipe</font></td><td>Return an RDD created by piping elements to a forked external process. &gt;&gt;&gt; sc.Parallelize(new char[] { '1', '2', '3', '4' }, 1).Pipe("cat").Collect() [u'1', u'2', u'3', u'4']</td></tr><tr><td><font color="blue">Foreach</font></td><td>Applies a function to all elements of this RDD. sc.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).Foreach(x =&gt; Console.Write(x))</td></tr><tr><td><font color="blue">ForeachPartition</font></td><td>Applies a function to each partition of this RDD. sc.parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).ForeachPartition(iter =&gt; { foreach (var x in iter) Console.Write(x + " "); })</td></tr><tr><td><font color="blue">Collect</font></td><td>Return a list that contains all of the elements in this RDD.</td></tr><tr><td><font color="blue">Reduce</font></td><td>Reduces the elements of this RDD using the specified commutative and associative binary operator. sc.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 1).Reduce((x, y) =&gt; x + y) 15</td></tr><tr><td><font color="blue">TreeReduce</font></td><td>Reduces the elements of this RDD in a multi-level tree pattern. &gt;&gt;&gt; add = lambda x, y: x + y &gt;&gt;&gt; rdd = sc.Parallelize(new int[] { -5, -4, -3, -2, -1, 1, 2, 3, 4 }, 10).TreeReduce((x, y) =&gt; x + y)) &gt;&gt;&gt; rdd.TreeReduce(add) -5 &gt;&gt;&gt; rdd.TreeReduce(add, 1) -5 &gt;&gt;&gt; rdd.TreeReduce(add, 2) -5 &gt;&gt;&gt; rdd.TreeReduce(add, 5) -5 &gt;&gt;&gt; rdd.TreeReduce(add, 10) -5</td></tr><tr><td><font color="blue">Fold</font></td><td>Aggregate the elements of each partition, and then the results for all the partitions, using a given associative and commutative function and a neutral "zero value." The function op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object allocation; however, it should not modify t2. This behaves somewhat differently from fold operations implemented for non-distributed collections in functional languages like Scala. This fold operation may be applied to partitions individually, and then fold those results into the final result, rather than apply the fold to each element sequentially in some defined ordering. For functions that are not commutative, the result may differ from that of a fold applied to a non-distributed collection. &gt;&gt;&gt; from operator import add &gt;&gt;&gt; sc.parallelize([1, 2, 3, 4, 5]).fold(0, add) 15</td></tr><tr><td><font color="blue">Aggregate``1</font></td><td>Aggregate the elements of each partition, and then the results for all the partitions, using a given combine functions and a neutral "zero value." The functions op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object allocation; however, it should not modify t2. The first function (seqOp) can return a different result type, U, than the type of this RDD. Thus, we need one operation for merging a T into an U and one operation for merging two U &gt;&gt;&gt; sc.parallelize(new int[] { 1, 2, 3, 4 }, 1).Aggregate(0, (x, y) =&gt; x + y, (x, y) =&gt; x + y)) 10</td></tr><tr><td><font color="blue">TreeAggregate``1</font></td><td>Aggregates the elements of this RDD in a multi-level tree pattern. rdd = sc.Parallelize(new int[] { 1, 2, 3, 4 }, 1).TreeAggregate(0, (x, y) =&gt; x + y, (x, y) =&gt; x + y)) 10</td></tr><tr><td><font color="blue">Count</font></td><td>Return the number of elements in this RDD.</td></tr><tr><td><font color="blue">CountByValue</font></td><td>Return the count of each unique value in this RDD as a dictionary of (value, count) pairs. sc.Parallelize(new int[] { 1, 2, 1, 2, 2 }, 2).CountByValue()) [(1, 2), (2, 3)]</td></tr><tr><td><font color="blue">Take</font></td><td>Take the first num elements of the RDD. It works by first scanning one partition, and use the results from that partition to estimate the number of additional partitions needed to satisfy the limit. Translated from the Scala implementation in RDD#take(). sc.Parallelize(new int[] { 2, 3, 4, 5, 6 }, 2).Cache().Take(2))) [2, 3] sc.Parallelize(new int[] { 2, 3, 4, 5, 6 }, 2).Take(10) [2, 3, 4, 5, 6] sc.Parallelize(Enumerable.Range(0, 100), 100).Filter(x =&gt; x &gt; 90).Take(3) [91, 92, 93]</td></tr><tr><td><font color="blue">First</font></td><td>Return the first element in this RDD. &gt;&gt;&gt; sc.Parallelize(new int[] { 2, 3, 4 }, 2).First() 2</td></tr><tr><td><font color="blue">IsEmpty</font></td><td>Returns true if and only if the RDD contains no elements at all. Note that an RDD may be empty even when it has at least 1 partition. sc.Parallelize(new int[0], 1).isEmpty() true sc.Parallelize(new int[] {1}).isEmpty() false</td></tr><tr><td><font color="blue">Subtract</font></td><td>Return each value in this RDD that is not contained in . var x = sc.Parallelize(new int[] { 1, 2, 3, 4 }, 1) var y = sc.Parallelize(new int[] { 3 }, 1) x.Subtract(y).Collect()) [1, 2, 4]</td></tr><tr><td><font color="blue">KeyBy``1</font></td><td>Creates tuples of the elements in this RDD by applying . sc.Parallelize(new int[] { 1, 2, 3, 4 }, 1).KeyBy(x =&gt; x * x).Collect()) (1, 1), (4, 2), (9, 3), (16, 4)</td></tr><tr><td><font color="blue">Repartition</font></td><td>Return a new RDD that has exactly numPartitions partitions. Can increase or decrease the level of parallelism in this RDD. Internally, this uses a shuffle to redistribute data. If you are decreasing the number of partitions in this RDD, consider using `Coalesce`, which can avoid performing a shuffle. var rdd = sc.Parallelize(new int[] { 1, 2, 3, 4, 5, 6, 7 }, 4) rdd.Glom().Collect().Length 4 rdd.Repartition(2).Glom().Collect().Length 2</td></tr><tr><td><font color="blue">Coalesce</font></td><td>Return a new RDD that is reduced into `numPartitions` partitions. sc.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 3).Glom().Collect().Length 3 &gt;&gt;&gt; sc.Parallelize(new int[] { 1, 2, 3, 4, 5 }, 3).Coalesce(1).Glom().Collect().Length 1</td></tr><tr><td><font color="blue">Zip``1</font></td><td>Zips this RDD with another one, returning key-value pairs with the first element in each RDD second element in each RDD, etc. Assumes that the two RDDs have the same number of partitions and the same number of elements in each partition (e.g. one was made through a map on the other). var x = sc.parallelize(range(0,5)) var y = sc.parallelize(range(1000, 1005)) x.Zip(y).Collect() [(0, 1000), (1, 1001), (2, 1002), (3, 1003), (4, 1004)]</td></tr><tr><td><font color="blue">ZipWithIndex</font></td><td>Zips this RDD with its element indices. The ordering is first based on the partition index and then the ordering of items within each partition. So the first item in the first partition gets index 0, and the last item in the last partition receives the largest index. This method needs to trigger a spark job when this RDD contains more than one partitions. sc.Parallelize(new string[] { "a", "b", "c", "d" }, 3).ZipWithIndex().Collect() [('a', 0), ('b', 1), ('c', 2), ('d', 3)]</td></tr><tr><td><font color="blue">ZipWithUniqueId</font></td><td>Zips this RDD with generated unique Long ids. Items in the kth partition will get ids k, n+k, 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method won't trigger a spark job, which is different from &gt;&gt;&gt; sc.Parallelize(new string[] { "a", "b", "c", "d" }, 1).ZipWithIndex().Collect() [('a', 0), ('b', 1), ('c', 4), ('d', 2), ('e', 5)]</td></tr><tr><td><font color="blue">SetName</font></td><td>Assign a name to this RDD. &gt;&gt;&gt; rdd1 = sc.parallelize([1, 2]) &gt;&gt;&gt; rdd1.setName('RDD1').name() u'RDD1'</td></tr><tr><td><font color="blue">ToDebugString</font></td><td>A description of this RDD and its recursive dependencies for debugging.</td></tr><tr><td><font color="blue">GetStorageLevel</font></td><td>Get the RDD's current storage level. &gt;&gt;&gt; rdd1 = sc.parallelize([1,2]) &gt;&gt;&gt; rdd1.getStorageLevel() StorageLevel(False, False, False, False, 1) &gt;&gt;&gt; print(rdd1.getStorageLevel()) Serialized 1x Replicated</td></tr><tr><td><font color="blue">ToLocalIterator</font></td><td>Return an iterator that contains all of the elements in this RDD. The iterator will consume as much memory as the largest partition in this RDD. sc.Parallelize(Enumerable.Range(0, 10), 1).ToLocalIterator() [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]</td></tr><tr><td><font color="blue">RandomSampleWithRange</font></td><td>Internal method exposed for Random Splits in DataFrames. Samples an RDD given a probability range.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.StringRDDFunctions</font>
####Summary
  
            
            Some useful utility functions for RDD{string}
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">SaveAsTextFile</font></td><td>Save this RDD as a text file, using string representations of elements.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.ComparableRDDFunctions</font>
####Summary
  
            
            Some useful utility functions for RDD's containing IComparable values.
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Max``1</font></td><td>Find the maximum item in this RDD. sc.Parallelize(new double[] { 1.0, 5.0, 43.0, 10.0 }, 2).Max() 43.0</td></tr><tr><td><font color="blue">Min``1</font></td><td>Find the minimum item in this RDD. sc.Parallelize(new double[] { 2.0, 5.0, 43.0, 10.0 }, 2).Min() &gt;&gt;&gt; rdd.min() 2.0</td></tr><tr><td><font color="blue">TakeOrdered``1</font></td><td>Get the N elements from a RDD ordered in ascending order or as specified by the optional key function. sc.Parallelize(new int[] { 10, 1, 2, 9, 3, 4, 5, 6, 7 }, 2).TakeOrdered(6) [1, 2, 3, 4, 5, 6]</td></tr><tr><td><font color="blue">Top``1</font></td><td>Get the top N elements from a RDD. Note: It returns the list sorted in descending order. sc.Parallelize(new int[] { 2, 3, 4, 5, 6 }, 2).Top(3) [6, 5, 4]</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Core.SparkConf</font>
####Summary
  
             
             Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
            
             Note that once a SparkConf object is passed to Spark, it is cloned and can no longer be modified
             by the user. Spark does not support modifying the configuration at runtime.
             
             See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkConf
             
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">SetMaster</font></td><td>The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.</td></tr><tr><td><font color="blue">SetAppName</font></td><td>Set a name for your application. Shown in the Spark web UI.</td></tr><tr><td><font color="blue">SetSparkHome</font></td><td>Set the location where Spark is installed on worker nodes.</td></tr><tr><td><font color="blue">Set</font></td><td>Set the value of a string config</td></tr><tr><td><font color="blue">GetInt</font></td><td>Get a int parameter value, falling back to a default if not set</td></tr><tr><td><font color="blue">Get</font></td><td>Get a string parameter value, falling back to a default if not set</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.DataFrame</font>
####Summary
  
            
             A distributed collection of data organized into named columns.
            
            See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">RegisterTempTable</font></td><td>Registers this DataFrame as a temporary table using the given name. The lifetime of this temporary table is tied to the SqlContext that was used to create this DataFrame.</td></tr><tr><td><font color="blue">Count</font></td><td>Number of rows in the DataFrame</td></tr><tr><td><font color="blue">Show</font></td><td>Displays rows of the DataFrame in tabular form</td></tr><tr><td><font color="blue">ShowSchema</font></td><td>Prints the schema information of the DataFrame</td></tr><tr><td><font color="blue">Collect</font></td><td>Returns all of Rows in this DataFrame</td></tr><tr><td><font color="blue">ToRDD</font></td><td>Converts the DataFrame to RDD of Row</td></tr><tr><td><font color="blue">ToJSON</font></td><td>Returns the content of the DataFrame as RDD of JSON strings</td></tr><tr><td><font color="blue">Explain</font></td><td>Prints the plans (logical and physical) to the console for debugging purposes</td></tr><tr><td><font color="blue">Select</font></td><td>Selects a set of columns specified by column name or Column. df.Select("colA", df["colB"]) df.Select("*", df["colB"] + 10)</td></tr><tr><td><font color="blue">Select</font></td><td>Selects a set of columns. This is a variant of `select` that can only select existing columns using column names (i.e. cannot construct expressions). df.Select("colA", "colB")</td></tr><tr><td><font color="blue">SelectExpr</font></td><td>Selects a set of SQL expressions. This is a variant of `select` that accepts SQL expressions. df.SelectExpr("colA", "colB as newName", "abs(colC)")</td></tr><tr><td><font color="blue">Where</font></td><td>Filters rows using the given condition</td></tr><tr><td><font color="blue">Filter</font></td><td>Filters rows using the given condition</td></tr><tr><td><font color="blue">GroupBy</font></td><td>Groups the DataFrame using the specified columns, so we can run aggregation on them.</td></tr><tr><td><font color="blue">Rollup</font></td><td>Create a multi-dimensional rollup for the current DataFrame using the specified columns, so we can run aggregation on them.</td></tr><tr><td><font color="blue">Cube</font></td><td>Create a multi-dimensional cube for the current DataFrame using the specified columns, so we can run aggregation on them.</td></tr><tr><td><font color="blue">Agg</font></td><td>Aggregates on the DataFrame for the given column-aggregate function mapping</td></tr><tr><td><font color="blue">Join</font></td><td>Join with another DataFrame - Cartesian join</td></tr><tr><td><font color="blue">Join</font></td><td>Join with another DataFrame - Inner equi-join using given column name</td></tr><tr><td><font color="blue">Join</font></td><td>Join with another DataFrame - Inner equi-join using given column name</td></tr><tr><td><font color="blue">Join</font></td><td>Join with another DataFrame, using the specified JoinType</td></tr><tr><td><font color="blue">Intersect</font></td><td>Intersect with another DataFrame. This is equivalent to `INTERSECT` in SQL. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, intersect(self, other)</td></tr><tr><td><font color="blue">UnionAll</font></td><td>Union with another DataFrame WITHOUT removing duplicated rows. This is equivalent to `UNION ALL` in SQL. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, unionAll(self, other)</td></tr><tr><td><font color="blue">Subtract</font></td><td>Returns a new DataFrame containing rows in this frame but not in another frame. This is equivalent to `EXCEPT` in SQL. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, subtract(self, other)</td></tr><tr><td><font color="blue">Drop</font></td><td>Returns a new DataFrame with a column dropped. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, drop(self, col)</td></tr><tr><td><font color="blue">DropNa</font></td><td>Returns a new DataFrame omitting rows with null values. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, dropna(self, how='any', thresh=None, subset=None)</td></tr><tr><td><font color="blue">Na</font></td><td>Returns a DataFrameNaFunctions for working with missing data.</td></tr><tr><td><font color="blue">FillNa</font></td><td>Replace null values, alias for ``na.fill()`</td></tr><tr><td><font color="blue">DropDuplicates</font></td><td>Returns a new DataFrame with duplicate rows removed, considering only the subset of columns. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, dropDuplicates(self, subset=None)</td></tr><tr><td><font color="blue">Replace``1</font></td><td>Returns a new DataFrame replacing a value with another value. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, replace(self, to_replace, value, subset=None)</td></tr><tr><td><font color="blue">ReplaceAll``1</font></td><td>Returns a new DataFrame replacing values with other values. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, replace(self, to_replace, value, subset=None)</td></tr><tr><td><font color="blue">ReplaceAll``1</font></td><td>Returns a new DataFrame replacing values with another value. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, replace(self, to_replace, value, subset=None)</td></tr><tr><td><font color="blue">RandomSplit</font></td><td>Randomly splits this DataFrame with the provided weights. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, randomSplit(self, weights, seed=None)</td></tr><tr><td><font color="blue">Columns</font></td><td>Returns all column names as a list. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, columns(self)</td></tr><tr><td><font color="blue">DTypes</font></td><td>Returns all column names and their data types. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, dtypes(self)</td></tr><tr><td><font color="blue">Sort</font></td><td>Returns a new DataFrame sorted by the specified column(s). Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, sort(self, *cols, **kwargs)</td></tr><tr><td><font color="blue">Sort</font></td><td>Returns a new DataFrame sorted by the specified column(s). Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, sort(self, *cols, **kwargs)</td></tr><tr><td><font color="blue">SortWithinPartitions</font></td><td>Returns a new DataFrame sorted by the specified column(s). Reference to https://github.com/apache/spark/blob/branch-1.6/python/pyspark/sql/dataframe.py, sortWithinPartitions(self, *cols, **kwargs)</td></tr><tr><td><font color="blue">SortWithinPartition</font></td><td>Returns a new DataFrame sorted by the specified column(s). Reference to https://github.com/apache/spark/blob/branch-1.6/python/pyspark/sql/dataframe.py, sortWithinPartitions(self, *cols, **kwargs)</td></tr><tr><td><font color="blue">Alias</font></td><td>Returns a new DataFrame with an alias set. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, alias(self, alias)</td></tr><tr><td><font color="blue">WithColumn</font></td><td>Returns a new DataFrame by adding a column. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, withColumn(self, colName, col)</td></tr><tr><td><font color="blue">WithColumnRenamed</font></td><td>Returns a new DataFrame by renaming an existing column. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, withColumnRenamed(self, existing, new)</td></tr><tr><td><font color="blue">Corr</font></td><td>Calculates the correlation of two columns of a DataFrame as a double value. Currently only supports the Pearson Correlation Coefficient. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, corr(self, col1, col2, method=None)</td></tr><tr><td><font color="blue">Cov</font></td><td>Calculate the sample covariance of two columns as a double value. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, cov(self, col1, col2)</td></tr><tr><td><font color="blue">FreqItems</font></td><td>Finding frequent items for columns, possibly with false positives. Using the frequent element count algorithm described in "http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou". Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, freqItems(self, cols, support=None) Note: This function is meant for exploratory data analysis, as we make no guarantee about the backward compatibility of the schema of the resulting DataFrame.</td></tr><tr><td><font color="blue">Crosstab</font></td><td>Computes a pair-wise frequency table of the given columns. Also known as a contingency table. The number of distinct values for each column should be less than 1e4. At most 1e6 non-zero pair frequencies will be returned. Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, crosstab(self, col1, col2)</td></tr><tr><td><font color="blue">Describe</font></td><td>Computes statistics for numeric columns. This include count, mean, stddev, min, and max. If no columns are given, this function computes statistics for all numerical columns.</td></tr><tr><td><font color="blue">Limit</font></td><td>Returns a new DataFrame by taking the first `n` rows. The difference between this function and `head` is that `head` returns an array while `limit` returns a new DataFrame.</td></tr><tr><td><font color="blue">Head</font></td><td>Returns the first `n` rows.</td></tr><tr><td><font color="blue">First</font></td><td>Returns the first row.</td></tr><tr><td><font color="blue">Take</font></td><td>Returns the first `n` rows in the DataFrame.</td></tr><tr><td><font color="blue">Distinct</font></td><td>Returns a new DataFrame that contains only the unique rows from this DataFrame.</td></tr><tr><td><font color="blue">Coalesce</font></td><td>Returns a new DataFrame that has exactly `numPartitions` partitions. Similar to coalesce defined on an RDD, this operation results in a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of the 100 new partitions will claim 10 of the current partitions.</td></tr><tr><td><font color="blue">Persist</font></td><td>Persist this DataFrame with the default storage level (`MEMORY_AND_DISK`)</td></tr><tr><td><font color="blue">Unpersist</font></td><td>Mark the DataFrame as non-persistent, and remove all blocks for it from memory and disk.</td></tr><tr><td><font color="blue">Cache</font></td><td>Persist this DataFrame with the default storage level (`MEMORY_AND_DISK`)</td></tr><tr><td><font color="blue">Repartition</font></td><td>Returns a new DataFrame that has exactly `numPartitions` partitions.</td></tr><tr><td><font color="blue">Repartition</font></td><td>Returns a new [[DataFrame]] partitioned by the given partitioning columns into . The resulting DataFrame is hash partitioned. optional. If not specified, keep current partitions.</td></tr><tr><td><font color="blue">Repartition</font></td><td>Returns a new [[DataFrame]] partitioned by the given partitioning columns into . The resulting DataFrame is hash partitioned. optional. If not specified, keep current partitions.</td></tr><tr><td><font color="blue">Sample</font></td><td>Returns a new DataFrame by sampling a fraction of rows.</td></tr><tr><td><font color="blue">FlatMap``1</font></td><td>Returns a new RDD by first applying a function to all rows of this DataFrame, and then flattening the results.</td></tr><tr><td><font color="blue">Map``1</font></td><td>Returns a new RDD by applying a function to all rows of this DataFrame.</td></tr><tr><td><font color="blue">MapPartitions``1</font></td><td>Returns a new RDD by applying a function to each partition of this DataFrame.</td></tr><tr><td><font color="blue">ForeachPartition</font></td><td>Applies a function f to each partition of this DataFrame.</td></tr><tr><td><font color="blue">Foreach</font></td><td>Applies a function f to all rows.</td></tr><tr><td><font color="blue">Write</font></td><td>Interface for saving the content of the DataFrame out into external storage.</td></tr><tr><td><font color="blue">SaveAsParquetFile</font></td><td>Saves the contents of this DataFrame as a parquet file, preserving the schema. Files that are written out using this method can be read back in as a DataFrame using the `parquetFile` function in SQLContext.</td></tr><tr><td><font color="blue">InsertInto</font></td><td>Adds the rows from this RDD to the specified table, optionally overwriting the existing data.</td></tr><tr><td><font color="blue">SaveAsTable</font></td><td>Creates a table from the the contents of this DataFrame based on a given data source, SaveMode specified by mode, and a set of options. Note that this currently only works with DataFrames that are created from a HiveContext as there is no notion of a persisted catalog in a standard SQL context. Instead you can write an RDD out to a parquet file, and then register that file as a table. This "table" can then be the target of an `insertInto`. Also note that while this function can persist the table metadata into Hive's metastore, the table will NOT be accessible from Hive, until SPARK-7550 is resolved.</td></tr><tr><td><font color="blue">Save</font></td><td>Saves the contents of this DataFrame based on the given data source, SaveMode specified by mode, and a set of options.</td></tr><tr><td><font color="blue"></font></td><td>Returns a new DataFrame that drops rows containing any null values.</td></tr><tr><td><font color="blue"></font></td><td>Returns a new DataFrame that drops rows containing null values. If `how` is "any", then drop rows containing any null values. If `how` is "all", then drop rows only if every column is null for that row.</td></tr><tr><td><font color="blue"></font></td><td>Returns a new [[DataFrame]] that drops rows containing null values in the specified columns. If `how` is "any", then drop rows containing any null values in the specified columns. If `how` is "all", then drop rows only if every specified column is null for that row.</td></tr><tr><td><font color="blue"></font></td><td>Returns a new DataFrame that drops rows containing any null values in the specified columns.</td></tr><tr><td><font color="blue"></font></td><td>Returns a new DataFrame that drops rows containing less than `minNonNulls` non-null values.</td></tr><tr><td><font color="blue"></font></td><td>Returns a new DataFrame that drops rows containing less than `minNonNulls` non-null values values in the specified columns.</td></tr><tr><td><font color="blue"></font></td><td>Returns a new DataFrame that replaces null values in numeric columns with `value`.</td></tr><tr><td><font color="blue"></font></td><td>Returns a new DataFrame that replaces null values in string columns with `value`.</td></tr><tr><td><font color="blue"></font></td><td>Returns a new DataFrame that replaces null values in specified numeric columns. If a specified column is not a numeric column, it is ignored.</td></tr><tr><td><font color="blue"></font></td><td>Returns a new DataFrame that replaces null values in specified string columns. If a specified column is not a numeric column, it is ignored.</td></tr><tr><td><font color="blue"></font></td><td>Replaces values matching keys in `replacement` map with the corresponding values. Key and value of `replacement` map must have the same type, and can only be doubles or strings. The value must be of the following type: `Integer`, `Long`, `Float`, `Double`, `String`. For example, the following replaces null values in column "A" with string "unknown", and null values in column "B" with numeric value 1.0. import com.google.common.collect.ImmutableMap; df.na.fill(ImmutableMap.of("A", "unknown", "B", 1.0));</td></tr><tr><td><font color="blue"></font></td><td>Replaces values matching keys in `replacement` map with the corresponding values. Key and value of `replacement` map must have the same type, and can only be doubles or strings. If `col` is "*", then the replacement is applied on all string columns or numeric columns. Example: import com.google.common.collect.ImmutableMap; // Replaces all occurrences of 1.0 with 2.0 in column "height". df.replace("height", ImmutableMap.of(1.0, 2.0)); // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name". df.replace("name", ImmutableMap.of("UNKNOWN", "unnamed")); // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns. df.replace("*", ImmutableMap.of("UNKNOWN", "unnamed"));</td></tr><tr><td><font color="blue"></font></td><td>Replaces values matching keys in `replacement` map with the corresponding values. Key and value of `replacement` map must have the same type, and can only be doubles or strings. If `col` is "*", then the replacement is applied on all string columns or numeric columns. Example: import com.google.common.collect.ImmutableMap; // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight". df.replace(new String[] {"height", "weight"}, ImmutableMap.of(1.0, 2.0)); // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname". df.replace(new String[] {"firstname", "lastname"}, ImmutableMap.of("UNKNOWN", "unnamed"));</td></tr><tr><td><font color="blue"></font></td><td>Specifies the input data source format.</td></tr><tr><td><font color="blue"></font></td><td>Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema automatically from data. By specifying the schema here, the underlying data source can skip the schema inference step, and thus speed up data loading.</td></tr><tr><td><font color="blue"></font></td><td>Adds an input option for the underlying data source.</td></tr><tr><td><font color="blue"></font></td><td>Adds input options for the underlying data source.</td></tr><tr><td><font color="blue"></font></td><td>Loads input in as a [[DataFrame]], for data sources that require a path (e.g. data backed by a local or distributed file system).</td></tr><tr><td><font color="blue"></font></td><td>Loads input in as a DataFrame, for data sources that don't require a path (e.g. external key-value stores).</td></tr><tr><td><font color="blue"></font></td><td>Construct a [[DataFrame]] representing the database table accessible via JDBC URL, url named table and connection properties.</td></tr><tr><td><font color="blue"></font></td><td>Construct a DataFrame representing the database table accessible via JDBC URL url named table. Partitions of the table will be retrieved in parallel based on the parameters passed to this function. Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash your external database systems.</td></tr><tr><td><font color="blue"></font></td><td>Construct a DataFrame representing the database table accessible via JDBC URL url named table using connection properties. The `predicates` parameter gives a list expressions suitable for inclusion in WHERE clauses; each one defines one partition of the DataFrame. Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash your external database systems.</td></tr><tr><td><font color="blue"></font></td><td>Loads a JSON file (one object per line) and returns the result as a DataFrame. This function goes through the input once to determine the input schema. If you know the schema in advance, use the version that specifies the schema to avoid the extra scan.</td></tr><tr><td><font color="blue"></font></td><td>Loads a Parquet file, returning the result as a [[DataFrame]]. This function returns an empty DataFrame if no paths are passed in.</td></tr><tr><td><font color="blue"></font></td><td>Specifies the behavior when data or table already exists. Options include: - `SaveMode.Overwrite`: overwrite the existing data. - `SaveMode.Append`: append the data. - `SaveMode.Ignore`: ignore the operation (i.e. no-op). - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.</td></tr><tr><td><font color="blue"></font></td><td>Specifies the behavior when data or table already exists. Options include: - `SaveMode.Overwrite`: overwrite the existing data. - `SaveMode.Append`: append the data. - `SaveMode.Ignore`: ignore the operation (i.e. no-op). - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.</td></tr><tr><td><font color="blue"></font></td><td>Specifies the underlying output data source. Built-in options include "parquet", "json", etc.</td></tr><tr><td><font color="blue"></font></td><td>Adds an output option for the underlying data source.</td></tr><tr><td><font color="blue"></font></td><td>Adds output options for the underlying data source.</td></tr><tr><td><font color="blue"></font></td><td>Partitions the output by the given columns on the file system. If specified, the output is laid out on the file system similar to Hive's partitioning scheme. This is only applicable for Parquet at the moment.</td></tr><tr><td><font color="blue"></font></td><td>Saves the content of the DataFrame at the specified path.</td></tr><tr><td><font color="blue"></font></td><td>Saves the content of the DataFrame as the specified table.</td></tr><tr><td><font color="blue"></font></td><td>Inserts the content of the DataFrame to the specified table. It requires that the schema of the DataFrame is the same as the schema of the table. Because it inserts data to an existing table, format or options will be ignored.</td></tr><tr><td><font color="blue"></font></td><td>Saves the content of the DataFrame as the specified table. In the case the table already exists, behavior of this function depends on the save mode, specified by the `mode` function (default to throwing an exception). When `mode` is `Overwrite`, the schema of the DataFrame does not need to be the same as that of the existing table. When `mode` is `Append`, the schema of the DataFrame need to be the same as that of the existing table, and format or options will be ignored.</td></tr><tr><td><font color="blue"></font></td><td>Saves the content of the DataFrame to a external database table via JDBC. In the case the table already exists in the external database, behavior of this function depends on the save mode, specified by the `mode` function (default to throwing an exception). Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash your external database systems.</td></tr><tr><td><font color="blue"></font></td><td>Saves the content of the DataFrame in JSON format at the specified path. This is equivalent to: Format("json").Save(path)</td></tr><tr><td><font color="blue"></font></td><td>Saves the content of the DataFrame in JSON format at the specified path. This is equivalent to: Format("parquet").Save(path)</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.DataFrameNaFunctions</font>
####Summary
  
            
            Functionality for working with missing data in DataFrames.
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Drop</font></td><td>Returns a new DataFrame that drops rows containing any null values.</td></tr><tr><td><font color="blue">Drop</font></td><td>Returns a new DataFrame that drops rows containing null values. If `how` is "any", then drop rows containing any null values. If `how` is "all", then drop rows only if every column is null for that row.</td></tr><tr><td><font color="blue">Drop</font></td><td>Returns a new [[DataFrame]] that drops rows containing null values in the specified columns. If `how` is "any", then drop rows containing any null values in the specified columns. If `how` is "all", then drop rows only if every specified column is null for that row.</td></tr><tr><td><font color="blue">Drop</font></td><td>Returns a new DataFrame that drops rows containing any null values in the specified columns.</td></tr><tr><td><font color="blue">Drop</font></td><td>Returns a new DataFrame that drops rows containing less than `minNonNulls` non-null values.</td></tr><tr><td><font color="blue">Drop</font></td><td>Returns a new DataFrame that drops rows containing less than `minNonNulls` non-null values values in the specified columns.</td></tr><tr><td><font color="blue">Fill</font></td><td>Returns a new DataFrame that replaces null values in numeric columns with `value`.</td></tr><tr><td><font color="blue">Fill</font></td><td>Returns a new DataFrame that replaces null values in string columns with `value`.</td></tr><tr><td><font color="blue">Fill</font></td><td>Returns a new DataFrame that replaces null values in specified numeric columns. If a specified column is not a numeric column, it is ignored.</td></tr><tr><td><font color="blue">Fill</font></td><td>Returns a new DataFrame that replaces null values in specified string columns. If a specified column is not a numeric column, it is ignored.</td></tr><tr><td><font color="blue">Fill</font></td><td>Replaces values matching keys in `replacement` map with the corresponding values. Key and value of `replacement` map must have the same type, and can only be doubles or strings. The value must be of the following type: `Integer`, `Long`, `Float`, `Double`, `String`. For example, the following replaces null values in column "A" with string "unknown", and null values in column "B" with numeric value 1.0. import com.google.common.collect.ImmutableMap; df.na.fill(ImmutableMap.of("A", "unknown", "B", 1.0));</td></tr><tr><td><font color="blue">Replace``1</font></td><td>Replaces values matching keys in `replacement` map with the corresponding values. Key and value of `replacement` map must have the same type, and can only be doubles or strings. If `col` is "*", then the replacement is applied on all string columns or numeric columns. Example: import com.google.common.collect.ImmutableMap; // Replaces all occurrences of 1.0 with 2.0 in column "height". df.replace("height", ImmutableMap.of(1.0, 2.0)); // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name". df.replace("name", ImmutableMap.of("UNKNOWN", "unnamed")); // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns. df.replace("*", ImmutableMap.of("UNKNOWN", "unnamed"));</td></tr><tr><td><font color="blue">Replace``1</font></td><td>Replaces values matching keys in `replacement` map with the corresponding values. Key and value of `replacement` map must have the same type, and can only be doubles or strings. If `col` is "*", then the replacement is applied on all string columns or numeric columns. Example: import com.google.common.collect.ImmutableMap; // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight". df.replace(new String[] {"height", "weight"}, ImmutableMap.of(1.0, 2.0)); // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname". df.replace(new String[] {"firstname", "lastname"}, ImmutableMap.of("UNKNOWN", "unnamed"));</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.DataFrameReader</font>
####Summary
  
            
            Interface used to load a DataFrame from external storage systems (e.g. file systems,
            key-value stores, etc). Use SQLContext.read() to access this.
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Format</font></td><td>Specifies the input data source format.</td></tr><tr><td><font color="blue">Schema</font></td><td>Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema automatically from data. By specifying the schema here, the underlying data source can skip the schema inference step, and thus speed up data loading.</td></tr><tr><td><font color="blue">Option</font></td><td>Adds an input option for the underlying data source.</td></tr><tr><td><font color="blue">Options</font></td><td>Adds input options for the underlying data source.</td></tr><tr><td><font color="blue">Load</font></td><td>Loads input in as a [[DataFrame]], for data sources that require a path (e.g. data backed by a local or distributed file system).</td></tr><tr><td><font color="blue">Load</font></td><td>Loads input in as a DataFrame, for data sources that don't require a path (e.g. external key-value stores).</td></tr><tr><td><font color="blue">Jdbc</font></td><td>Construct a [[DataFrame]] representing the database table accessible via JDBC URL, url named table and connection properties.</td></tr><tr><td><font color="blue">Jdbc</font></td><td>Construct a DataFrame representing the database table accessible via JDBC URL url named table. Partitions of the table will be retrieved in parallel based on the parameters passed to this function. Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash your external database systems.</td></tr><tr><td><font color="blue">Jdbc</font></td><td>Construct a DataFrame representing the database table accessible via JDBC URL url named table using connection properties. The `predicates` parameter gives a list expressions suitable for inclusion in WHERE clauses; each one defines one partition of the DataFrame. Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash your external database systems.</td></tr><tr><td><font color="blue">Json</font></td><td>Loads a JSON file (one object per line) and returns the result as a DataFrame. This function goes through the input once to determine the input schema. If you know the schema in advance, use the version that specifies the schema to avoid the extra scan.</td></tr><tr><td><font color="blue">Parquet</font></td><td>Loads a Parquet file, returning the result as a [[DataFrame]]. This function returns an empty DataFrame if no paths are passed in.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.DataFrameWriter</font>
####Summary
  
            
            Interface used to write a DataFrame to external storage systems (e.g. file systems,
            key-value stores, etc). Use DataFrame.Write to access this.
            
            See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Mode</font></td><td>Specifies the behavior when data or table already exists. Options include: - `SaveMode.Overwrite`: overwrite the existing data. - `SaveMode.Append`: append the data. - `SaveMode.Ignore`: ignore the operation (i.e. no-op). - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.</td></tr><tr><td><font color="blue">Mode</font></td><td>Specifies the behavior when data or table already exists. Options include: - `SaveMode.Overwrite`: overwrite the existing data. - `SaveMode.Append`: append the data. - `SaveMode.Ignore`: ignore the operation (i.e. no-op). - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.</td></tr><tr><td><font color="blue">Format</font></td><td>Specifies the underlying output data source. Built-in options include "parquet", "json", etc.</td></tr><tr><td><font color="blue">Option</font></td><td>Adds an output option for the underlying data source.</td></tr><tr><td><font color="blue">Options</font></td><td>Adds output options for the underlying data source.</td></tr><tr><td><font color="blue">PartitionBy</font></td><td>Partitions the output by the given columns on the file system. If specified, the output is laid out on the file system similar to Hive's partitioning scheme. This is only applicable for Parquet at the moment.</td></tr><tr><td><font color="blue">Save</font></td><td>Saves the content of the DataFrame at the specified path.</td></tr><tr><td><font color="blue">Save</font></td><td>Saves the content of the DataFrame as the specified table.</td></tr><tr><td><font color="blue">InsertInto</font></td><td>Inserts the content of the DataFrame to the specified table. It requires that the schema of the DataFrame is the same as the schema of the table. Because it inserts data to an existing table, format or options will be ignored.</td></tr><tr><td><font color="blue">SaveAsTable</font></td><td>Saves the content of the DataFrame as the specified table. In the case the table already exists, behavior of this function depends on the save mode, specified by the `mode` function (default to throwing an exception). When `mode` is `Overwrite`, the schema of the DataFrame does not need to be the same as that of the existing table. When `mode` is `Append`, the schema of the DataFrame need to be the same as that of the existing table, and format or options will be ignored.</td></tr><tr><td><font color="blue">Jdbc</font></td><td>Saves the content of the DataFrame to a external database table via JDBC. In the case the table already exists in the external database, behavior of this function depends on the save mode, specified by the `mode` function (default to throwing an exception). Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash your external database systems.</td></tr><tr><td><font color="blue">Json</font></td><td>Saves the content of the DataFrame in JSON format at the specified path. This is equivalent to: Format("json").Save(path)</td></tr><tr><td><font color="blue">Parquet</font></td><td>Saves the content of the DataFrame in JSON format at the specified path. This is equivalent to: Format("parquet").Save(path)</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.PythonSerDe</font>
####Summary
  
            
            Used for SerDe of Python objects
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">GetUnpickledObjects</font></td><td>Unpickles objects from byte[]</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.RowConstructor</font>
####Summary
  
            
            Used by Unpickler to unpickle pickled objects. It is also used to construct a Row (C# representation of pickled objects).
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">construct</font></td><td>Used by Unpickler - do not use to construct Row. Use GetRow() method</td></tr><tr><td><font color="blue">GetRow</font></td><td>Used to construct a Row</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.Row</font>
####Summary
  
            
             Represents one row of output from a relational operator.
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue"></font></td><td>Used by Unpickler - do not use to construct Row. Use GetRow() method</td></tr><tr><td><font color="blue"></font></td><td>Used to construct a Row</td></tr><tr><td><font color="blue">Size</font></td><td>Number of elements in the Row.</td></tr><tr><td><font color="blue">GetSchema</font></td><td>Schema for the row.</td></tr><tr><td><font color="blue">Get</font></td><td>Returns the value at position i.</td></tr><tr><td><font color="blue">Get</font></td><td>Returns the value of a given columnName.</td></tr><tr><td><font color="blue">GetAs``1</font></td><td>Returns the value at position i, the return value will be cast to type T.</td></tr><tr><td><font color="blue">GetAs``1</font></td><td>Returns the value of a given columnName, the return value will be cast to type T.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.Functions</font>
####Summary
  
            
            DataFrame Built-in functions
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Exp</font></td><td>Computes the exponential of the given value.</td></tr><tr><td><font color="blue">Expm1</font></td><td>Computes the exponential of the given value minus one.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.SaveMode</font>
####Summary
  
            
            SaveMode is used to specify the expected behavior of saving a DataFrame to a data source.
            
        
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.SaveModeExtensions</font>
####Summary
  
            
            For SaveMode.ErrorIfExists, the corresponding literal string in spark is "error" or "default".
            
        
###<font color="#68228B">Microsoft.Spark.CSharp.Sql.SqlContext</font>
####Summary
  
            
            The entry point for working with structured data (rows and columns) in Spark.  
            Allows the creation of [[DataFrame]] objects as well as the execution of SQL queries.
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">GetOrCreate</font></td><td>Get the existing SQLContext or create a new one with given SparkContext.</td></tr><tr><td><font color="blue">NewSession</font></td><td>Returns a new SQLContext as new session, that has separate SQLConf, registered temporary tables and UDFs, but shared SparkContext and table cache.</td></tr><tr><td><font color="blue">GetConf</font></td><td>Returns the value of Spark SQL configuration property for the given key. If the key is not set, returns defaultValue.</td></tr><tr><td><font color="blue">SetConf</font></td><td>Sets the given Spark SQL configuration property.</td></tr><tr><td><font color="blue">Read</font></td><td>Returns a DataFrameReader that can be used to read data in as a DataFrame.</td></tr><tr><td><font color="blue">ReadDataFrame</font></td><td>Loads a dataframe the source path using the given schema and options</td></tr><tr><td><font color="blue">CreateDataFrame</font></td><td>Creates a from a RDD containing array of object using the given schema.</td></tr><tr><td><font color="blue">RegisterDataFrameAsTable</font></td><td>Registers the given as a temporary table in the catalog. Temporary tables exist only during the lifetime of this instance of SqlContext.</td></tr><tr><td><font color="blue">DropTempTable</font></td><td>Remove the temp table from catalog.</td></tr><tr><td><font color="blue">Table</font></td><td>Returns the specified table as a</td></tr><tr><td><font color="blue">Tables</font></td><td>Returns a containing names of tables in the given database. If is not specified, the current database will be used. The returned DataFrame has two columns: 'tableName' and 'isTemporary' (a column with bool type indicating if a table is a temporary one or not).</td></tr><tr><td><font color="blue">TableNames</font></td><td>Returns a list of names of tables in the database</td></tr><tr><td><font color="blue">CacheTable</font></td><td>Caches the specified table in-memory.</td></tr><tr><td><font color="blue">UncacheTable</font></td><td>Removes the specified table from the in-memory cache.</td></tr><tr><td><font color="blue">ClearCache</font></td><td>Removes all cached tables from the in-memory cache.</td></tr><tr><td><font color="blue">IsCached</font></td><td>Returns true if the table is currently cached in-memory.</td></tr><tr><td><font color="blue">Sql</font></td><td>Executes a SQL query using Spark, returning the result as a DataFrame. The dialect that is used for SQL parsing can be configured with 'spark.sql.dialect'</td></tr><tr><td><font color="blue">JsonFile</font></td><td>Loads a JSON file (one object per line), returning the result as a DataFrame It goes through the entire dataset once to determine the schema.</td></tr><tr><td><font color="blue">JsonFile</font></td><td>Loads a JSON file (one object per line) and applies the given schema</td></tr><tr><td><font color="blue">TextFile</font></td><td>Loads text file with the specific column delimited using the given schema</td></tr><tr><td><font color="blue">TextFile</font></td><td>Loads a text file (one object per line), returning the result as a DataFrame</td></tr><tr><td><font color="blue">RegisterFunction``1</font></td><td>Register UDF with no input argument, e.g: ("MyFilter", () =&gt; true); sqlContext.Sql("SELECT * FROM MyTable where MyFilter()");</td></tr><tr><td><font color="blue">RegisterFunction``2</font></td><td>Register UDF with 1 input argument, e.g: ("MyFilter", (arg1) =&gt; arg1 != null); sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1)");</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Streaming.DStream`1</font>
####Summary
  
            
            A Discretized Stream (DStream), the basic abstraction in Spark Streaming,
            is a continuous sequence of RDDs (of the same type) representing a
            continuous stream of data (see ) in the Spark core documentation
            for more details on RDDs).
            
            DStreams can either be created from live data (such as, data from TCP
            sockets, Kafka, Flume, etc.) using a  or it can be
            generated by transforming existing DStreams using operations such as
            `Map`, `Window` and `ReduceByKeyAndWindow`. While a Spark Streaming
            program is running, each DStream periodically generates a RDD, either
            from live data or by transforming the RDD generated by a parent DStream.
            
            DStreams internally is characterized by a few basic properties:
             - A list of other DStreams that the DStream depends on
             - A time interval at which the DStream generates an RDD
             - A function that is used to generate an RDD after each time interval
            
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">Count</font></td><td>Return a new DStream in which each RDD has a single element generated by counting each RDD of this DStream.</td></tr><tr><td><font color="blue">Filter</font></td><td>Return a new DStream containing only the elements that satisfy predicate.</td></tr><tr><td><font color="blue">FlatMap``1</font></td><td>Return a new DStream by applying a function to all elements of this DStream, and then flattening the results</td></tr><tr><td><font color="blue">Map``1</font></td><td>Return a new DStream by applying a function to each element of DStream.</td></tr><tr><td><font color="blue">MapPartitions``1</font></td><td>Return a new DStream in which each RDD is generated by applying mapPartitions() to each RDDs of this DStream.</td></tr><tr><td><font color="blue">MapPartitionsWithIndex``1</font></td><td>Return a new DStream in which each RDD is generated by applying mapPartitionsWithIndex() to each RDDs of this DStream.</td></tr><tr><td><font color="blue">Reduce</font></td><td>Return a new DStream in which each RDD has a single element generated by reducing each RDD of this DStream.</td></tr><tr><td><font color="blue">ForeachRDD</font></td><td>Apply a function to each RDD in this DStream.</td></tr><tr><td><font color="blue">ForeachRDD</font></td><td>Apply a function to each RDD in this DStream.</td></tr><tr><td><font color="blue">Print</font></td><td>Print the first num elements of each RDD generated in this DStream. @param num: the number of elements from the first will be printed.</td></tr><tr><td><font color="blue">Glom</font></td><td>Return a new DStream in which RDD is generated by applying glom() to RDD of this DStream.</td></tr><tr><td><font color="blue">Cache</font></td><td>Persist the RDDs of this DStream with the default storage level .</td></tr><tr><td><font color="blue">Persist</font></td><td>Persist the RDDs of this DStream with the given storage level</td></tr><tr><td><font color="blue">Checkpoint</font></td><td>Enable periodic checkpointing of RDDs of this DStream</td></tr><tr><td><font color="blue">CountByValue</font></td><td>Return a new DStream in which each RDD contains the counts of each distinct value in each RDD of this DStream.</td></tr><tr><td><font color="blue">SaveAsTextFiles</font></td><td>Save each RDD in this DStream as text file, using string representation of elements.</td></tr><tr><td><font color="blue">Transform``1</font></td><td>Return a new DStream in which each RDD is generated by applying a function on each RDD of this DStream. `func` can have one argument of `rdd`, or have two arguments of (`time`, `rdd`)</td></tr><tr><td><font color="blue">Transform``1</font></td><td>Return a new DStream in which each RDD is generated by applying a function on each RDD of this DStream. `func` can have one argument of `rdd`, or have two arguments of (`time`, `rdd`)</td></tr><tr><td><font color="blue">TransformWith``2</font></td><td>Return a new DStream in which each RDD is generated by applying a function on each RDD of this DStream and 'other' DStream. `func` can have two arguments of (`rdd_a`, `rdd_b`) or have three arguments of (`time`, `rdd_a`, `rdd_b`)</td></tr><tr><td><font color="blue">TransformWith``2</font></td><td>Return a new DStream in which each RDD is generated by applying a function on each RDD of this DStream and 'other' DStream. `func` can have two arguments of (`rdd_a`, `rdd_b`) or have three arguments of (`time`, `rdd_a`, `rdd_b`)</td></tr><tr><td><font color="blue">Repartition</font></td><td>Return a new DStream with an increased or decreased level of parallelism.</td></tr><tr><td><font color="blue">Union</font></td><td>Return a new DStream by unifying data of another DStream with this DStream. @param other: Another DStream having the same interval (i.e., slideDuration) as this DStream.</td></tr><tr><td><font color="blue">Slice</font></td><td>Return all the RDDs between 'fromTime' to 'toTime' (both included)</td></tr><tr><td><font color="blue">Window</font></td><td>Return a new DStream in which each RDD contains all the elements in seen in a sliding window of time over this DStream. @param windowDuration: width of the window; must be a multiple of this DStream's batching interval @param slideDuration: sliding interval of the window (i.e., the interval after which the new DStream will generate RDDs); must be a multiple of this DStream's batching interval</td></tr><tr><td><font color="blue">ReduceByWindow</font></td><td>Return a new DStream in which each RDD has a single element generated by reducing all elements in a sliding window over this DStream. if `invReduceFunc` is not None, the reduction is done incrementally using the old window's reduced value : 1. reduce the new values that entered the window (e.g., adding new counts) 2. "inverse reduce" the old values that left the window (e.g., subtracting old counts) This is more efficient than `invReduceFunc` is None.</td></tr><tr><td><font color="blue">CountByWindow</font></td><td>Return a new DStream in which each RDD has a single element generated by counting the number of elements in a window over this DStream. windowDuration and slideDuration are as defined in the window() operation. This is equivalent to window(windowDuration, slideDuration).count(), but will be more efficient if window is large.</td></tr><tr><td><font color="blue">CountByValueAndWindow</font></td><td>Return a new DStream in which each RDD contains the count of distinct elements in RDDs in a sliding window over this DStream.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Streaming.EventHubsUtils</font>
####Summary
  
            
            Utility for creating streams from 
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">CreateUnionStream</font></td><td>Create a unioned EventHubs stream that receives data from Microsoft Azure Eventhubs The unioned stream will receive message from all partitions of the EventHubs</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Streaming.MapWithStateDStream`4</font>
####Summary
  
            
            DStream representing the stream of data generated by `mapWithState` operation on a pair DStream.
            Additionally, it also gives access to the stream of state snapshots, that is, the state data of all keys after a batch has updated them.
            
            Type of the key
            Type of the value
            Type of the state data
            Type of the mapped data
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">StateSnapshots</font></td><td>Return a pair DStream where each RDD is the snapshot of the state of all the keys.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Streaming.KeyedState`1</font>
####Summary
  
            
            Class to hold a state instance and the timestamp when the state is updated or created.
            No need to explicitly make this class clonable, since the serialization and deserialization in Worker is already a kind of clone mechanism. 
            
            Type of the state data
        
###<font color="#68228B">Microsoft.Spark.CSharp.Streaming.MapWithStateRDDRecord`3</font>
####Summary
  
            
            Record storing the keyed-state MapWithStateRDD. 
            Each record contains a stateMap and a sequence of records returned by the mapping function of MapWithState.
            Note: don't need to explicitly make this class clonable, since the serialization and deserialization in Worker is already a kind of clone. 
            
            Type of the key
            Type of the state data
            Type of the mapped data
        
###<font color="#68228B">Microsoft.Spark.CSharp.Streaming.StateSpec`4</font>
####Summary
  
            
            Representing all the specifications of the DStream transformation `mapWithState` operation.
            
            Type of the key
            Type of the value
            Type of the state data
            Type of the mapped data
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">NumPartitions</font></td><td>Set the number of partitions by which the state RDDs generated by `mapWithState` will be partitioned. Hash partitioning will be used.</td></tr><tr><td><font color="blue">Timeout</font></td><td>Set the duration after which the state of an idle key will be removed. A key and its state is considered idle if it has not received any data for at least the given duration. The mapping function will be called one final time on the idle states that are going to be removed; [[org.apache.spark.streaming.State State.isTimingOut()]] set to `true` in that call.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Streaming.State`1</font>
####Summary
  
            
            class for getting and updating the state in mapping function used in the `mapWithState` operation
            
            Type of the state
        
###<font color="#68228B">Microsoft.Spark.CSharp.Streaming.PairDStreamFunctions</font>
####Summary
  
            
            operations only available to KeyValuePair RDD
            
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">ReduceByKey``2</font></td><td>Return a new DStream by applying ReduceByKey to each RDD.</td></tr><tr><td><font color="blue">CombineByKey``3</font></td><td>Return a new DStream by applying combineByKey to each RDD.</td></tr><tr><td><font color="blue">PartitionBy``2</font></td><td>Return a new DStream in which each RDD are partitioned by numPartitions.</td></tr><tr><td><font color="blue">MapValues``3</font></td><td>Return a new DStream by applying a map function to the value of each key-value pairs in this DStream without changing the key.</td></tr><tr><td><font color="blue">FlatMapValues``3</font></td><td>Return a new DStream by applying a flatmap function to the value of each key-value pairs in this DStream without changing the key.</td></tr><tr><td><font color="blue">GroupByKey``2</font></td><td>Return a new DStream by applying groupByKey on each RDD.</td></tr><tr><td><font color="blue">GroupWith``3</font></td><td>Return a new DStream by applying 'cogroup' between RDDs of this DStream and `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions` partitions.</td></tr><tr><td><font color="blue">Join``3</font></td><td>Return a new DStream by applying 'join' between RDDs of this DStream and `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions` partitions.</td></tr><tr><td><font color="blue">LeftOuterJoin``3</font></td><td>Return a new DStream by applying 'left outer join' between RDDs of this DStream and `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions` partitions.</td></tr><tr><td><font color="blue">RightOuterJoin``3</font></td><td>Return a new DStream by applying 'right outer join' between RDDs of this DStream and `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions` partitions.</td></tr><tr><td><font color="blue">FullOuterJoin``3</font></td><td>Return a new DStream by applying 'full outer join' between RDDs of this DStream and `other` DStream. Hash partitioning is used to generate the RDDs with `numPartitions` partitions.</td></tr><tr><td><font color="blue">GroupByKeyAndWindow``2</font></td><td>Return a new DStream by applying `GroupByKey` over a sliding window. Similar to `DStream.GroupByKey()`, but applies it over a sliding window.</td></tr><tr><td><font color="blue">ReduceByKeyAndWindow``2</font></td><td>Return a new DStream by applying incremental `reduceByKey` over a sliding window. The reduced value of over a new window is calculated using the old window's reduce value : 1. reduce the new values that entered the window (e.g., adding new counts) 2. "inverse reduce" the old values that left the window (e.g., subtracting old counts) `invFunc` can be None, then it will reduce all the RDDs in window, could be slower than having `invFunc`.</td></tr><tr><td><font color="blue">UpdateStateByKey``3</font></td><td>Return a new "state" DStream where the state for each key is updated by applying the given function on the previous state of the key and the new values of the key.</td></tr><tr><td><font color="blue">UpdateStateByKey``3</font></td><td>Return a new "state" DStream where the state for each key is updated by applying the given function on the previous state of the key and the new values of the key.</td></tr><tr><td><font color="blue">UpdateStateByKey``3</font></td><td>Return a new "state" DStream where the state for each key is updated by applying the given function on the previous state of the key and the new values of the key.</td></tr><tr><td><font color="blue">MapWithState``4</font></td><td>Return a new "state" DStream where the state for each key is updated by applying the given function on the previous state of the key and the new values of the key.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Streaming.StreamingContext</font>
####Summary
  
            Main entry point for Spark Streaming functionality. It provides methods used to create
            [[org.apache.spark.streaming.dstream.DStream]]s from various input sources. It can be either
            created by providing a Spark master URL and an appName, or from a org.apache.spark.SparkConf
            configuration (see core Spark documentation), or from an existing org.apache.spark.SparkContext.
            The associated SparkContext can be accessed using `context.sparkContext`. After
            creating and transforming DStreams, the streaming computation can be started and stopped
            using `context.start()` and `context.stop()`, respectively.
            `context.awaitTermination()` allows the current thread to wait for the termination
            of the context by `stop()` or by an exception.
        
####Methods

<table><tr><th>Name</th><th>Description</th></tr><tr><td><font color="blue">GetOrCreate</font></td><td>Either recreate a StreamingContext from checkpoint data or create a new StreamingContext. If checkpoint data exists in the provided `checkpointPath`, then StreamingContext will be recreated from the checkpoint data. If the data does not exist, then the provided setupFunc will be used to create a JavaStreamingContext.</td></tr><tr><td><font color="blue">Remember</font></td><td>Set each DStreams in this context to remember RDDs it generated in the last given duration. DStreams remember RDDs only for a limited duration of time and releases them for garbage collection. This method allows the developer to specify how long to remember the RDDs ( if the developer wishes to query old data outside the DStream computation).</td></tr><tr><td><font color="blue">Checkpoint</font></td><td>Set the context to periodically checkpoint the DStream operations for driver fault-tolerance.</td></tr><tr><td><font color="blue">SocketTextStream</font></td><td>Create an input from TCP source hostname:port. Data is received using a TCP socket and receive byte is interpreted as UTF8 encoded ``\\n`` delimited lines.</td></tr><tr><td><font color="blue">TextFileStream</font></td><td>Create an input stream that monitors a Hadoop-compatible file system for new files and reads them as text files. Files must be wrriten to the monitored directory by "moving" them from another location within the same file system. File names starting with . are ignored.</td></tr><tr><td><font color="blue">AwaitTermination</font></td><td>Wait for the execution to stop.</td></tr><tr><td><font color="blue">AwaitTerminationOrTimeout</font></td><td>Wait for the execution to stop.</td></tr><tr><td><font color="blue">Transform``1</font></td><td>Create a new DStream in which each RDD is generated by applying a function on RDDs of the DStreams. The order of the JavaRDDs in the transform function parameter will be the same as the order of corresponding DStreams in the list.</td></tr><tr><td><font color="blue">Union``1</font></td><td>Create a unified DStream from multiple DStreams of the same type and same slide duration.</td></tr></table>

---
  
  
###<font color="#68228B">Microsoft.Spark.CSharp.Streaming.TransformedDStream`1</font>
####Summary
  
            
            TransformedDStream is an DStream generated by an C# function
            transforming each RDD of an DStream to another RDDs.
            
            Multiple continuous transformations of DStream can be combined into
            one transformation.
            
            
        