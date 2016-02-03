// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Core
{
    public class SparkContext
    {
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkContext));
        internal ISparkContextProxy SparkContextProxy { get; private set; }
        internal SparkConf SparkConf { get; private set; }

        private AccumulatorServer accumulatorServer;
        private int nextAccumulatorId;

        /// <summary>
        /// The version of Spark on which this application is running.
        /// </summary>
        public string Version
        {
            get { return SparkContextProxy.Version; }
        }

        /// <summary>
        /// Return the epoch time when the Spark Context was started.
        /// </summary>
        public long StartTime
        {
            get { return SparkContextProxy.StartTime; }
        }

        /// <summary>
        /// Default level of parallelism to use when not given by user (e.g. for reduce tasks)
        /// </summary>
        public int DefaultParallelism
        {
            get { return SparkContextProxy.DefaultParallelism; }
        }

        /// <summary>
        /// Default min number of partitions for Hadoop RDDs when not given by user
        /// </summary>
        public int DefaultMinPartitions
        {
            get { return SparkContextProxy.DefaultMinPartitions; }
        }

        /// <summary>
        /// Get SPARK_USER for user who is running SparkContext.
        /// </summary>
        public string SparkUser { get { return SparkContextProxy.SparkUser; } }

        /// <summary>
        /// Return :class:`StatusTracker` object
        /// </summary>
        public StatusTracker StatusTracker { get { return new StatusTracker(SparkContextProxy.StatusTracker); } }

        public SparkContext(string master, string appName, string sparkHome)
            : this(master, appName, sparkHome, null)
        {}

        public SparkContext(string master, string appName)
            : this(master, appName, null, null)
        {}

        public SparkContext(SparkConf conf)
            : this(null, null, null, conf)
        {}

        /// <summary>
        /// when created from checkpoint
        /// </summary>
        /// <param name="sparkContextProxy"></param>
        /// <param name="conf"></param>
        internal SparkContext(ISparkContextProxy sparkContextProxy, SparkConf conf)
        {
            SparkContextProxy = sparkContextProxy;
            SparkConf = conf;
        }

        private SparkContext(string master, string appName, string sparkHome, SparkConf conf)
        {
            SparkConf = conf ?? new SparkConf();
            if (master != null)
                SparkConf.SetMaster(master);
            if (appName != null)
                SparkConf.SetAppName(appName);
            if (sparkHome != null)
                SparkConf.SetSparkHome(sparkHome);

            SparkContextProxy = SparkCLREnvironment.SparkCLRProxy.CreateSparkContext(SparkConf.SparkConfProxy);
        }

        internal void StartAccumulatorServer()
        {
            if (accumulatorServer == null)
            {
                accumulatorServer = new AccumulatorServer();
                int port = accumulatorServer.StartUpdateServer();
                SparkContextProxy.Accumulator(port);
            }
        }

        public RDD<string> TextFile(string filePath, int minPartitions = 0)
        {
            logger.LogInfo("Reading text file {0} as RDD<string> with {1} partitions", filePath, minPartitions);
            return new RDD<string>(SparkContextProxy.TextFile(filePath, minPartitions), this, SerializedMode.String);
        }

        /// <summary>
        /// Distribute a local collection to form an RDD.
        ///
        /// sc.Parallelize(new int[] {0, 2, 3, 4, 6}, 5).Glom().Collect()
        /// [[0], [2], [3], [4], [6]]
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="serializableObjects"></param>
        /// <param name="numSlices"></param>
        /// <returns></returns>
        public RDD<T> Parallelize<T>(IEnumerable<T> serializableObjects, int numSlices = 1)
        {
            List<byte[]> collectionOfByteRepresentationOfObjects = new List<byte[]>();
            foreach (T obj in serializableObjects)
            {
                var memoryStream = new MemoryStream();
                var formatter = new BinaryFormatter();
                formatter.Serialize(memoryStream, obj);
                collectionOfByteRepresentationOfObjects.Add(memoryStream.ToArray());
            }

            if (numSlices < 1)
                numSlices = 1;

            logger.LogInfo("Parallelizing {0} items to form RDD in the cluster with {1} partitions", collectionOfByteRepresentationOfObjects.Count, numSlices);
            return new RDD<T>(SparkContextProxy.Parallelize(collectionOfByteRepresentationOfObjects, numSlices), this);
        }

        /// <summary>
        /// Create an RDD that has no partitions or elements.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public RDD<T> EmptyRDD<T>()
        {
            return new RDD<T>(SparkContextProxy.EmptyRDD(), this, SerializedMode.None);
        }

        /// <summary>
        /// Read a directory of text files from HDFS, a local file system (available on all nodes), or any
        /// Hadoop-supported file system URI. Each file is read as a single record and returned in a
        /// key-value pair, where the key is the path of each file, the value is the content of each file.
        ///
        /// For example, if you have the following files:
        /// {{{
        ///   hdfs://a-hdfs-path/part-00000
        ///   hdfs://a-hdfs-path/part-00001
        ///   ...
        ///   hdfs://a-hdfs-path/part-nnnnn
        /// }}}
        ///
        /// Do
        /// {{{
        ///   <see cref="RDD{KeyValuePair{string, string}}"/> rdd = sparkContext.WholeTextFiles("hdfs://a-hdfs-path")
        /// }}}
        ///
        /// then `rdd` contains
        /// {{{
        ///   (a-hdfs-path/part-00000, its content)
        ///   (a-hdfs-path/part-00001, its content)
        ///   ...
        ///   (a-hdfs-path/part-nnnnn, its content)
        /// }}}
        ///
        /// Small files are preferred, large file is also allowable, but may cause bad performance.
        ///
        /// minPartitions A suggestion value of the minimal splitting number for input data.
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="minPartitions"></param>
        /// <returns></returns>
        public RDD<KeyValuePair<byte[], byte[]>> WholeTextFiles(string filePath, int? minPartitions = null)
        {
            return new RDD<KeyValuePair<byte[], byte[]>>(SparkContextProxy.WholeTextFiles(filePath, minPartitions ?? DefaultMinPartitions), this, SerializedMode.Pair);
        }

        /// <summary>
        /// Read a directory of binary files from HDFS, a local file system (available on all nodes),
        /// or any Hadoop-supported file system URI as a byte array. Each file is read as a single
        /// record and returned in a key-value pair, where the key is the path of each file,
        /// the value is the content of each file.
        ///
        /// For example, if you have the following files:
        /// {{{
        ///   hdfs://a-hdfs-path/part-00000
        ///   hdfs://a-hdfs-path/part-00001
        ///   ...
        ///   hdfs://a-hdfs-path/part-nnnnn
        /// }}}
        ///
        /// Do
        /// <see cref="RDD{KeyValuePair{string, byte[]}}"/> rdd = sparkContext.dataStreamFiles("hdfs://a-hdfs-path")`,
        ///
        /// then `rdd` contains
        /// {{{
        ///   (a-hdfs-path/part-00000, its content)
        ///   (a-hdfs-path/part-00001, its content)
        ///   ...
        ///   (a-hdfs-path/part-nnnnn, its content)
        /// }}}
        ///
        /// @note Small files are preferred; very large files but may cause bad performance.
        ///
        /// @param minPartitions A suggestion value of the minimal splitting number for input data.
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="minPartitions"></param>
        /// <returns></returns>
        public RDD<KeyValuePair<byte[], byte[]>> BinaryFiles(string filePath, int? minPartitions)
        {
            return new RDD<KeyValuePair<byte[], byte[]>>(SparkContextProxy.BinaryFiles(filePath, minPartitions ?? DefaultMinPartitions), this, SerializedMode.Pair);
        }

        /// <summary>
        /// Read a Hadoop SequenceFile with arbitrary key and value Writable class from HDFS,
        /// a local file system (available on all nodes), or any Hadoop-supported file system URI.
        /// The mechanism is as follows:
        /// 
        ///     1. A Java RDD is created from the SequenceFile or other InputFormat, and the key
        ///         and value Writable classes
        ///     2. Serialization is attempted via Pyrolite pickling
        ///     3. If this fails, the fallback is to call 'toString' on each key and value
        ///     4. PickleSerializer is used to deserialize pickled objects on the Python side
        ///     
        /// </summary>
        /// <param name="filePath">path to sequncefile</param>
        /// <param name="keyClass">fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.Text")</param>
        /// <param name="valueClass">fully qualified classname of value Writable class (e.g. "org.apache.hadoop.io.LongWritable")</param>
        /// <param name="keyConverterClass"></param>
        /// <param name="valueConverterClass"></param>
        /// <param name="minSplits">minimum splits in dataset (default min(2, sc.defaultParallelism))</param>
        /// <returns></returns>
        public RDD<byte[]> SequenceFile(string filePath, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, int? minSplits)
        {
            return new RDD<byte[]>(SparkContextProxy.SequenceFile(filePath, keyClass, valueClass, keyConverterClass, valueConverterClass, minSplits ?? Math.Min(DefaultParallelism, 2), 1), this, SerializedMode.None);
        }

        /// <summary>
        /// Read a 'new API' Hadoop InputFormat with arbitrary key and value class from HDFS,
        /// a local file system (available on all nodes), or any Hadoop-supported file system URI.
        /// The mechanism is the same as for sc.sequenceFile.
        /// 
        /// A Hadoop configuration can be passed in as a Python dict. This will be converted into a Configuration in Java
        /// 
        /// </summary>
        /// <param name="filePath">path to Hadoop file</param>
        /// <param name="inputFormatClass">fully qualified classname of Hadoop InputFormat (e.g. "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")</param>
        /// <param name="keyClass">fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.Text")</param>
        /// <param name="valueClass">fully qualified classname of value Writable class (e.g. "org.apache.hadoop.io.LongWritable")</param>
        /// <param name="keyConverterClass">(None by default)</param>
        /// <param name="valueConverterClass">(None by default)</param>
        /// <param name="conf"> Hadoop configuration, passed in as a dict (None by default)</param>
        /// <returns></returns>
        public RDD<byte[]> NewAPIHadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass = null, string valueConverterClass = null, IEnumerable<KeyValuePair<string, string>> conf = null)
        {
            return new RDD<byte[]>(SparkContextProxy.NewAPIHadoopFile(filePath, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, conf, 1), this, SerializedMode.None);
        }

        /// <summary>
        /// Read a 'new API' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
        /// Hadoop configuration, which is passed in as a Python dict.
        /// This will be converted into a Configuration in Java.
        /// The mechanism is the same as for sc.sequenceFile.
        /// 
        /// </summary>
        /// <param name="inputFormatClass">fully qualified classname of Hadoop InputFormat (e.g. "org.apache.hadoop.mapreduce.lib.input.TextInputFormat")</param>
        /// <param name="keyClass">fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.Text")</param>
        /// <param name="valueClass">fully qualified classname of value Writable class (e.g. "org.apache.hadoop.io.LongWritable")</param>
        /// <param name="keyConverterClass">(None by default)</param>
        /// <param name="valueConverterClass">(None by default)</param>
        /// <param name="conf">Hadoop configuration, passed in as a dict (None by default)</param>
        /// <returns></returns>
        public RDD<byte[]> NewAPIHadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass = null, string valueConverterClass = null, IEnumerable<KeyValuePair<string, string>> conf = null)
        {
            return new RDD<byte[]>(SparkContextProxy.NewAPIHadoopRDD(inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, conf, 1), this, SerializedMode.None);
        }

        /// <summary>
        /// Read an 'old' Hadoop InputFormat with arbitrary key and value class from HDFS,
        /// a local file system (available on all nodes), or any Hadoop-supported file system URI.
        /// The mechanism is the same as for sc.sequenceFile.
        /// 
        /// A Hadoop configuration can be passed in as a Python dict. This will be converted into a Configuration in Java.
        /// 
        /// </summary>
        /// <param name="filePath">path to Hadoop file</param>
        /// <param name="inputFormatClass">fully qualified classname of Hadoop InputFormat (e.g. "org.apache.hadoop.mapred.TextInputFormat")</param>
        /// <param name="keyClass">fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.Text")</param>
        /// <param name="valueClass">fully qualified classname of value Writable class (e.g. "org.apache.hadoop.io.LongWritable")</param>
        /// <param name="keyConverterClass">(None by default)</param>
        /// <param name="valueConverterClass">(None by default)</param>
        /// <param name="conf">Hadoop configuration, passed in as a dict (None by default)</param>
        /// <returns></returns>
        public RDD<byte[]> HadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass = null, string valueConverterClass = null, IEnumerable<KeyValuePair<string, string>> conf = null)
        {
            return new RDD<byte[]>(SparkContextProxy.HadoopFile(filePath, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, conf, 1), this, SerializedMode.None);
        }

        /// <summary>
        /// Read an 'old' Hadoop InputFormat with arbitrary key and value class, from an arbitrary
        /// Hadoop configuration, which is passed in as a Python dict.
        /// This will be converted into a Configuration in Java.
        /// The mechanism is the same as for sc.sequenceFile.
        /// 
        /// </summary>
        /// <param name="inputFormatClass">fully qualified classname of Hadoop InputFormat (e.g. "org.apache.hadoop.mapred.TextInputFormat")</param>
        /// <param name="keyClass">fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.Text")</param>
        /// <param name="valueClass">fully qualified classname of value Writable class (e.g. "org.apache.hadoop.io.LongWritable")</param>
        /// <param name="keyConverterClass">(None by default)</param>
        /// <param name="valueConverterClass">(None by default)</param>
        /// <param name="conf">Hadoop configuration, passed in as a dict (None by default)</param>
        /// <returns></returns>
        public RDD<byte[]> HadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass = null, string valueConverterClass = null, IEnumerable<KeyValuePair<string, string>> conf = null)
        {
            return new RDD<byte[]>(SparkContextProxy.HadoopRDD(inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, conf, 1), this, SerializedMode.None);
        }

        /// <summary>
        /// Build the union of a list of RDDs.
        /// 
        /// This supports unions() of RDDs with different serialized formats,
        /// although this forces them to be reserialized using the default serializer:
        /// 
        /// >>> path = os.path.join(tempdir, "union-text.txt")
        /// >>> with open(path, "w") as testFile:
        /// ...    _ = testFile.write("Hello")
        /// >>> textFile = sc.textFile(path)
        /// >>> textFile.collect()
        /// [u'Hello']
        /// >>> parallelized = sc.parallelize(["World!"])
        /// >>> sorted(sc.union([textFile, parallelized]).collect())
        /// [u'Hello', 'World!']
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rdds"></param>
        /// <returns></returns>
        public RDD<T> Union<T>(IEnumerable<RDD<T>> rdds)
        {
            if (rdds == null || !rdds.Any())
                return EmptyRDD<T>();
            if (rdds.Count() == 1)
                return rdds.First();
            return new RDD<T>(SparkContextProxy.Union(rdds.Select(rdd => rdd.RddProxy)), this, rdds.First().serializedMode);
        }

        /// <summary>
        /// Broadcast a read-only variable to the cluster, returning a Broadcast
        /// object for reading it in distributed functions. The variable will
        /// be sent to each cluster only once.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public Broadcast<T> Broadcast<T>(T value)
        {
            var broadcast = new Broadcast<T>(this, value);
            return broadcast;
        }

        /// <summary>
        /// Create an <see cref="Accumulator"/> with the given initial value, using a given
        /// <see cref="AccumulatorParam{T}"/> helper object to define how to add values of the
        /// data type if provided. Default AccumulatorParams are used for integers
        /// and floating-point numbers if you do not provide one. For other types,
        /// a custom AccumulatorParam can be used.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public Accumulator<T> Accumulator<T>(T value)
        {
            if (accumulatorServer == null)
            {
                StartAccumulatorServer();
            }

            return new Accumulator<T>(nextAccumulatorId++, value);
        }

        /// <summary>
        /// Shut down the SparkContext.
        /// </summary>
        public void Stop()
        {
            logger.LogInfo("Stopping SparkContext");
            logger.LogInfo("Note that there might be error in Spark logs on the failure to delete userFiles directory " +
                           "under Spark temp directory (spark.local.dir config value in local mode)");
            logger.LogInfo("This error may be ignored for now. See https://issues.apache.org/jira/browse/SPARK-8333 for details");

            if (accumulatorServer != null)
                accumulatorServer.Shutdown();

            SparkContextProxy.Stop();

        }

        /// <summary>
        /// Add a file to be downloaded with this Spark job on every node.
        /// The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
        /// filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
        /// use `SparkFiles.get(fileName)` to find its download location.
        /// </summary>
        /// <param name="path"></param>
        public void AddFile(string path)
        {
            SparkContextProxy.AddFile(path);
        }

        /// <summary>
        /// Set the directory under which RDDs are going to be checkpointed. The directory must
        /// be a HDFS path if running on a cluster.
        /// </summary>
        /// <param name="directory"></param>
        public void SetCheckpointDir(string directory)
        {
            SparkContextProxy.SetCheckpointDir(directory);
        }

        /// <summary>
        /// Assigns a group ID to all the jobs started by this thread until the group ID is set to a
        /// different value or cleared.
        ///
        /// Often, a unit of execution in an application consists of multiple Spark actions or jobs.
        /// Application programmers can use this method to group all those jobs together and give a
        /// group description. Once set, the Spark web UI will associate such jobs with this group.
        ///
        /// The application can also use [[org.apache.spark.api.java.JavaSparkContext.cancelJobGroup]]
        /// to cancel all running jobs in this group. For example,
        /// {{{
        /// // In the main thread:
        /// sc.setJobGroup("some_job_to_cancel", "some job description");
        /// rdd.map(...).count();
        ///
        /// // In a separate thread:
        /// sc.cancelJobGroup("some_job_to_cancel");
        /// }}}
        ///
        /// If interruptOnCancel is set to true for the job group, then job cancellation will result
        /// in Thread.interrupt() being called on the job's executor threads. This is useful to help ensure
        /// that the tasks are actually stopped in a timely manner, but is off by default due to HDFS-1208,
        /// where HDFS may respond to Thread.interrupt() by marking nodes as dead.
        /// </summary>
        /// <param name="groupId"></param>
        /// <param name="description"></param>
        /// <param name="interruptOnCancel"></param>
        public void SetJobGroup(string groupId, string description, bool interruptOnCancel = false)
        {
            SparkContextProxy.SetJobGroup(groupId, description, interruptOnCancel);
        }

        /// <summary>
        /// Set a local property that affects jobs submitted from this thread, such as the
        /// Spark fair scheduler pool.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void SetLocalProperty(string key, string value)
        {
            SparkContextProxy.SetLocalProperty(key, value);
        }

        /// <summary>
        /// Get a local property set in this thread, or null if it is missing. See
        /// [[org.apache.spark.api.java.JavaSparkContext.setLocalProperty]].
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string GetLocalProperty(string key)
        {
            return SparkContextProxy.GetLocalProperty(key);
        }

        /// <summary>
        /// Control our logLevel. This overrides any user-defined log settings.
        /// @param logLevel The desired log level as a string.
        /// Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
        /// </summary>
        /// <param name="logLevel"></param>
        public void SetLogLevel(string logLevel)
        {
            SparkContextProxy.SetLogLevel(logLevel);
        }

        /// <summary>
        /// Cancel active jobs for the specified group. See <see cref="SetJobGroup"/> for more information.
        /// </summary>
        /// <param name="groupId"></param>
        public void CancelJobGroup(string groupId)
        {
            SparkContextProxy.CancelJobGroup(groupId);
        }

        /// <summary>
        /// Cancel all jobs that have been scheduled or are running.
        /// </summary>
        public void CancelAllJobs()
        {
            SparkContextProxy.CancelAllJobs();
        }

        internal static byte[] BuildCommand(CSharpWorkerFunc workerFunc, SerializedMode deserializerMode = SerializedMode.Byte, SerializedMode serializerMode = SerializedMode.Byte)
        {
            var formatter = new BinaryFormatter();
            var stream = new MemoryStream();
            formatter.Serialize(stream, workerFunc);
            List<byte[]> commandPayloadBytesList = new List<byte[]>();

            // reserve 12 bytes for RddId, stageId and partitionId, this info will be filled in CSharpRDD.scala
            byte[] rddInfo = new byte[12];
            for (int i = 0; i < rddInfo.Length; i++)
            {
                rddInfo[i] = 0;
            }
            commandPayloadBytesList.Add(rddInfo);

            // add deserializer mode
            var modeBytes = Encoding.UTF8.GetBytes(deserializerMode.ToString());
            var length = modeBytes.Length;
            var lengthAsBytes = BitConverter.GetBytes(length);
            Array.Reverse(lengthAsBytes);
            commandPayloadBytesList.Add(lengthAsBytes);
            commandPayloadBytesList.Add(modeBytes);
            // add serializer mode
            modeBytes = Encoding.UTF8.GetBytes(serializerMode.ToString());
            length = modeBytes.Length;
            lengthAsBytes = BitConverter.GetBytes(length);
            Array.Reverse(lengthAsBytes);
            commandPayloadBytesList.Add(lengthAsBytes);
            commandPayloadBytesList.Add(modeBytes);
            // add func
            var funcBytes = stream.ToArray();
            var funcBytesLengthAsBytes = BitConverter.GetBytes(funcBytes.Length);
            Array.Reverse(funcBytesLengthAsBytes);
            commandPayloadBytesList.Add(funcBytesLengthAsBytes);
            commandPayloadBytesList.Add(funcBytes);
            return commandPayloadBytesList.SelectMany(byteArray => byteArray).ToArray();
        }
    }
}
