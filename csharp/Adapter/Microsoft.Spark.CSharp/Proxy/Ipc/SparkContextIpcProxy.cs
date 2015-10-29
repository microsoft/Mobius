// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy.Ipc;

namespace Microsoft.Spark.CSharp.Proxy
{
    internal class SparkContextIpcProxy : ISparkContextProxy
    {
        private JvmObjectReference jvmSparkContextReference;
        private JvmObjectReference jvmJavaContextReference;
        private JvmObjectReference jvmAccumulatorReference;

        internal JvmObjectReference JvmSparkContextReference
        {
            get { return jvmSparkContextReference; }
        }

        public void CreateSparkContext(string master, string appName, string sparkHome, ISparkConfProxy conf)
        {
            object[] args = (new object[] { master, appName, sparkHome, (conf == null ? null : (conf as SparkConfIpcProxy).JvmSparkConfReference) }).Where(x => x != null).ToArray();
            jvmSparkContextReference = SparkCLREnvironment.JvmBridge.CallConstructor("org.apache.spark.SparkContext", args);
            jvmJavaContextReference = SparkCLREnvironment.JvmBridge.CallConstructor("org.apache.spark.api.java.JavaSparkContext", new object[] { jvmSparkContextReference });
        }

        public void SetLogLevel(string logLevel)
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "setLogLevel", new object[] { logLevel });
        }

        private string version;
        public string Version
        {
            get { if (version == null) { version = (string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "version"); } return version; }
        }

        private long? startTime;
        public long StartTime
        {
            get { if (startTime == null) { startTime = (long)(double)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "startTime"); } return (long)startTime; }
        }

        private int? defaultParallelism;
        public int DefaultParallelism
        {
            get { if (defaultParallelism == null) { defaultParallelism = (int)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "defaultParallelism"); } return (int)defaultParallelism; }
        }

        private int? defaultMinPartitions;
        public int DefaultMinPartitions
        {
            get { if (defaultMinPartitions == null) { defaultMinPartitions = (int)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "defaultMinPartitions"); } return (int)defaultMinPartitions; }
        }
        public void Accumulator(string host, int port)
        {
            jvmAccumulatorReference = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "accumulator", 
                SparkCLREnvironment.JvmBridge.CallConstructor("java.util.ArrayList"),
                SparkCLREnvironment.JvmBridge.CallConstructor("org.apache.spark.api.python.PythonAccumulatorParam", host, port)
            ));
        }

        public void Stop()
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "stop", new object[] { });
            SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("SparkCLRHandler", "stopBackend", new object[] { }); //className and methodName hardcoded in CSharpBackendHandler
        }

        public IRDDProxy EmptyRDD<T>()
        {
            var jvmRddReference = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "emptyRDD"));
            return new RDDIpcProxy(jvmRddReference);
        }

        //TODO - this implementation is slow. Replace with call to createRDDFromArray() in CSharpRDD
        public IRDDProxy Parallelize(IEnumerable<byte[]> values, int? numSlices)
        {
            JvmObjectReference jvalues = GetJavaList(values);
            var jvmRddReference = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "parallelize", new object[] { jvalues, numSlices }).ToString());
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy TextFile(string filePath, int minPartitions)
        {
            var jvmRddReference = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "textFile", new object[] { filePath, minPartitions }).ToString());
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy WholeTextFiles(string filePath, int minPartitions)
        {
            var jvmRddReference = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "wholeTextFiles", new object[] { filePath, minPartitions }).ToString());
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy BinaryFiles(string filePath, int minPartitions)
        {
            var jvmRddReference = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "binaryFiles", new object[] { filePath, minPartitions }).ToString());
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy SequenceFile(string filePath, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, int minSplits, int batchSize)
        {
            var jvmRddReference = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "sequenceFile",
                new object[] { jvmJavaContextReference, filePath, keyClass, valueClass, keyConverterClass, valueConverterClass, minSplits, batchSize }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy NewAPIHadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            var jconf = GetJavaMap<string, string>(conf) as JvmObjectReference;
            var jvmRddReference = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "newAPIHadoopFile",
                new object[] { jvmJavaContextReference, filePath, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, jconf, batchSize }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy NewAPIHadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            var jconf = GetJavaMap<string, string>(conf) as JvmObjectReference;
            var jvmRddReference = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "newAPIHadoopRDD",
                new object[] { jvmJavaContextReference, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, jconf, batchSize }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy HadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            var jconf = GetJavaMap<string, string>(conf) as JvmObjectReference;
            var jvmRddReference = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "hadoopFile",
                new object[] { jvmJavaContextReference, filePath, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, jconf, batchSize }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy HadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            var jconf = GetJavaMap<string, string>(conf) as JvmObjectReference;
            var jvmRddReference = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "hadoopRDD",
                new object[] { jvmJavaContextReference, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, jconf, batchSize }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy CheckpointFile(string filePath)
        {
            var jvmRddReference = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "checkpointFile", new object[] { filePath }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy Union<T>(IEnumerable<RDD<T>> rdds)
        {
            int count = rdds == null ? 0 : rdds.Count();
            if (count == 0)
                return null;
            if (count == 1)
                return rdds.First().RddProxy;

            var jfirst = (rdds.First().RddProxy as RDDIpcProxy).JvmRddReference;
            var jrest = GetJavaList(rdds.TakeWhile((r, i) => i > 0).Select(r => (r.RddProxy as RDDIpcProxy).JvmRddReference)) as JvmObjectReference;
            var jvmRddReference = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "union", new object[] { jfirst, jrest }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public void AddFile(string path)
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmSparkContextReference, "addFile", new object[] { path });
        }

        public void SetCheckpointDir(string directory)
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmSparkContextReference, "setCheckpointDir", new object[] { directory });
        }

        public void SetJobGroup(string groupId, string description, bool interruptOnCancel)
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "setCheckpointDir", new object[] { groupId, description, interruptOnCancel });
        }

        public void SetLocalProperty(string key, string value)
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "setLocalProperty", new object[] { key, value });
        }
        public string GetLocalProperty(string key)
        {
            return (string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "getLocalProperty", new object[] { key });
        }
        public string SparkUser
        {
            get
            {
                return (string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmSparkContextReference, "sparkUser");
            }
        }

        public void CancelJobGroup(string groupId)
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "cancelJobGroup", new object[] { groupId });
        }
        public void CancelAllJobs()
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "cancelAllJobs");
        }

        private IStatusTrackerProxy statusTracker;
        public IStatusTrackerProxy StatusTracker
        {
            get
            {
                if (statusTracker == null)
                {
                    statusTracker = new StatusTrackerIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "statusTracker")));
                }
                return statusTracker;
            }
        }

        public IRDDProxy CreatePairwiseRDD<K, V>(IRDDProxy jvmReferenceOfByteArrayRdd, int numPartitions)
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod((jvmReferenceOfByteArrayRdd as RDDIpcProxy).JvmRddReference, "rdd"));
            var pairwiseRdd = SparkCLREnvironment.JvmBridge.CallConstructor("org.apache.spark.api.python.PairwiseRDD", rdd);
            var pairRddJvmReference = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(pairwiseRdd, "asJavaPairRDD", new object[] { }).ToString());

            var jpartitionerJavaReference = SparkCLREnvironment.JvmBridge.CallConstructor("org.apache.spark.api.python.PythonPartitioner", new object[] { numPartitions, 0 });
            var partitionedPairRddJvmReference = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(pairRddJvmReference, "partitionBy", new object[] { jpartitionerJavaReference }).ToString());
            var jvmRddReference = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "valueOfPair", new object[] { partitionedPairRddJvmReference }).ToString());
            //var jvmRddReference = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(partitionedRddJvmReference, "rdd", new object[] { }).ToString());
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy CreateCSharpRdd(IRDDProxy prevJvmRddReference, byte[] command, Dictionary<string, string> environmentVariables, List<string> pythonIncludes, bool preservesPartitioning, List<Broadcast> broadcastVariables, List<byte[]> accumulator)
        {
            var hashTableReference = SparkCLREnvironment.JvmBridge.CallConstructor("java.util.Hashtable", new object[] { });
            var arrayListReference = SparkCLREnvironment.JvmBridge.CallConstructor("java.util.ArrayList", new object[] { });
            var jbroadcastVariables = GetJavaList<JvmObjectReference>(broadcastVariables.Select(x => new JvmObjectReference(x.broadcastObjId)));

            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod((prevJvmRddReference as RDDIpcProxy).JvmRddReference, "rdd"));

            var csRdd = SparkCLREnvironment.JvmBridge.CallConstructor("org.apache.spark.api.csharp.CSharpRDD",
                new object[]
                {
                    rdd, command, hashTableReference, arrayListReference, preservesPartitioning, 
                    SparkCLREnvironment.ConfigurationService.GetCSharpRDDExternalProcessName(),
                    "1.0",
                    jbroadcastVariables, jvmAccumulatorReference
                });

            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(csRdd, "asJavaRDD")));

        }
        public IRDDProxy CreateUserDefinedCSharpFunction(string name, byte[] command, string returnType = "string")
        {
            var jSqlContext = SparkCLREnvironment.JvmBridge.CallConstructor("org.apache.spark.sql.SQLContext", new object[] { (SparkCLREnvironment.SparkContextProxy as SparkContextIpcProxy).jvmSparkContextReference });
            var jDataType = SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jSqlContext, "parseDataType", new object[] { "\"" + returnType + "\"" });

            var hashTableReference = SparkCLREnvironment.JvmBridge.CallConstructor("java.util.Hashtable", new object[] { });
            var arrayListReference = SparkCLREnvironment.JvmBridge.CallConstructor("java.util.ArrayList", new object[] { });

            return new RDDIpcProxy(SparkCLREnvironment.JvmBridge.CallConstructor("org.apache.spark.sql.UserDefinedPythonFunction",
                new object[]
                {
                    name, command, hashTableReference, arrayListReference, 
                    SparkCLREnvironment.ConfigurationService.GetCSharpRDDExternalProcessName(),
                    "1.0",
                    arrayListReference, null, jDataType
                }));

        }
        
        public int RunJob(IRDDProxy rdd, IEnumerable<int> partitions, bool allowLocal)
        {
            var jpartitions = GetJavaList<int>(partitions);
            return int.Parse(SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "runJob", new object[] { jvmSparkContextReference, (rdd as RDDIpcProxy).JvmRddReference, jpartitions, allowLocal }).ToString());
        }

        public string ReadBroadcastFromFile(string path, out long broadcastId)
        {
            string broadcastObjId = (string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "readBroadcastFromFile", new object[] { jvmJavaContextReference, path });
            broadcastId = (long)(double)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(new JvmObjectReference(broadcastObjId), "id");
            return broadcastObjId;
        }

        public void UnpersistBroadcast(string broadcastObjId, bool blocking)
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(new JvmObjectReference(broadcastObjId), "unpersist", new object[] { blocking });
        }

        public IColumnProxy CreateColumnFromName(string name)
        {
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.functions", "col", name)));
        }

        public IColumnProxy CreateFunction(string name, object self)
        {
            if (self is ColumnIpcProxy)
                self = (self as ColumnIpcProxy).ScalaColumnReference;
            else if (self is IColumnProxy[])
                self = GetJavaSeq<JvmObjectReference>((self as IColumnProxy[]).Select(x => (x as ColumnIpcProxy).ScalaColumnReference));
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.functions", name, self)));
        }

        public IColumnProxy CreateBinaryMathFunction(string name, object self, object other)
        {
            if (self is ColumnIpcProxy)
                self = (self as ColumnIpcProxy).ScalaColumnReference;
            if (other is ColumnIpcProxy)
                other = (self as ColumnIpcProxy).ScalaColumnReference;
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.functions", name, self, other)));
        }

        public IColumnProxy CreateWindowFunction(string name)
        {
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.functions", name)));
        }

        public static JvmObjectReference GetJavaMap<K, V>(IEnumerable<KeyValuePair<K, V>> enumerable)
        {
            var jmap = SparkCLREnvironment.JvmBridge.CallConstructor("java.util.Hashtable", new object[] { });
            if (enumerable != null)
            {
                foreach (var item in enumerable)
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jmap, "put", new object[] { item.Key, item.Value });
            }
            return jmap;
        }
        public static JvmObjectReference GetJavaSet<T>(IEnumerable<T> enumerable)
        {
            var jset = SparkCLREnvironment.JvmBridge.CallConstructor("java.util.HashSet", new object[] { });
            if (enumerable != null)
            {
                foreach (var item in enumerable)
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jset, "add", new object[] { item });
            }
            return jset;
        }
        public static JvmObjectReference GetJavaList<T>(IEnumerable<T> enumerable)
        {
            var jlist = SparkCLREnvironment.JvmBridge.CallConstructor("java.util.ArrayList", new object[] { });
            if (enumerable != null)
            {
                foreach (var item in enumerable)
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jlist, "add", new object[] { item });
            }
            return jlist;
        }
        public JvmObjectReference GetJavaSeq<T>(IEnumerable<T> enumerable)
        {
            return new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", GetJavaList<T>(enumerable)));
        }
    }
}
