// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class SparkContextIpcProxy : ISparkContextProxy
    {
        private JvmObjectReference jvmSparkContextReference;
        private JvmObjectReference jvmJavaContextReference;
        private JvmObjectReference jvmAccumulatorReference;
        internal List<JvmObjectReference> jvmBroadcastReferences = new List<JvmObjectReference>();

        internal JvmObjectReference JvmSparkContextReference
        {
            get { return jvmSparkContextReference; }
        }

        public SparkContextIpcProxy(JvmObjectReference jvmSparkContextReference, JvmObjectReference jvmJavaContextReference)
        {
            this.jvmSparkContextReference = jvmSparkContextReference;
            this.jvmJavaContextReference = jvmJavaContextReference;
        }
        
        public ISqlContextProxy CreateSqlContext()
        {
            return new SqlContextIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "createSQLContext", new object[] { jvmSparkContextReference })));
        }

        public void CreateSparkContext(string master, string appName, string sparkHome, ISparkConfProxy conf)
        {
            object[] args = (new object[] { master, appName, sparkHome, (conf == null ? null : (conf as SparkConfIpcProxy).JvmSparkConfReference) }).Where(x => x != null).ToArray();
            jvmSparkContextReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.SparkContext", args);
            jvmJavaContextReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.api.java.JavaSparkContext", new object[] { jvmSparkContextReference });
        }

        public void SetLogLevel(string logLevel)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "setLogLevel", new object[] { logLevel });
        }

        private string version;
        public string Version
        {
            get {
                return version ??
                       (version =
                           (string)
                               SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "version"));
            }
        }

        private long? startTime;
        public long StartTime
        {
            get
            {
                if (startTime == null)
                {
                    startTime =
                        (long)
                            (double)
                                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "startTime");
                }
                return (long) startTime;
            }
        }

        private int? defaultParallelism;
        public int DefaultParallelism
        {
            get
            {
                if (defaultParallelism == null)
                {
                    defaultParallelism =
                        (int)
                            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference,
                                "defaultParallelism");
                }
                return (int) defaultParallelism;
            }
        }

        private int? defaultMinPartitions;
        public int DefaultMinPartitions
        {
            get { if (defaultMinPartitions == null) { defaultMinPartitions = (int)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "defaultMinPartitions"); } return (int)defaultMinPartitions; }
        }
        public void Accumulator(int port)
        {
            jvmAccumulatorReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "accumulator", 
                SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.ArrayList"),
                SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.api.python.PythonAccumulatorParam", IPAddress.Loopback.ToString(), port)
            ));
        }

        public void Stop()
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "stop", new object[] { });
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("SparkCLRHandler", "stopBackend", new object[] { }); //className and methodName hardcoded in CSharpBackendHandler
        }

        public IRDDProxy EmptyRDD()
        {
            var jvmRddReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "emptyRDD"));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy Parallelize(IEnumerable<byte[]> values, int numSlices)
        {
            var jvmRddReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.csharp.CSharpRDD", "createRDDFromArray", new object[] { jvmSparkContextReference, values, numSlices }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy TextFile(string filePath, int minPartitions)
        {
            var jvmRddReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "textFile", new object[] { filePath, minPartitions }).ToString());
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy WholeTextFiles(string filePath, int minPartitions)
        {
            var jvmRddReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "wholeTextFiles", new object[] { filePath, minPartitions }).ToString());
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy BinaryFiles(string filePath, int minPartitions)
        {
            var jvmRddReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "binaryFiles", new object[] { filePath, minPartitions }).ToString());
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy SequenceFile(string filePath, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, int minSplits, int batchSize)
        {
            var jvmRddReference = new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "sequenceFile",
                new object[] { jvmJavaContextReference, filePath, keyClass, valueClass, keyConverterClass, valueConverterClass, minSplits, batchSize }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy NewAPIHadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            var jconf = GetJavaMap<string, string>(conf);
            var jvmRddReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "newAPIHadoopFile",
                new object[] { jvmJavaContextReference, filePath, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, jconf, batchSize }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy NewAPIHadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            var jconf = GetJavaMap<string, string>(conf);
            var jvmRddReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "newAPIHadoopRDD",
                new object[] { jvmJavaContextReference, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, jconf, batchSize }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy HadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            var jconf = GetJavaMap<string, string>(conf);
            var jvmRddReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "hadoopFile",
                new object[] { jvmJavaContextReference, filePath, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, jconf, batchSize }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy HadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            var jconf = GetJavaMap<string, string>(conf);
            var jvmRddReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "hadoopRDD",
                new object[] { jvmJavaContextReference, inputFormatClass, keyClass, valueClass, keyConverterClass, valueConverterClass, jconf, batchSize }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy CheckpointFile(string filePath)
        {
            var jvmRddReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "checkpointFile", new object[] { filePath }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy Union(IEnumerable<IRDDProxy> rdds)
        {
            var jfirst = (rdds.First() as RDDIpcProxy).JvmRddReference;
            var jrest = GetJavaList<JvmObjectReference>(rdds.Skip(1).Select(r => (r as RDDIpcProxy).JvmRddReference));
            var jvmRddReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "union", new object[] { jfirst, jrest }));
            return new RDDIpcProxy(jvmRddReference);
        }

        public void AddFile(string path)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkContextReference, "addFile", new object[] { path });
        }

        public void SetCheckpointDir(string directory)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkContextReference, "setCheckpointDir", new object[] { directory });
        }

        public void SetJobGroup(string groupId, string description, bool interruptOnCancel)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "setCheckpointDir", new object[] { groupId, description, interruptOnCancel });
        }

        public void SetLocalProperty(string key, string value)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "setLocalProperty", new object[] { key, value });
        }
        public string GetLocalProperty(string key)
        {
            return (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "getLocalProperty", new object[] { key });
        }
        public string SparkUser
        {
            get
            {
                return (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmSparkContextReference, "sparkUser");
            }
        }

        public void CancelJobGroup(string groupId)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "cancelJobGroup", new object[] { groupId });
        }
        public void CancelAllJobs()
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference, "cancelAllJobs");
        }

        private IStatusTrackerProxy statusTracker;
        public IStatusTrackerProxy StatusTracker
        {
            get {
                return statusTracker ??
                       (statusTracker =
                           new StatusTrackerIpcProxy(
                               new JvmObjectReference(
                                   (string)
                                       SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaContextReference,
                                           "statusTracker"))));
            }
        }

        public IRDDProxy CreatePairwiseRDD(IRDDProxy jvmReferenceOfByteArrayRdd, int numPartitions)
        {
            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod((jvmReferenceOfByteArrayRdd as RDDIpcProxy).JvmRddReference, "rdd"));
            var pairwiseRdd = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.api.python.PairwiseRDD", rdd);
            var pairRddJvmReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(pairwiseRdd, "asJavaPairRDD", new object[] { }).ToString());

            var jpartitionerJavaReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.api.python.PythonPartitioner", new object[] { numPartitions, (long)0 });
            var partitionedPairRddJvmReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(pairRddJvmReference, "partitionBy", new object[] { jpartitionerJavaReference }).ToString());
            var jvmRddReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "valueOfPair", new object[] { partitionedPairRddJvmReference }).ToString());
            //var jvmRddReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(partitionedRddJvmReference, "rdd", new object[] { }).ToString());
            return new RDDIpcProxy(jvmRddReference);
        }

        public IRDDProxy CreateCSharpRdd(IRDDProxy prevJvmRddReference, byte[] command, Dictionary<string, string> environmentVariables, List<string> pythonIncludes, bool preservesPartitioning, List<Broadcast> broadcastVariables, List<byte[]> accumulator)
        {
            var hashTableReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.Hashtable", new object[] { });
            var arrayListReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.ArrayList", new object[] { });
            var jbroadcastVariables = GetJavaList<JvmObjectReference>(jvmBroadcastReferences);

            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod((prevJvmRddReference as RDDIpcProxy).JvmRddReference, "rdd"));

            var csRdd = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.api.csharp.CSharpRDD",
                new object[]
                {
                    rdd, command, hashTableReference, arrayListReference, preservesPartitioning, 
                    SparkCLREnvironment.ConfigurationService.GetCSharpWorkerExePath(),
                    "1.0",
                    jbroadcastVariables, jvmAccumulatorReference
                });

            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(csRdd, "asJavaRDD")));

        }
        
        public IUDFProxy CreateUserDefinedCSharpFunction(string name, byte[] command, string returnType = "string")
        {
            var jSqlContext = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.sql.SQLContext", new object[] { jvmSparkContextReference });
            var jDataType = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jSqlContext, "parseDataType", new object[] { "\"" + returnType + "\"" }));
            var jbroadcastVariables = GetJavaList<JvmObjectReference>(jvmBroadcastReferences);

            var hashTableReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.Hashtable", new object[] { });
            var arrayListReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.ArrayList", new object[] { });

            return new UDFIpcProxy(SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.sql.UserDefinedPythonFunction",
                new object[]
                {
                    name, command, hashTableReference, arrayListReference, 
                    SparkCLREnvironment.ConfigurationService.GetCSharpWorkerExePath(),
                    "1.0",
                    jbroadcastVariables, jvmAccumulatorReference, jDataType
                }));

        }
        
        public int RunJob(IRDDProxy rdd, IEnumerable<int> partitions)
        {
            var jpartitions = GetJavaList<int>(partitions);
            return int.Parse(SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "runJob", new object[] { jvmSparkContextReference, (rdd as RDDIpcProxy).JvmRddReference, jpartitions }).ToString());
        }

        public IBroadcastProxy ReadBroadcastFromFile(string path, out long broadcastId)
        {
            JvmObjectReference jbroadcast = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "readBroadcastFromFile", new object[] { jvmJavaContextReference, path }));
            broadcastId = (long)(double)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jbroadcast, "id");
            jvmBroadcastReferences.Add(jbroadcast);
            return new BroadcastIpcProxy(jbroadcast, this);        
        }

        public void UnpersistBroadcast(string broadcastObjId, bool blocking)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(new JvmObjectReference(broadcastObjId), "unpersist", new object[] { blocking });
        }

        public IColumnProxy CreateColumnFromName(string name)
        {
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.functions", "col", name)));
        }

        public IColumnProxy CreateFunction(string name, object self)
        {
            if (self is ColumnIpcProxy)
                self = (self as ColumnIpcProxy).ScalaColumnReference;
            else if (self is IColumnProxy[])
                self = GetJavaSeq<JvmObjectReference>((self as IColumnProxy[]).Select(x => (x as ColumnIpcProxy).ScalaColumnReference));
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.functions", name, self)));
        }

        public IColumnProxy CreateBinaryMathFunction(string name, object self, object other)
        {
            if (self is ColumnIpcProxy)
                self = (self as ColumnIpcProxy).ScalaColumnReference;
            if (other is ColumnIpcProxy)
                other = (self as ColumnIpcProxy).ScalaColumnReference;
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.functions", name, self, other)));
        }

        public IColumnProxy CreateWindowFunction(string name)
        {
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.functions", name)));
        }

        public static JvmObjectReference GetJavaMap<K, V>(IEnumerable<KeyValuePair<K, V>> enumerable)
        {
            var jmap = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.Hashtable", new object[] { });
            if (enumerable != null)
            {
                foreach (var item in enumerable)
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jmap, "put", new object[] { item.Key, item.Value });
            }
            return jmap;
        }
        public static JvmObjectReference GetJavaSet<T>(IEnumerable<T> enumerable)
        {
            var jset = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.HashSet", new object[] { });
            if (enumerable != null)
            {
                foreach (var item in enumerable)
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jset, "add", new object[] { item });
            }
            return jset;
        }
        public static JvmObjectReference GetJavaList<T>(IEnumerable<T> enumerable)
        {
            var jlist = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.ArrayList", new object[] { });
            if (enumerable != null)
            {
                foreach (var item in enumerable)
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jlist, "add", new object[] { item });
            }
            return jlist;
        }
        public static JvmObjectReference GetJavaSeq<T>(IEnumerable<T> enumerable)
        {
            return new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", GetJavaList<T>(enumerable)));
        }
        public static JvmObjectReference GetJavaStorageLevel(StorageLevelType storageLevelType)
        {
            return new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.java.StorageLevels", "create",
                new object[] 
                { 
                    StorageLevel.storageLevel[storageLevelType].useDisk,
                    StorageLevel.storageLevel[storageLevelType].useMemory,
                    StorageLevel.storageLevel[storageLevelType].useOffHeap,
                    StorageLevel.storageLevel[storageLevelType].deserialized,
                    StorageLevel.storageLevel[storageLevelType].replication
                }).ToString());
        }
    }
    
    internal class BroadcastIpcProxy : IBroadcastProxy
    {
        private readonly JvmObjectReference jvmBroadcastReference;
        private readonly SparkContextIpcProxy sparkContextIpcProxy;

        public BroadcastIpcProxy(JvmObjectReference jvmBroadcastReference, SparkContextIpcProxy sparkContextIpcProxy)
        {
            this.jvmBroadcastReference = jvmBroadcastReference;
            this.sparkContextIpcProxy = sparkContextIpcProxy;
        }
        public void Unpersist(bool blocking)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmBroadcastReference, "unpersist", new object[] { blocking });
            sparkContextIpcProxy.jvmBroadcastReferences.Remove(jvmBroadcastReference);
        }
    }
}
