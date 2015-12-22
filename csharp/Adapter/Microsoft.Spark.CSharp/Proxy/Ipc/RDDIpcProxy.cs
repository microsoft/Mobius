// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class RDDIpcProxy : IRDDProxy
    {
        private readonly JvmObjectReference jvmRddReference;

        internal JvmObjectReference JvmRddReference
        {
            get { return jvmRddReference; }
        }

        private IRDDCollector rddCollector;
        public IRDDCollector RDDCollector
        {
            get { return rddCollector ?? (rddCollector = new RDDCollector()); }

            set
            {
                rddCollector = value;
            }
        }

        public string Name
        {
            get
            {
                var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
                return (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(rdd, "name");
            }
        }

        public bool IsCheckpointed
        {
            get
            {
                var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
                return (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(rdd, "isCheckpointed");
            }
        }

        public RDDIpcProxy(JvmObjectReference jvmRddReference)
        {
            this.jvmRddReference = jvmRddReference;
        }

        public long Count()
        {
            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return long.Parse(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(rdd, "count").ToString());
        }

        public int CollectAndServe()
        {
            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return int.Parse(SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "collectAndServe", new object[] { rdd }).ToString());
        }


        public IRDDProxy Union(IRDDProxy javaRddReferenceOther)
        {
            var jref = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "union", new object[] { (javaRddReferenceOther as RDDIpcProxy).jvmRddReference }).ToString());
            return new RDDIpcProxy(jref);
        }

        public int PartitionLength()
        {
            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            var partitions = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(rdd, "partitions", new object[] { });
            return int.Parse(SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("java.lang.reflect.Array", "getLength", new object[] { partitions }).ToString());
        }

        public IRDDProxy Coalesce(int numPartitions, bool shuffle)
        {
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "coalesce", new object[] { numPartitions, shuffle })));
        }

        public IRDDProxy Sample(bool withReplacement, double fraction, long seed)
        {
            var jref = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "sample", new object[] { withReplacement, fraction, seed }));
            return new RDDIpcProxy(jref);
        }

        public IRDDProxy[] RandomSplit(double[] weights, long seed)
        {
            return ((List<JvmObjectReference>)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "randomSplit", new object[] { weights, seed }))
                .Select(obj => new RDDIpcProxy(obj)).ToArray();
        }

        public IRDDProxy RandomSampleWithRange(double lb, double ub, long seed)
        {
            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(rdd, "randomSampleWithRange", new object[] { lb, ub, seed })));
        }


        public void Cache()
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "cache");
        }

        public void Persist(StorageLevelType storageLevelType)
        {
            var jstorageLevel = SparkContextIpcProxy.GetJavaStorageLevel(storageLevelType);
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "persist", new object[] { jstorageLevel });
        }

        public void Unpersist()
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "unpersist");
        }

        public void Checkpoint()
        {
            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(rdd, "checkpoint");
        }

        public string GetCheckpointFile()
        {
            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(rdd, "getCheckpointFile");
        }

        public int GetNumPartitions()
        {
            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return ((List<JvmObjectReference>)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(rdd, "partitions")).Count;
        }

        public IRDDProxy Repartition(int numPartitions)
        {
            return new RDDIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "repartition", new object[] { numPartitions })));
        }

        public IRDDProxy Cartesian(IRDDProxy other)
        {
            return new RDDIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "cartesian", (other as RDDIpcProxy).jvmRddReference)));
        }

        public IRDDProxy Pipe(string command)
        {
            var rdd = new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return new RDDIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "pipe", new object[] { command })));
        }

        public void SetName(string name)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "setName", new object[] { name });
        }

        public IRDDProxy SampleByKey(bool withReplacement, Dictionary<string, double> fractions, long seed)
        {
            var jfractions = SparkContextIpcProxy.GetJavaMap(fractions) as JvmObjectReference;
            return new RDDIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "sampleByKey", new object[] { withReplacement, jfractions, seed })));
        }

        public string ToDebugString()
        {
            var rdd = new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "toDebugString");
        }

        public IRDDProxy Zip(IRDDProxy other)
        {
            var rdd = new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return new RDDIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "zip", new object[] { (other as RDDIpcProxy).jvmRddReference })));
        }

        public void SaveAsNewAPIHadoopDataset(IEnumerable<KeyValuePair<string, string>> conf)
        {
            var jconf = SparkContextIpcProxy.GetJavaMap<string, string>(conf);
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "saveAsHadoopDataset", new object[] { jvmRddReference, false, jconf, null, null, true });
        }

        public void SaveAsNewAPIHadoopFile(string path, string outputFormatClass, string keyClass, string valueClass, IEnumerable<KeyValuePair<string, string>> conf)
        {
            var jconf = SparkContextIpcProxy.GetJavaMap<string, string>(conf);
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "saveAsNewAPIHadoopFile", new object[] { jvmRddReference, false, path, outputFormatClass, keyClass, valueClass, null, null, jconf });
        }

        public void SaveAsHadoopDataset(IEnumerable<KeyValuePair<string, string>> conf)
        {
            var jconf = SparkContextIpcProxy.GetJavaMap<string, string>(conf);
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "saveAsHadoopDataset", new object[] { jvmRddReference, false, jconf, null, null, false });
        }

        public void SaveAsHadoopFile(string path, string outputFormatClass, string keyClass, string valueClass, IEnumerable<KeyValuePair<string, string>> conf, string compressionCodecClass)
        {
            var jconf = SparkContextIpcProxy.GetJavaMap<string, string>(conf);
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "saveAsHadoopFile", new object[] { jvmRddReference, false, path, outputFormatClass, keyClass, valueClass, null, null, jconf, compressionCodecClass });
        }

        public void SaveAsSequenceFile(string path, string compressionCodecClass)
        {
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "SaveAsSequenceFile", new object[] { jvmRddReference, false, path, compressionCodecClass });
        }

        public void SaveAsTextFile(string path, string compressionCodecClass)
        {
            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            if (!string.IsNullOrEmpty(compressionCodecClass))
            {
                var codec = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("java.lang.Class", "forName", new object[] { compressionCodecClass }));
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "saveAsTextFile", new object[] { path, codec });
            }
            else
            {
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "saveAsTextFile", new object[] { path });
            }
        }
        public StorageLevel GetStorageLevel()
        {
            var rdd = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            var storageLevel = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(rdd, "getStorageLevel"));

            return new StorageLevel
            (
                (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(storageLevel, "useDisk"),
                (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(storageLevel, "useMemory"),
                (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(storageLevel, "useOffHeap"),
                (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(storageLevel, "deserialized"),
                (int)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(storageLevel, "replication")
            );
        }
    }
}
