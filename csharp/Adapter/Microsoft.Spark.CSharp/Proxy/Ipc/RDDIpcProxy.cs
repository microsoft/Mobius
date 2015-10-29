// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    internal class RDDIpcProxy : IRDDProxy
    {
        private readonly JvmObjectReference jvmRddReference;

        internal JvmObjectReference JvmRddReference
        {
            get { return jvmRddReference; }
        }

        public string Name
        {
            get
            {
                var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
                return (string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(rdd, "name");
            }
        }

        public bool IsCheckpointed
        {
            get
            {
                var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
                return (bool)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(rdd, "isCheckpointed");
            }
        }

        public RDDIpcProxy(JvmObjectReference jvmRddReference)
        {
            this.jvmRddReference = jvmRddReference;
        }

        public long Count()
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return long.Parse(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(rdd, "count").ToString());
        }

        public int CollectAndServe()
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return int.Parse(SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "collectAndServe", new object[] { rdd }).ToString());
        }


        public IRDDProxy Union(IRDDProxy javaRddReferenceOther)
        {
            var jref = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "union", new object[] { (javaRddReferenceOther as RDDIpcProxy).jvmRddReference }).ToString());
            return new RDDIpcProxy(jref);
        }

        public int PartitionLength()
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            var partitions = SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(rdd, "partitions", new object[] { });
            return int.Parse(SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("java.lang.reflect.Array", "getLength", new object[] { partitions }).ToString());
        }

        public IRDDProxy Coalesce(int numPartitions, bool shuffle)
        {
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "coalesce", new object[] { numPartitions, shuffle })));
        }

        public IRDDProxy Sample(bool withReplacement, double fraction, long seed)
        {
            var jref = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "sample", new object[] { withReplacement, fraction, seed }));
            return new RDDIpcProxy(jref);
        }

        public IRDDProxy[] RandomSplit(double[] weights, long seed)
        {
            return ((List<JvmObjectReference>)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "randomSplit", new object[] { weights, seed }))
                .Select(obj => new RDDIpcProxy(obj)).ToArray();
        }

        public IRDDProxy RandomSampleWithRange(double lb, double ub, long seed)
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(rdd, "randomSampleWithRange", new object[] { lb, ub, seed })));
        }


        public void Cache()
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "cache");
        }

        public void Persist(StorageLevelType storageLevelType)
        {
            var jstorageLevel = GetJavaStorageLevel(storageLevelType);
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "persist", new object[] { jstorageLevel });
        }

        public void Unpersist()
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "unpersist");
        }

        public void Checkpoint()
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(rdd, "checkpoint");
        }

        public string GetCheckpointFile()
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return (string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(rdd, "getCheckpointFile");
        }

        public int GetNumPartitions()
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return ((List<JvmObjectReference>)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(rdd, "partitions")).Count;
        }

        public IRDDProxy Intersection(IRDDProxy other)
        {
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "intersection", new object[] { (other as RDDIpcProxy).jvmRddReference })));
        }

        public IRDDProxy Repartition(int numPartitions)
        {
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "repartition", new object[] { numPartitions })));
        }

        public IRDDProxy Cartesian(IRDDProxy other)
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            var otherRdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod((other as RDDIpcProxy).jvmRddReference, "rdd"));
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "cartesian", new object[] { otherRdd })));
        }

        public IRDDProxy Pipe(string command)
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "pipe", new object[] { command })));
        }

        public void SetName(string name)
        {
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "setName", new object[] { name });
        }

        public IRDDProxy SampleByKey(bool withReplacement, Dictionary<string, double> fractions, long seed)
        {
            var jfractions = SparkContextIpcProxy.GetJavaMap(fractions) as JvmObjectReference;
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "sampleByKey", new object[] { withReplacement, jfractions, seed })));
        }

        public string ToDebugString()
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return (string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "toDebugString");
        }

        public IRDDProxy Zip(IRDDProxy other)
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "zip", new object[] { (other as RDDIpcProxy).jvmRddReference })));
        }

        public IRDDProxy ZipWithIndex()
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "zipWithIndex")));
        }

        public IRDDProxy ZipWithUniqueId()
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            return new RDDIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "zipWithUniqueId")));
        }

        public void SaveAsNewAPIHadoopDataset(IEnumerable<KeyValuePair<string, string>> conf)
        {
            var jconf = SparkContextIpcProxy.GetJavaMap<string, string>(conf);
            SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "saveAsHadoopDataset", new object[] { jvmRddReference, false, jconf, null, null, true });
        }

        public void SaveAsNewAPIHadoopFile(string path, string outputFormatClass, string keyClass, string valueClass, IEnumerable<KeyValuePair<string, string>> conf)
        {
            var jconf = SparkContextIpcProxy.GetJavaMap<string, string>(conf);
            SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "saveAsNewAPIHadoopFile", new object[] { jvmRddReference, false, path, outputFormatClass, keyClass, valueClass, null, null, jconf });
        }

        public void SaveAsHadoopDataset(IEnumerable<KeyValuePair<string, string>> conf)
        {
            var jconf = SparkContextIpcProxy.GetJavaMap<string, string>(conf);
            SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "saveAsHadoopDataset", new object[] { jvmRddReference, false, jconf, null, null, false });
        }

        public void saveAsHadoopFile(string path, string outputFormatClass, string keyClass, string valueClass, IEnumerable<KeyValuePair<string, string>> conf, string compressionCodecClass)
        {
            var jconf = SparkContextIpcProxy.GetJavaMap<string, string>(conf);
            SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "saveAsHadoopFile", new object[] { jvmRddReference, false, path, outputFormatClass, keyClass, valueClass, null, null, jconf, compressionCodecClass });
        }

        public void SaveAsSequenceFile(string path, string compressionCodecClass)
        {
            SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.python.PythonRDD", "SaveAsSequenceFile", new object[] { jvmRddReference, false, path, compressionCodecClass });
        }

        public void SaveAsTextFile(string path, string compressionCodecClass)
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            if (!string.IsNullOrEmpty(compressionCodecClass))
            {
                var codec = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("java.lang.Class", "forName", new object[] { compressionCodecClass }));
                SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "saveAsTextFile", new object[] { path, codec });
            }
            else
            {
                SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "saveAsTextFile", new object[] { path });
            }
        }
        public StorageLevel GetStorageLevel()
        {
            var rdd = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmRddReference, "rdd"));
            var storageLevel = new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(rdd, "getStorageLevel"));

            return new StorageLevel
            (
                (bool)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(storageLevel, "useDisk"),
                (bool)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(storageLevel, "useMemory"),
                (bool)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(storageLevel, "useOffHeap"),
                (bool)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(storageLevel, "deserialized"),
                (int)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(storageLevel, "replication")
            );
        }
        private JvmObjectReference GetJavaStorageLevel(StorageLevelType storageLevelType)
        {
            return new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.api.java.StorageLevels", "create",
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
}
