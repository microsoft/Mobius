// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;


namespace Microsoft.Spark.CSharp.Proxy
{
    internal interface ISparkContextProxy
    {
        ISparkConfProxy GetConf();
        ISqlContextProxy CreateSqlContext();
        ISqlContextProxy CreateHiveContext();
        IColumnProxy CreateColumnFromName(string name);
        IColumnProxy CreateFunction(string name, object self);
        IColumnProxy CreateBinaryMathFunction(string name, object self, object other);
        IColumnProxy CreateWindowFunction(string name);
        void Accumulator(int port);
        void SetLogLevel(string logLevel);
        string Version { get; }
        long StartTime { get; }
        int DefaultParallelism { get; }
        int DefaultMinPartitions { get; }
        void Stop();
        IRDDProxy EmptyRDD();
        IRDDProxy Parallelize(IEnumerable<byte[]> values, int numSlices);
        IRDDProxy TextFile(string filePath, int minPartitions);
        IRDDProxy WholeTextFiles(string filePath, int minPartitions);
        IRDDProxy BinaryFiles(string filePath, int minPartitions);
        IRDDProxy SequenceFile(string filePath, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, int minSplits, int batchSize);
        IRDDProxy NewAPIHadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize);
        IRDDProxy NewAPIHadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize);
        IRDDProxy HadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize);
        IRDDProxy HadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize);
        IRDDProxy CheckpointFile(string filePath);
        IRDDProxy Union(IEnumerable<IRDDProxy> rdds);
        void AddFile(string path);
        void SetCheckpointDir(string directory);
        void SetJobGroup(string groupId, string description, bool interruptOnCancel);
        void SetLocalProperty(string key, string value);
        string GetLocalProperty(string key);
        string SparkUser { get; }
        void CancelJobGroup(string groupId);
        void CancelAllJobs();
        IStatusTrackerProxy StatusTracker { get; }
        int RunJob(IRDDProxy rdd, IEnumerable<int> partitions);
        IBroadcastProxy ReadBroadcastFromFile(string path, out long broadcastId);
        IRDDProxy CreateCSharpRdd(IRDDProxy prefvJavaRddReference, byte[] command, Dictionary<string, string> environmentVariables, List<string> pythonIncludes, bool preservePartitioning, List<Broadcast> broadcastVariables, List<byte[]> accumulator);
        IRDDProxy CreatePairwiseRDD(IRDDProxy javaReferenceInByteArrayRdd, int numPartitions, long partitionFuncId);
        IUDFProxy CreateUserDefinedCSharpFunction(string name, byte[] command, string returnType);
    }
    internal interface IBroadcastProxy
    {
        void Unpersist(bool blocking);
    }
}
