// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Network;

namespace Microsoft.Spark.CSharp.Proxy
{
    internal interface IRDDProxy
    {
        IRDDCollector RDDCollector { get; set; }
        StorageLevel GetStorageLevel();
        void Cache();
        void Persist(StorageLevelType storageLevelType);
        void Unpersist();
        void Checkpoint();
        bool IsCheckpointed { get; }
        string GetCheckpointFile();
        int GetNumPartitions();
        IRDDProxy Sample(bool withReplacement, double fraction, long seed);
        IRDDProxy[] RandomSplit(double[] weights, long seed);
        IRDDProxy Union(IRDDProxy other);
        IRDDProxy Cartesian(IRDDProxy other);
        IRDDProxy Pipe(string command);
        IRDDProxy Repartition(int numPartitions);
        IRDDProxy Coalesce(int numPartitions, bool shuffle);
        string Name { get; }
        void SetName(string name);
        IRDDProxy RandomSampleWithRange(double lb, double ub, long seed);
        IRDDProxy SampleByKey(bool withReplacement, IEnumerable<Tuple<string, double>> fractions, long seed);
        IRDDProxy Zip(IRDDProxy other);
        string ToDebugString();
        void SaveAsNewAPIHadoopDataset(IEnumerable<Tuple<string, string>> conf);
        void SaveAsNewAPIHadoopFile(string path, string outputFormatClass, string keyClass, string valueClass, IEnumerable<Tuple<string, string>> conf);
        void SaveAsHadoopDataset(IEnumerable<Tuple<string, string>> conf);
        void SaveAsHadoopFile(string path, string outputFormatClass, string keyClass, string valueClass, IEnumerable<Tuple<string, string>> conf, string compressionCodecClass);
        void SaveAsSequenceFile(string path, string compressionCodecClass);
        void SaveAsTextFile(string path, string compressionCodecClass);
        long Count();
        SocketInfo CollectAndServe();
    }
}
