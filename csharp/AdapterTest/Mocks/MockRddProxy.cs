// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Interop.Ipc;
using NUnit.Framework;

namespace AdapterTest.Mocks
{
    internal class MockRddProxy : IRDDProxy
    {
        internal IEnumerable<dynamic> result;
        internal bool pickle;

        internal object[] mockRddReference;

        public IRDDCollector RDDCollector
        {
            get { return new RDDCollector(); }

            set
            {
                throw new NotImplementedException();
            }
        }
        
        public MockRddProxy(object[] parameterCollection)
        {
            mockRddReference = parameterCollection;
        }

        public MockRddProxy(IEnumerable<dynamic> result, bool pickle = false)
        {
            this.result = result;
            this.pickle = pickle;
        }

        public long Count()
        {
            return result.Count();
        }

        public IRDDProxy Union(IRDDProxy javaRddReferenceOther)
        {
            var union = new MockRddProxy(new object[] { this, javaRddReferenceOther });
            if (result != null)
                union.result = result.Concat((javaRddReferenceOther as MockRddProxy).result);
            return union;
        }

        public int CollectAndServe()
        {
            return MockSparkContextProxy.RunJob(this);
        }

        public int PartitionLength()
        {
            return 1;
        }

        public void Cache()
        { }

        public void Unpersist()
        { }

        public void Checkpoint()
        {
            isCheckpointed = true; ;
        }

        private bool isCheckpointed;
        public bool IsCheckpointed
        {
            get { return isCheckpointed; }
        }

        public string GetCheckpointFile()
        {
            return null;
        }

        public int GetNumPartitions()
        {
            return 1;
        }

        public IRDDProxy Sample(bool withReplacement, double fraction, long seed)
        {
            return this;
        }

        public IRDDProxy[] RandomSplit(double[] weights, long seed)
        {
            return new IRDDProxy[] { this };
        }

        public IRDDProxy Cartesian(IRDDProxy other)
        {
            return this;
        }

        public IRDDProxy Pipe(string command)
        {
            return this;
        }

        public IRDDProxy Repartition(int numPartitions)
        {
            return this;
        }

        public IRDDProxy RandomSampleWithRange(double lb, double ub, long seed)
        {
            return this;
        }

        public string Name
        {
            get { return null; }
        }

        public void SetName(string name)
        {}

        public IRDDProxy Coalesce(int numPartitions, bool shuffle)
        {
            return this;
        }

        public IRDDProxy SampleByKey(bool withReplacement, Dictionary<string, double> fractions, long seed)
        {
            return this;
        }

        public IRDDProxy Zip(IRDDProxy other)
        {
            return this;
        }

        public string ToDebugString()
        {
            return null;
        }

        public void SaveAsNewAPIHadoopDataset(IEnumerable<KeyValuePair<string, string>> conf)
        { }

        public void SaveAsNewAPIHadoopFile(string path, string outputFormatClass, string keyClass, string valueClass, IEnumerable<KeyValuePair<string, string>> conf)
        { }

        public void SaveAsHadoopDataset(IEnumerable<KeyValuePair<string, string>> conf)
        { }

        public void SaveAsSequenceFile(string path, string compressionCodecClass)
        { }

        public void SaveAsTextFile(string path, string compressionCodecClass)
        { }


        public void SaveAsHadoopFile(string path, string outputFormatClass, string keyClass, string valueClass, IEnumerable<KeyValuePair<string, string>> conf, string compressionCodecClass)
        { }


        public void Persist(StorageLevelType storageLevelType)
        { }

        public StorageLevel GetStorageLevel()
        {
            return null;
        }
    }
}
