// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy;

namespace AdapterTest.Mocks
{
    internal class MockRddProxy : IRDDProxy
    {
        internal object[] mockRddReference;

        public MockRddProxy(object[] parameterCollection)
        {
            mockRddReference = parameterCollection;
        }

        public IRDDProxy Distinct<T>()
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Distinct<T>(int numPartitions)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Repartition<T>(int numPartitions)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Coalesce<T>(int numPartitions, bool shuffle)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Sample<T>(bool withReplacement, double fraction, long seed)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy[] RandomSplit<T>(double[] weights, long seed)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy RandomSampleWithRange<T>(double lb, double ub, long seed)
        {
            throw new NotImplementedException();
        }

        public long Count()
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Union(IRDDProxy javaRddReferenceOther)
        {
            return new MockRddProxy(new object[] { this, javaRddReferenceOther });
        }

        public int CollectAndServe()
        {
            throw new NotImplementedException();
        }

        public int PartitionLength()
        {
            throw new NotImplementedException();
        }


        public IRDDProxy Cache()
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Unpersist()
        {
            throw new NotImplementedException();
        }

        public void Checkpoint()
        {
            throw new NotImplementedException();
        }

        public bool IsCheckpointed
        {
            get { throw new NotImplementedException(); }
        }

        public string GetCheckpointFile()
        {
            throw new NotImplementedException();
        }

        public int GetNumPartitions()
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Distinct()
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Distinct(int numPartitions)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Sample(bool withReplacement, double fraction, long seed)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy[] RandomSplit(double[] weights, long seed)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Intersection(IRDDProxy[] other)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Cartesian(IRDDProxy other)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Pipe(string command)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Repartition(int numPartitions)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Coalesce(int numPartitions)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy RandomSampleWithRange(double lb, double ub, long seed)
        {
            throw new NotImplementedException();
        }

        public string Name
        {
            get { throw new NotImplementedException(); }
        }

        public void SetName(string name)
        {
            throw new NotImplementedException();
        }


        void IRDDProxy.Cache()
        {
            throw new NotImplementedException();
        }

        void IRDDProxy.Unpersist()
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Intersection(IRDDProxy other)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Coalesce(int numPartitions, bool shuffle)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy SampleByKey(bool withReplacement, Dictionary<string, double> fractions, long seed)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Zip(IRDDProxy other)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy ZipWithIndex()
        {
            throw new NotImplementedException();
        }

        public IRDDProxy ZipWithUniqueId()
        {
            throw new NotImplementedException();
        }

        public string ToDebugString()
        {
            throw new NotImplementedException();
        }

        public void SaveAsNewAPIHadoopDataset(IEnumerable<KeyValuePair<string, string>> conf)
        {
            throw new NotImplementedException();
        }

        public void SaveAsNewAPIHadoopFile(string path, string outputFormatClass, string keyClass, string valueClass, IEnumerable<KeyValuePair<string, string>> conf)
        {
            throw new NotImplementedException();
        }

        public void SaveAsHadoopDataset(IEnumerable<KeyValuePair<string, string>> conf)
        {
            throw new NotImplementedException();
        }

        public void SaveAsSequenceFile(string path, string compressionCodecClass)
        {
            throw new NotImplementedException();
        }

        public void SaveAsTextFile(string path, string compressionCodecClass)
        {
            throw new NotImplementedException();
        }


        public void saveAsHadoopFile(string path, string outputFormatClass, string keyClass, string valueClass, IEnumerable<KeyValuePair<string, string>> conf, string compressionCodecClass)
        {
            throw new NotImplementedException();
        }


        public void Persist(Microsoft.Spark.CSharp.Core.StorageLevelType storageLevelType)
        {
            throw new NotImplementedException();
        }

        public Microsoft.Spark.CSharp.Core.StorageLevel GetStorageLevel()
        {
            throw new NotImplementedException();
        }
    }
}
