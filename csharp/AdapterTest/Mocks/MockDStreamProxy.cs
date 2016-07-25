// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;

namespace AdapterTest.Mocks
{
    internal class MockDStreamProxy : IDStreamProxy
    {
        internal IRDDProxy rddProxy;
        private IFormatter formatter = new BinaryFormatter();

        public int SlideDuration
        {
            get { return 1000 ;}
        }

        public MockDStreamProxy()
        { }

        public MockDStreamProxy(IRDDProxy rddProxy)
        {
            this.rddProxy = rddProxy;
        }

        public IDStreamProxy Window(int windowSeconds, int slideSeconds = 0)
        {
            return this;
        }

        public IDStreamProxy AsJavaDStream()
        {
            return this;
        }

        public void CallForeachRDD(byte[] func, string serializedMode)
        {
            Action<double, RDD<dynamic>> f = (Action<double, RDD<dynamic>>)formatter.Deserialize(new MemoryStream(func));
            f(DateTime.UtcNow.Ticks, new RDD<dynamic>(rddProxy, new SparkContext("", "")));
        }

        public void Print(int num = 10)
        {
        }

        public void Persist(Microsoft.Spark.CSharp.Core.StorageLevelType storageLevelType)
        {
        }

        public void Checkpoint(long intervalMs)
        {
        }

        public IRDDProxy[] Slice(long fromUnixTime, long toUnixTime)
        {
            return new IRDDProxy[] { rddProxy };
        }
    }
}
