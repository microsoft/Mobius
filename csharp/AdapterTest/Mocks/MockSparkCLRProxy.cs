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
using Microsoft.Spark.CSharp.Sql;

namespace AdapterTest.Mocks
{
    internal class MockSparkCLRProxy : ISparkCLRProxy
    {
        private IFormatter formatter = new BinaryFormatter();

        public ISparkConfProxy CreateSparkConf(bool loadDefaults = true)
        {
            return new MockSparkConfProxy();
        }
        
        public ISparkContextProxy CreateSparkContext(ISparkConfProxy conf)
        {
            string master = null;
            string appName = null;
            string sparkHome =  null;
            
            if (conf != null)
            {
                MockSparkConfProxy proxy = conf as MockSparkConfProxy;
                if (proxy.stringConfDictionary.ContainsKey("mockmaster"))
                    master = proxy.stringConfDictionary["mockmaster"];
                if (proxy.stringConfDictionary.ContainsKey("mockappName"))
                    appName = proxy.stringConfDictionary["mockappName"];
                if (proxy.stringConfDictionary.ContainsKey("mockhome"))
                    sparkHome = proxy.stringConfDictionary["mockhome"];
            }

            return new MockSparkContextProxy(conf);
        }

        public ISparkContextProxy SparkContextProxy
        {
            get { throw new NotImplementedException(); }
        }

        public IDStreamProxy CreateCSharpDStream(IDStreamProxy jdstream, byte[] func, string deserializer)
        {
            Func<double, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>>)formatter.Deserialize(new MemoryStream(func));
            RDD<dynamic> rdd = f(DateTime.UtcNow.Ticks,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));
            return new MockDStreamProxy(rdd.RddProxy);
        }

        public IDStreamProxy CreateCSharpTransformed2DStream(IDStreamProxy jdstream, IDStreamProxy jother, byte[] func, string deserializer, string deserializerOther)
        {
            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)formatter.Deserialize(new MemoryStream(func));
            RDD<dynamic> rdd = f(DateTime.UtcNow.Ticks,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")),
                new RDD<dynamic>((jother as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));
            return new MockDStreamProxy(rdd.RddProxy);
        }

        public IDStreamProxy CreateCSharpReducedWindowedDStream(IDStreamProxy jdstream, byte[] func, byte[] invFunc, int windowSeconds, int slideSeconds, string deserializer)
        {
            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)formatter.Deserialize(new MemoryStream(func));
            RDD<dynamic> rdd = f(DateTime.UtcNow.Ticks,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")),
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));
            return new MockDStreamProxy(rdd.RddProxy);
        }


        public IDStreamProxy CreateCSharpStateDStream(IDStreamProxy jdstream, byte[] func, string deserializer, string deserializer2)
        {
            Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>> f = (Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)formatter.Deserialize(new MemoryStream(func));
            RDD<dynamic> rdd = f(DateTime.UtcNow.Ticks,
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")),
                new RDD<dynamic>((jdstream as MockDStreamProxy).rddProxy ?? new MockRddProxy(null), new SparkContext("", "")));
            return new MockDStreamProxy(rdd.RddProxy);
        }

        public bool CheckpointExists(string checkpointPath)
        {
            return false;
        }

        public IStreamingContextProxy CreateStreamingContext(SparkContext sparkContext, long durationMs)
        {
            return new MockStreamingContextProxy();
        }

        public IStreamingContextProxy CreateStreamingContext(string checkpointPath)
        {
            return new MockStreamingContextProxy();
        }
    }
}
