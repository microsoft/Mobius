// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;

[assembly: InternalsVisibleTo("ReplTest")] 
namespace AdapterTest.Mocks
{
    internal class MockSparkCLRProxy : ISparkCLRProxy
    {
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

        private IStreamingContextProxy streamingContextProxy;
        public IStreamingContextProxy StreamingContextProxy
        {
            get { return streamingContextProxy; }
        }

        public bool CheckpointExists(string checkpointPath)
        {
            return false;
        }

        public IStreamingContextProxy CreateStreamingContext(SparkContext sparkContext, int durationSeconds)
        {
            streamingContextProxy = new MockStreamingContextProxy();
            return streamingContextProxy;
        }

        public IStreamingContextProxy CreateStreamingContext(string checkpointPath)
        {
            streamingContextProxy = new MockStreamingContextProxy();
            return streamingContextProxy;
        }
    }
}
