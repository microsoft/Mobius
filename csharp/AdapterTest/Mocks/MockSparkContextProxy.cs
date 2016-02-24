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
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Microsoft.Spark.CSharp.Interop.Ipc;
using NUnit.Framework;

namespace AdapterTest.Mocks
{
    internal class MockSparkContextProxy : ISparkContextProxy
    {
        private static IFormatter formatter = new BinaryFormatter();

        internal object mockSparkContextReference;

        public MockSparkContextProxy(ISparkConfProxy conf)
        {
            mockSparkContextReference = new object[] { conf };
        }

        public void AddFile(string filePath)
        { }

        public IRDDProxy TextFile(string filePath, int minPartitions)
        {
            return new MockRddProxy(new object[] { filePath, minPartitions });
        }

        public void Stop()
        {
            mockSparkContextReference = null;
        }

        public IRDDProxy CreateCSharpRdd(IRDDProxy prefvJavaRddReference, byte[] command, Dictionary<string, string> environmentVariables, List<string> cSharpIncludes, bool preservePartitioning, List<Broadcast> broadcastVariables, List<byte[]> accumulator)
        {
            IEnumerable<dynamic> input = (prefvJavaRddReference as MockRddProxy).result ??
                (new string[] {
                "The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy dog",
                "The quick brown fox jumps over the lazy dog",
                "The dog lazy"
            }).AsEnumerable().Cast<dynamic>();

            using (MemoryStream s = new MemoryStream(command))
            {
                int rddId = SerDe.ReadInt(s);
                int stageId = SerDe.ReadInt(s);
                int partitionId = SerDe.ReadInt(s);

                string deserializerMode = SerDe.ReadString(s);
                string serializerMode = SerDe.ReadString(s);
                CSharpWorkerFunc workerFunc = (CSharpWorkerFunc)formatter.Deserialize(new MemoryStream(SerDe.ReadBytes(s)));
                var func = workerFunc.Func;
                IEnumerable<dynamic> output = func(default(int), input);

                // number 8 indicates shuffling scenario's leading 8-byte hash code of each data row which should be filtered
                if (output.FirstOrDefault() is byte[] && (output.First() as byte[]).Length == 8)
                {
                    output = output.Where(e => (e as byte[]).Length != 8).Select(e => formatter.Deserialize(new MemoryStream(e as byte[])));
                }

                return new MockRddProxy(output);
            }
        }

        public IRDDProxy CreatePairwiseRDD(IRDDProxy javaReferenceInByteArrayRdd, int numPartitions)
        {
            return javaReferenceInByteArrayRdd;
        }


        public void SetLogLevel(string logLevel)
        { }

        public string Version
        {
            get { throw new NotImplementedException(); }
        }

        public long StartTime
        {
            get { throw new NotImplementedException(); }
        }

        public int DefaultParallelism
        {
            get { return 2; }
        }

        public int DefaultMinPartitions
        {
            get { return 1; }
        }

        public IRDDProxy EmptyRDD()
        {
            return new MockRddProxy(null);
        }

        public IRDDProxy WholeTextFiles(string filePath, int minPartitions)
        {
            return new MockRddProxy(null);
        }

        public IRDDProxy BinaryFiles(string filePath, int minPartitions)
        {
            return new MockRddProxy(null);
        }

        public IRDDProxy SequenceFile(string filePath, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, int minSplits, int batchSize)
        {
            return new MockRddProxy(null);
        }

        public IRDDProxy NewAPIHadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            return new MockRddProxy(null);
        }

        public IRDDProxy NewAPIHadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            return new MockRddProxy(null);
        }

        public IRDDProxy HadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            return new MockRddProxy(null);
        }

        public IRDDProxy HadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            return new MockRddProxy(null);
        }

        public IRDDProxy CheckpointFile(string filePath)
        {
            return new MockRddProxy(null);
        }

        public IRDDProxy Union(IEnumerable<IRDDProxy> rdds)
        {
            return new MockRddProxy(null);
        }

        public void SetCheckpointDir(string directory)
        { }

        public void SetJobGroup(string groupId, string description, bool interruptOnCancel)
        { }

        public void SetLocalProperty(string key, string value)
        { }

        public string GetLocalProperty(string key)
        {
            return null;
        }

        public string SparkUser
        {
            get { throw new NotImplementedException(); }
        }

        public void CancelJobGroup(string groupId)
        { }

        public void CancelAllJobs()
        { }

        public IUDFProxy CreateUserDefinedCSharpFunction(string name, byte[] command, string returnType)
        {
            throw new NotImplementedException();
        }

        internal static int RunJob(IRDDProxy rdd)
        {
            var mockRdd = (rdd as MockRddProxy);
            IEnumerable<byte[]> result = mockRdd.pickle ? mockRdd.result.Cast<byte[]>() :
                mockRdd.result.Select(x =>
                {
                    var ms = new MemoryStream();
                    formatter.Serialize(ms, x);
                    return ms.ToArray();
                });

            TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();

            Task.Run(() =>
            {
                using (Socket socket = listener.AcceptSocket())
                using (Stream ns = new NetworkStream(socket))
                {
                    foreach (var item in result)
                    {
                        SerDe.Write(ns, item.Length);
                        SerDe.Write(ns, item);
                    }
                }
            });
            return (listener.LocalEndpoint as IPEndPoint).Port;
        }

        public int RunJob(IRDDProxy rdd, IEnumerable<int> partitions)
        {
            return RunJob(rdd);
        }

        public IRDDProxy CreateCSharpRdd(IRDDProxy prefvJavaRddReference, byte[] command, Dictionary<string, string> environmentVariables, List<string> pythonIncludes, bool preservePartitioning, List<Broadcast<dynamic>> broadcastVariables, List<byte[]> accumulator)
        {
            throw new NotImplementedException();
        }

        public IStatusTrackerProxy StatusTracker
        {
            get { throw new NotImplementedException(); }
        }

        private int accumuatorServerPort = 0;

        public int AccumulatorServerPort
        {
            get
            {
                return accumuatorServerPort;
            }
        }

        public void Accumulator(int port)
        {
            accumuatorServerPort = port;
        }

        public IColumnProxy CreateColumnFromName(string name)
        {
            throw new NotImplementedException();
        }

        public IColumnProxy CreateFunction(string name, object self)
        {
            throw new NotImplementedException();
        }

        public IColumnProxy CreateBinaryMathFunction(string name, object self, object other)
        {
            throw new NotImplementedException();
        }

        public IColumnProxy CreateWindowFunction(string name)
        {
            throw new NotImplementedException();
        }

        IBroadcastProxy ISparkContextProxy.ReadBroadcastFromFile(string path, out long broadcastId)
        {
            throw new NotImplementedException();
        }

        public ISqlContextProxy CreateSqlContext()
        {
            return new MockSqlContextProxy(this);
        }

        public IRDDProxy Parallelize(IEnumerable<byte[]> values, int numSlices)
        {
            return new MockRddProxy(null);
        }
    }
}
