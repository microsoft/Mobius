// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace AdapterTest.Mocks
{
    internal class MockSparkContextProxy : ISparkContextProxy
    {
        private static IFormatter formatter = new BinaryFormatter();
        internal static IEnumerable<dynamic> result;

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
                string deserializerMode = SerDe.ReadString(s);
                string serializerMode = SerDe.ReadString(s);
                var func = (Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>)formatter.Deserialize(new MemoryStream(SerDe.ReadBytes(s)));
                result = func(default(int), input);
            }

            if (result.FirstOrDefault() is byte[] && (result.First() as byte[]).Length == 8)
            {
                result = result.Where(e => (e as byte[]).Length != 8).Select(e => formatter.Deserialize(new MemoryStream(e as byte[])));
            }

            return new MockRddProxy(result);
        }

        public IRDDProxy CreatePairwiseRDD(IRDDProxy javaReferenceInByteArrayRdd, int numPartitions)
        {
            return javaReferenceInByteArrayRdd;
        }


        public void SetLogLevel(string logLevel)
        {
            throw new NotImplementedException();
        }

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

        public IRDDProxy EmptyRDD<T>()
        {
            throw new NotImplementedException();
        }

        public IRDDProxy WholeTextFiles(string filePath, int minPartitions)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy BinaryFiles(string filePath, int minPartitions)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy SequenceFile(string filePath, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, int minSplits, int batchSize)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy NewAPIHadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy NewAPIHadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy HadoopFile(string filePath, string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy HadoopRDD(string inputFormatClass, string keyClass, string valueClass, string keyConverterClass, string valueConverterClass, IEnumerable<KeyValuePair<string, string>> conf, int batchSize)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy CheckpointFile(string filePath)
        {
            throw new NotImplementedException();
        }

        public IRDDProxy Union<T>(IEnumerable<RDD<T>> rdds)
        {
            throw new NotImplementedException();
        }

        public void SetCheckpointDir(string directory)
        {
            throw new NotImplementedException();
        }

        public void SetJobGroup(string groupId, string description, bool interruptOnCancel)
        {
            throw new NotImplementedException();
        }

        public void SetLocalProperty(string key, string value)
        {
            throw new NotImplementedException();
        }

        public string GetLocalProperty(string key)
        {
            throw new NotImplementedException();
        }

        public string SparkUser
        {
            get { throw new NotImplementedException(); }
        }

        public void CancelJobGroup(string groupId)
        {
            throw new NotImplementedException();
        }

        public void CancelAllJobs()
        {
            throw new NotImplementedException();
        }

        public IUDFProxy CreateUserDefinedCSharpFunction(string name, byte[] command, string returnType)
        {
            throw new NotImplementedException();
        }

        public object GetJavaMap<K, V>(IEnumerable<KeyValuePair<K, V>> enumerable)
        {
            throw new NotImplementedException();
        }

        public object GetJavaSet<T>(IEnumerable<T> enumerable)
        {
            throw new NotImplementedException();
        }

        public object GetJavaList<T>(IEnumerable<T> enumerable)
        {
            throw new NotImplementedException();
        }

        internal static int RunJob()
        {
            TcpListener listener = new TcpListener(IPAddress.Parse("127.0.0.1"), 0);
            listener.Start();

            Task.Run(() =>
            {
                using (Socket socket = listener.AcceptSocket())
                using (Stream ns = new NetworkStream(socket))
                {
                    foreach (var item in MockSparkContextProxy.result)
                    {
                        var ms = new MemoryStream();
                        formatter.Serialize(ms, item);
                        byte[] buffer = ms.ToArray();
                        SerDe.Write(ns, buffer.Length);
                        SerDe.Write(ns, buffer);
                    }
                }
            });
            return (listener.LocalEndpoint as IPEndPoint).Port;
        }

        public int RunJob(IRDDProxy rdd, IEnumerable<int> partitions, bool allowLocal)
        {
            return RunJob();
        }

        public IRDDProxy CreateCSharpRdd(IRDDProxy prefvJavaRddReference, byte[] command, Dictionary<string, string> environmentVariables, List<string> pythonIncludes, bool preservePartitioning, List<Broadcast<dynamic>> broadcastVariables, List<byte[]> accumulator)
        {
            throw new NotImplementedException();
        }

        public IStatusTrackerProxy StatusTracker
        {
            get { throw new NotImplementedException(); }
        }


        public void Accumulator(int port)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        public IStreamingContextProxy CreateStreamingContext(SparkContext sparkContext, long durationMs)
        {
            return new MockStreamingContextProxy();
        }
    }
}
