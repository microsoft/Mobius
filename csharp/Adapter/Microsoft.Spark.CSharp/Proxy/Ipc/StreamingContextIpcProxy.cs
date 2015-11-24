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
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    /// <summary>
    /// calling Spark jvm side API in JavaStreamingContext.scala, StreamingContext.scala or external KafkaUtils.scala
    /// </summary>
    internal class StreamingContextIpcProxy : IStreamingContextProxy
    {
        private ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkConf));
        internal readonly JvmObjectReference jvmStreamingContextReference;
        private readonly JvmObjectReference jvmJavaStreamingReference;
        private readonly ISparkContextProxy sparkContextProxy;
        private readonly SparkContext sparkContext;

        public StreamingContextIpcProxy(SparkContext sparkContext, long durationMs)
        {
            this.sparkContext = sparkContext;
            sparkContextProxy = sparkContext.SparkContextProxy;
            var jduration = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.Duration", new object[] { durationMs });

            JvmObjectReference jvmSparkContextReference = (sparkContextProxy as SparkContextIpcProxy).JvmSparkContextReference;
            jvmStreamingContextReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.StreamingContext", new object[] { jvmSparkContextReference, jduration });
            jvmJavaStreamingReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.java.JavaStreamingContext", new object[] { jvmStreamingContextReference });

            int port = StartCallback();
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("SparkCLRHandler", "connectCallback", port); //className and methodName hardcoded in CSharpBackendHandler
        }

        public void Start()
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStreamingContextReference, "start");
        }

        public void Stop()
        {
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("SparkCLRHandler", "closeCallback");
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStreamingContextReference, "stop", new object[] { false });
        }

        public void Remember(long durationMs)
        {
            var jduration = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.Duration", new object[] { (int)durationMs });

            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStreamingContextReference, "remember", new object[] { jduration });
        }

        public void Checkpoint(string directory)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStreamingContextReference, "checkpoint", new object[] { directory });
        }

        public IDStreamProxy TextFileStream(string directory)
        {
            var jstream = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaStreamingReference, "textFileStream", new object[] { directory }).ToString());
            return new DStreamIpcProxy(jstream);
        }

        public IDStreamProxy SocketTextStream(string hostname, int port, StorageLevelType storageLevelType)
        {
            JvmObjectReference jlevel = SparkContextIpcProxy.GetJavaStorageLevel(storageLevelType);
            var jstream = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaStreamingReference, "socketTextStream", hostname, port, jlevel).ToString());
            return new DStreamIpcProxy(jstream);
        }

        public IDStreamProxy KafkaStream(Dictionary<string, int> topics, Dictionary<string, string> kafkaParams, StorageLevelType storageLevelType)
        {
            JvmObjectReference jtopics = SparkContextIpcProxy.GetJavaMap<string, int>(topics);
            JvmObjectReference jkafkaParams = SparkContextIpcProxy.GetJavaMap<string, string>(kafkaParams);
            JvmObjectReference jlevel = SparkContextIpcProxy.GetJavaStorageLevel(storageLevelType);
            JvmObjectReference jhelper = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper", new object[] { });
            var jstream = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jhelper, "createStream", new object[] { jvmJavaStreamingReference, jkafkaParams, jtopics, jlevel }).ToString());
            return new DStreamIpcProxy(jstream);
        }
        
        public IDStreamProxy DirectKafkaStream(List<string> topics, Dictionary<string, string> kafkaParams, Dictionary<string, long> fromOffsets)
        {
            JvmObjectReference jtopics = SparkContextIpcProxy.GetJavaSet<string>(topics);
            JvmObjectReference jkafkaParams = SparkContextIpcProxy.GetJavaMap<string, string>(kafkaParams);

            var jTopicAndPartitions = fromOffsets.Select(x =>
                new KeyValuePair<JvmObjectReference, long>
                (
                    SparkCLRIpcProxy.JvmBridge.CallConstructor("kafka.common.TopicAndPartition", new object[] { x.Key.Split(':')[0], int.Parse(x.Key.Split(':')[1]) }),
                    x.Value
                )
            );

            JvmObjectReference jfromOffsets = SparkContextIpcProxy.GetJavaMap<JvmObjectReference, long>(jTopicAndPartitions);
            JvmObjectReference jhelper = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper", new object[] { });
            var jstream = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jhelper, "createDirectStream", new object[] { jvmJavaStreamingReference, jkafkaParams, jtopics, jfromOffsets }).ToString());
            return new DStreamIpcProxy(jstream);
        }
        
        public IDStreamProxy Union(IDStreamProxy firstDStream, IDStreamProxy[] otherDStreams)
        {
            return new DStreamIpcProxy(
                new JvmObjectReference(
                    (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaStreamingReference, "union", 
                        new object[] 
                        { 
                            (firstDStream as DStreamIpcProxy).javaDStreamReference,
                            otherDStreams.Select(x => (x as DStreamIpcProxy).javaDStreamReference).ToArray()
                        }
                    )));
        }

        public void AwaitTermination()
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStreamingContextReference, "awaitTermination");
        }

        public void AwaitTermination(int timeout)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStreamingContextReference, "awaitTermination", new object[] { timeout });
        }

        public int StartCallback()
        {
            TcpListener callbackServer = new TcpListener(IPAddress.Parse("127.0.0.1"), 0);
            callbackServer.Start();

            Task.Run(() =>
            {
                try
                {
                    using (Socket sock = callbackServer.AcceptSocket())
                    using (var s = new NetworkStream(sock))
                    {
                        while (true)
                        {
                            try
                            {
                                string cmd = SerDe.ReadString(s);
                                if (cmd == "close")
                                {
                                    break;
                                }
                                else if (cmd == "callback")
                                {
                                    int numRDDs = SerDe.ReadInt(s);
                                    var jrdds = new List<JvmObjectReference>();
                                    for (int i = 0; i < numRDDs; i++)
                                    {
                                        jrdds.Add(new JvmObjectReference(SerDe.ReadObjectId(s)));
                                    }
                                    double time = SerDe.ReadDouble(s);

                                    IFormatter formatter = new BinaryFormatter();
                                    object func = formatter.Deserialize(new MemoryStream(SerDe.ReadBytes(s)));

                                    string deserializer = SerDe.ReadString(s);
                                    RDD<dynamic> rdd = null;
                                    if (jrdds[0].Id != null)
                                        rdd = new RDD<dynamic>(new RDDIpcProxy(jrdds[0]), sparkContext, (SerializedMode)Enum.Parse(typeof(SerializedMode), deserializer));

                                    if (func is Func<double, RDD<dynamic>, RDD<dynamic>>)
                                    {
                                        JvmObjectReference jrdd = (((Func<double, RDD<dynamic>, RDD<dynamic>>)func)(time, rdd).RddProxy as RDDIpcProxy).JvmRddReference;
                                        SerDe.Write(s, (byte)'j');
                                        SerDe.Write(s, jrdd.Id);
                                    }
                                    else if (func is Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)
                                    {
                                        string deserializer2 = SerDe.ReadString(s);
                                        RDD<dynamic> rdd2 = new RDD<dynamic>(new RDDIpcProxy(jrdds[1]), sparkContext, (SerializedMode)Enum.Parse(typeof(SerializedMode), deserializer2));
                                        JvmObjectReference jrdd = (((Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)func)(time, rdd, rdd2).RddProxy as RDDIpcProxy).JvmRddReference;
                                        SerDe.Write(s, (byte)'j');
                                        SerDe.Write(s, jrdd.Id);
                                    }
                                    else
                                    {
                                        ((Action<double, RDD<dynamic>>)func)(time, rdd);
                                        SerDe.Write(s, (byte)'n');
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                logger.LogInfo(e.ToString());
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    logger.LogInfo(e.ToString());
                    throw;
                }
                finally
                {
                    if (callbackServer != null)
                        callbackServer.Stop();
                }
            });

            return (callbackServer.LocalEndpoint as IPEndPoint).Port;
        }
    }
}
