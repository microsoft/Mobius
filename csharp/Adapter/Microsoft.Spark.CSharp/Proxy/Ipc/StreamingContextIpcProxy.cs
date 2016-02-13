// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    /// <summary>
    /// calling Spark jvm side API in JavaStreamingContext.scala, StreamingContext.scala or external KafkaUtils.scala
    /// </summary>
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class StreamingContextIpcProxy : IStreamingContextProxy
    {
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(StreamingContextIpcProxy));
        internal readonly JvmObjectReference jvmStreamingContextReference;
        private readonly JvmObjectReference jvmJavaStreamingReference;
        private readonly ISparkContextProxy sparkContextProxy;
        private readonly SparkContext sparkContext;

        // flag to denote whether the callback socket is shutdown explicitly
        private volatile bool callbackSocketShutdown = false;

        public SparkContext SparkContext 
        { 
            get
            {
                return sparkContext;
            }
        }

        public StreamingContextIpcProxy(SparkContext sparkContext, long durationMs)
        {
            this.sparkContext = sparkContext;
            sparkContextProxy = sparkContext.SparkContextProxy;
            var jduration = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.Duration", new object[] { durationMs });

            JvmObjectReference jvmSparkContextReference = (sparkContextProxy as SparkContextIpcProxy).JvmSparkContextReference;
            jvmStreamingContextReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.StreamingContext", new object[] { jvmSparkContextReference, jduration });
            jvmJavaStreamingReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.java.JavaStreamingContext", new object[] { jvmStreamingContextReference });

            StartAccumulatorServer(sparkContext);
        }
        
        public StreamingContextIpcProxy(string checkpointPath)
        {
            jvmJavaStreamingReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.java.JavaStreamingContext", new object[] { checkpointPath });
            jvmStreamingContextReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaStreamingReference, "ssc"));
            JvmObjectReference jvmSparkContextReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStreamingContextReference, "sc"));
            JvmObjectReference jvmSparkConfReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStreamingContextReference, "conf"));
            JvmObjectReference jvmJavaContextReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaStreamingReference, "sparkContext"));
            sparkContextProxy = new SparkContextIpcProxy(jvmSparkContextReference, jvmJavaContextReference);
            var sparkConfProxy = new SparkConfIpcProxy(jvmSparkConfReference);
            sparkContext = new SparkContext(sparkContextProxy, new SparkConf(sparkConfProxy));
            StartAccumulatorServer(sparkContext);
        }

        private void StartAccumulatorServer(SparkContext sparkContext)
        {
            // TODO: We don't know whether accumulator variable is used before restart. We just start accumuator server for safety.
            sparkContext.StartAccumulatorServer();
        }

        public void Start()
        {
            int port = StartCallbackServer();
            logger.LogInfo("Callback server port number is {0}", port);
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("SparkCLRHandler", "connectCallback", port); //className and methodName hardcoded in CSharpBackendHandler
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStreamingContextReference, "start");
        }

        public void Stop()
        {
            // stop streamingContext first, then close the callback socket
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStreamingContextReference, "stop", new object[] { false });
            callbackSocketShutdown = true;
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("SparkCLRHandler", "closeCallback");
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

        public IDStreamProxy CreateCSharpDStream(IDStreamProxy jdstream, byte[] func, string serializationMode)
        {
            var jvmDStreamReference =
                SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.csharp.CSharpDStream",
                    new object[] {(jdstream as DStreamIpcProxy).jvmDStreamReference, func, serializationMode});

            var javaDStreamReference =
                new JvmObjectReference(
                    (string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDStreamReference, "asJavaDStream"));
            return new DStreamIpcProxy(javaDStreamReference, jvmDStreamReference);
        }

        public IDStreamProxy CreateCSharpTransformed2DStream(IDStreamProxy jdstream, IDStreamProxy jother, byte[] func, string serializationMode, string serializationModeOther)
        {
            var jvmDStreamReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.csharp.CSharpTransformed2DStream",
                new object[] { (jdstream as DStreamIpcProxy).jvmDStreamReference, (jother as DStreamIpcProxy).jvmDStreamReference, func, serializationMode, serializationModeOther });

            var javaDStreamReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDStreamReference, "asJavaDStream"));
            return new DStreamIpcProxy(javaDStreamReference, jvmDStreamReference);
        }

        public IDStreamProxy CreateCSharpReducedWindowedDStream(IDStreamProxy jdstream, byte[] func, byte[] invFunc, int windowSeconds, int slideSeconds, string serializationMode)
        {
            var windowDurationReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.Duration", new object[] { (long) windowSeconds * 1000 });
            var slideDurationReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.Duration", new object[] { (long) slideSeconds * 1000 });

            var jvmDStreamReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.csharp.CSharpReducedWindowedDStream",
                new object[] { (jdstream as DStreamIpcProxy).jvmDStreamReference, func, invFunc, windowDurationReference, slideDurationReference, serializationMode });

            var javaDStreamReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDStreamReference, "asJavaDStream"));
            return new DStreamIpcProxy(javaDStreamReference, jvmDStreamReference);
        }

        public IDStreamProxy CreateCSharpStateDStream(IDStreamProxy jdstream, byte[] func, string className, string serializationMode, string serializationMode2)
        {
            var jvmDStreamReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.api.csharp." + className,
                new object[] { (jdstream as DStreamIpcProxy).jvmDStreamReference, func, serializationMode, serializationMode2 });

            var javaDStreamReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDStreamReference, "asJavaDStream"));
            return new DStreamIpcProxy(javaDStreamReference, jvmDStreamReference);
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
            JvmObjectReference jtopics = JvmBridgeUtils.GetJavaMap<string, int>(topics);
            JvmObjectReference jkafkaParams = JvmBridgeUtils.GetJavaMap<string, string>(kafkaParams);
            JvmObjectReference jlevel = SparkContextIpcProxy.GetJavaStorageLevel(storageLevelType);
            // KafkaUtilsPythonHelper: external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaUtils.scala
            JvmObjectReference jhelper = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper", new object[] { });
            var jstream = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jhelper, "createStream", new object[] { jvmJavaStreamingReference, jkafkaParams, jtopics, jlevel }).ToString());
            return new DStreamIpcProxy(jstream);
        }
        
        public IDStreamProxy DirectKafkaStream(List<string> topics, Dictionary<string, string> kafkaParams, Dictionary<string, long> fromOffsets)
        {
            JvmObjectReference jtopics = JvmBridgeUtils.GetJavaSet<string>(topics);
            JvmObjectReference jkafkaParams = JvmBridgeUtils.GetJavaMap<string, string>(kafkaParams);

            var jTopicAndPartitions = fromOffsets.Select(x =>
                new KeyValuePair<JvmObjectReference, long>
                (
                    SparkCLRIpcProxy.JvmBridge.CallConstructor("kafka.common.TopicAndPartition", new object[] { x.Key.Split(':')[0], int.Parse(x.Key.Split(':')[1]) }),
                    x.Value
                )
            );

            JvmObjectReference jfromOffsets = JvmBridgeUtils.GetJavaMap<JvmObjectReference, long>(jTopicAndPartitions);
            // KafkaUtilsPythonHelper: external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaUtils.scala
            JvmObjectReference jhelper = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper", new object[] { });
            var jstream = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jhelper, "createDirectStreamWithoutMessageHandler", new object[] { jvmJavaStreamingReference, jkafkaParams, jtopics, jfromOffsets }).ToString());
            return new DStreamIpcProxy(jstream);
        }

        public IDStreamProxy EventHubsUnionStream(Dictionary<string, string> eventHubsParams, StorageLevelType storageLevelType)
        {
            JvmObjectReference eventHubsParamsReference = JvmBridgeUtils.GetScalaMutableMap<string, string>(eventHubsParams);
            JvmObjectReference storageLevelTypeReference = SparkContextIpcProxy.GetJavaStorageLevel(storageLevelType);
            return
                new DStreamIpcProxy(
                    new JvmObjectReference(
                        SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod(
                            "org.apache.spark.streaming.api.csharp.EventHubsUtils", "createUnionStream",
                            new object[] { jvmJavaStreamingReference, eventHubsParamsReference, storageLevelTypeReference })
                            .ToString()));
        }

        public IDStreamProxy Union(IDStreamProxy firstDStream, IDStreamProxy[] otherDStreams)
        {
            return new DStreamIpcProxy(
                new JvmObjectReference(
                    (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmJavaStreamingReference, "union", 
                        new object[] 
                        { 
                            (firstDStream as DStreamIpcProxy).javaDStreamReference,
                            JvmBridgeUtils.GetJavaList<JvmObjectReference>(otherDStreams.Select(x => (x as DStreamIpcProxy).javaDStreamReference))
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

        private void ProcessCallbackRequest(object socket)
        {
            logger.LogDebug("New thread (id={0}) created to process callback request", Thread.CurrentThread.ManagedThreadId);

            try
            {
                using (Socket sock = (Socket)socket)
                using (var s = new NetworkStream(sock))
                {
                    while (true)
                    {
                        try
                        {
                            string cmd = SerDe.ReadString(s);
                            if (cmd == "close")
                            {
                                logger.LogDebug("receive close cmd from Scala side");
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

                                string serializedMode = SerDe.ReadString(s);
                                RDD<dynamic> rdd = null;
                                if (jrdds[0].Id != null)
                                    rdd = new RDD<dynamic>(new RDDIpcProxy(jrdds[0]), sparkContext, (SerializedMode)Enum.Parse(typeof(SerializedMode), serializedMode));

                                if (func is Func<double, RDD<dynamic>, RDD<dynamic>>)
                                {
                                    JvmObjectReference jrdd = ((((Func<double, RDD<dynamic>, RDD<dynamic>>)func)(time, rdd)).RddProxy as RDDIpcProxy).JvmRddReference;
                                    SerDe.Write(s, (byte)'j');
                                    SerDe.Write(s, jrdd.Id);
                                }
                                else if (func is Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)
                                {
                                    string serializedMode2 = SerDe.ReadString(s);
                                    RDD<dynamic> rdd2 = new RDD<dynamic>(new RDDIpcProxy(jrdds[1]), sparkContext, (SerializedMode)Enum.Parse(typeof(SerializedMode), serializedMode2));
                                    JvmObjectReference jrdd = ((((Func<double, RDD<dynamic>, RDD<dynamic>, RDD<dynamic>>)func)(time, rdd, rdd2)).RddProxy as RDDIpcProxy).JvmRddReference;
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
                            //log exception only when callback socket is not shutdown explicitly
                            if (!callbackSocketShutdown)
                            {
                                logger.LogError("Exception processing call back request. Thread id {0}", Thread.CurrentThread.ManagedThreadId);
                                logger.LogException(e);

                                // exit when exception happens
                                logger.LogError("ProcessCallbackRequest fail, will exit ...");
                                Thread.Sleep(1000);
                                System.Environment.Exit(1);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.LogError("Exception in callback. Thread id {0}", Thread.CurrentThread.ManagedThreadId);
                logger.LogException(e);
            }

            logger.LogDebug("Thread (id={0}) to process callback request exiting", Thread.CurrentThread.ManagedThreadId);
        }

        private int StartCallbackServer()
        {
            TcpListener callbackServer = new TcpListener(IPAddress.Loopback, 0);
            callbackServer.Start();

            Task.Run(() =>
            {
                try
                {
                    ThreadPool.SetMaxThreads(10, 10);
                    while (!callbackSocketShutdown)
                    {
                        Socket sock = callbackServer.AcceptSocket();
                        ThreadPool.QueueUserWorkItem(ProcessCallbackRequest, sock);
                    }
                }
                catch (Exception e)
                {
                    logger.LogError("Exception starting callback server");
                    logger.LogException(e);
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
