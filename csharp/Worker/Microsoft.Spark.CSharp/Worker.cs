// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql;
using Razorvine.Pickle;
using Razorvine.Pickle.Objects;

namespace Microsoft.Spark.CSharp
{
    /// <summary>
    /// Worker implementation for SparkCLR. The implementation is identical to the 
    /// worker used in PySpark. The RDD implementation to fork an external process
    /// and pipe data in and out between JVM & the other runtime is already implemented in PySpark.
    /// SparkCLR uses the same design and implementation of PythonRDD (CSharpRDD extends PythonRDD).
    /// So the worker behavior is also the identical between PySpark and SparkCLR.
    /// </summary>
    public class Worker
    {
        private static readonly DateTime UnixTimeEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private static ILoggerService logger;

        static void Main(string[] args)
        {
            // if there exists exe.config file, then use log4net
            if (File.Exists(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile))
            {
                LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance);
            }
            logger = LoggerServiceFactory.GetLogger(typeof(Worker));

            Socket sock = null;
            try
            {
                PrintFiles();
                int javaPort = int.Parse(Console.ReadLine());
                logger.LogInfo("java_port: " + javaPort);
                sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                sock.Connect(IPAddress.Loopback, javaPort);
            }
            catch (Exception e)
            {
                logger.LogError("CSharpWorker failed with exception:");
                logger.LogException(e);
                Environment.Exit(-1);
            }

            using (NetworkStream s = new NetworkStream(sock))
            {
                try
                {
                    DateTime bootTime = DateTime.UtcNow;

                    int splitIndex = SerDe.ReadInt(s);
                    logger.LogInfo("split_index: " + splitIndex);
                    if (splitIndex == -1)
                        Environment.Exit(-1);

                    string ver = SerDe.ReadString(s);
                    logger.LogInfo("ver: " + ver);

                    //// initialize global state
                    //shuffle.MemoryBytesSpilled = 0
                    //shuffle.DiskBytesSpilled = 0

                    // fetch name of workdir
                    string sparkFilesDir = SerDe.ReadString(s);
                    logger.LogInfo("spark_files_dir: " + sparkFilesDir);
                    //SparkFiles._root_directory = sparkFilesDir
                    //SparkFiles._is_running_on_worker = True

                    // fetch names of includes - not used //TODO - complete the impl
                    int numberOfIncludesItems = SerDe.ReadInt(s);
                    logger.LogInfo("num_includes: " + numberOfIncludesItems);

                    if (numberOfIncludesItems > 0)
                    {
                        for (int i = 0; i < numberOfIncludesItems; i++)
                        {
                            string filename = SerDe.ReadString(s);
                        }
                    }

                    // fetch names and values of broadcast variables
                    int numBroadcastVariables = SerDe.ReadInt(s);
                    logger.LogInfo("num_broadcast_variables: " + numBroadcastVariables);

                    if (numBroadcastVariables > 0)
                    {
                        for (int i = 0; i < numBroadcastVariables; i++)
                        {
                            long bid = SerDe.ReadLong(s);
                            if (bid >= 0)
                            {
                                string path = SerDe.ReadString(s);
                                Broadcast.broadcastRegistry[bid] = new Broadcast(path);
                            }
                            else
                            {
                                bid = -bid - 1;
                                Broadcast.broadcastRegistry.Remove(bid);
                            }
                        }
                    }

                    Accumulator.accumulatorRegistry.Clear();

                    int lengthOfCommandByteArray = SerDe.ReadInt(s);
                    logger.LogInfo("command length: " + lengthOfCommandByteArray);

                    IFormatter formatter = new BinaryFormatter();

                    if (lengthOfCommandByteArray > 0)
                    {
                        Stopwatch commandProcessWatch = new Stopwatch();
                        Stopwatch funcProcessWatch = new Stopwatch();
                        commandProcessWatch.Start();

                        int rddId = SerDe.ReadInt(s);
                        int stageId = SerDe.ReadInt(s);
                        int partitionId = SerDe.ReadInt(s);
                        logger.LogInfo(string.Format("rddInfo: rddId {0}, stageId {1}, partitionId {2}", rddId, stageId, partitionId));

                        string deserializerMode = SerDe.ReadString(s);
                        logger.LogInfo("Deserializer mode: " + deserializerMode);

                        string serializerMode = SerDe.ReadString(s);
                        logger.LogInfo("Serializer mode: " + serializerMode);

                        byte[] command = SerDe.ReadBytes(s);

                        logger.LogInfo("command bytes read: " + command.Length);
                        var stream = new MemoryStream(command);

                        var workerFunc = (CSharpWorkerFunc)formatter.Deserialize(stream);
                        var func = workerFunc.Func;
                        logger.LogDebug(string.Format("stack trace of workerFunc (dont't panic, this is just for debug):\n{0}", workerFunc.StackTrace));
                        DateTime initTime = DateTime.UtcNow;
                        int count = 0;

                        // here we use low level API because we need to get perf metrics
                        WorkerInputEnumerator inputEnumerator = new WorkerInputEnumerator(s, deserializerMode);
                        IEnumerable<dynamic> inputEnumerable = Enumerable.Cast<dynamic>(inputEnumerator);
                        funcProcessWatch.Start();
                        IEnumerable<dynamic> outputEnumerable = func(splitIndex, inputEnumerable);
                        var outputEnumerator = outputEnumerable.GetEnumerator();
                        funcProcessWatch.Stop();
                        while (true)
                        {
                            funcProcessWatch.Start();
                            bool hasNext = outputEnumerator.MoveNext();
                            funcProcessWatch.Stop();
                            if (!hasNext)
                            {
                                break;
                            }

                            funcProcessWatch.Start();
                            var message = outputEnumerator.Current;
                            funcProcessWatch.Stop();

                            if (object.ReferenceEquals(null, message))
                            {
                                continue;
                            }

                            byte[] buffer;

                            if (serializerMode == "None")
                            {
                                buffer = message as byte[];
                            }
                            else if (serializerMode == "String")
                            {
                                buffer = SerDe.ToBytes(message as string);
                            }
                            else if (serializerMode == "Row")
                            {
                                Pickler pickler = new Pickler();
                                buffer = pickler.dumps(new ArrayList { message });
                            }
                            else
                            {
                                try
                                {
                                    var ms = new MemoryStream();
                                    formatter.Serialize(ms, message);
                                    buffer = ms.ToArray();
                                }
                                catch (Exception)
                                {
                                    logger.LogError(string.Format("{0} : {1}", message.GetType().Name, message.GetType().FullName));
                                    throw;
                                }
                            }

                            count++;
                            SerDe.Write(s, buffer.Length);
                            SerDe.Write(s, buffer);
                        }

                        //TODO - complete the impl
                        logger.LogInfo("Count: " + count);

                        //if profiler:
                        //    profiler.profile(process)
                        //else:
                        //    process()

                        DateTime finish_time = DateTime.UtcNow;
                        const string format = "MM/dd/yyyy hh:mm:ss.fff tt";
                        logger.LogInfo(string.Format("bootTime: {0}, initTime: {1}, finish_time: {2}",
                            bootTime.ToString(format), initTime.ToString(format), finish_time.ToString(format)));
                        SerDe.Write(s, (int)SpecialLengths.TIMING_DATA);
                        SerDe.Write(s, ToUnixTime(bootTime));
                        SerDe.Write(s, ToUnixTime(initTime));
                        SerDe.Write(s, ToUnixTime(finish_time));

                        SerDe.Write(s, 0L); //shuffle.MemoryBytesSpilled  
                        SerDe.Write(s, 0L); //shuffle.DiskBytesSpilled

                        commandProcessWatch.Stop();

                        // log statistics
                        inputEnumerator.LogStatistic();
                        logger.LogInfo(string.Format("func process time: {0}", funcProcessWatch.ElapsedMilliseconds));
                        logger.LogInfo(string.Format("command process time: {0}", commandProcessWatch.ElapsedMilliseconds));
                    }
                    else
                    {
                        logger.LogWarn("Nothing to execute :-(");
                    }

                    // Mark the beginning of the accumulators section of the output
                    SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);

                    SerDe.Write(s, Accumulator.accumulatorRegistry.Count);
                    foreach (var item in Accumulator.accumulatorRegistry)
                    {
                        var ms = new MemoryStream();
                        var value = item.Value.GetType().GetField("value", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(item.Value);
                        logger.LogInfo(string.Format("({0}, {1})", item.Key, value));
                        formatter.Serialize(ms, new KeyValuePair<int, dynamic>(item.Key, value));
                        byte[] buffer = ms.ToArray();
                        SerDe.Write(s, buffer.Length);
                        SerDe.Write(s, buffer);
                    }

                    int end = SerDe.ReadInt(s);

                    // check end of stream
                    if (end == (int)SpecialLengths.END_OF_STREAM)
                    {
                        SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                        logger.LogInfo("END_OF_STREAM: " + (int)SpecialLengths.END_OF_STREAM);
                    }
                    else
                    {
                        // write a different value to tell JVM to not reuse this worker
                        SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                        Environment.Exit(-1);
                    }
                    s.Flush();

                    // log bytes read and write
                    logger.LogInfo(string.Format("total read bytes: {0}", SerDe.totalReadNum));
                    logger.LogInfo(string.Format("total write bytes: {0}", SerDe.totalWriteNum));

                    // wait for server to complete, otherwise server gets 'connection reset' exception
                    // Use SerDe.ReadBytes() to detect java side has closed socket properly
                    // ReadBytes() will block until the socket is closed
                    SerDe.ReadBytes(s);
                }
                catch (Exception e)
                {
                    logger.LogError(e.ToString());
                    try
                    {
                        SerDe.Write(s, e.ToString());
                    }
                    catch (IOException)
                    {
                        // JVM close the socket
                    }
                    catch (Exception ex)
                    {
                        logger.LogError("CSharpWorker failed with exception:");
                        logger.LogException(ex);
                    }
                    Environment.Exit(-1);
                }
            }

            sock.Close();
        }

        private static void PrintFiles()
        {
            logger.LogInfo("Files available in executor");
            var driverFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            var files = Directory.EnumerateFiles(driverFolder);
            foreach (var file in files)
            {
                logger.LogDebug(file);
            }
        }

        private static long ToUnixTime(DateTime dt)
        {
            return (long)(dt - UnixTimeEpoch).TotalMilliseconds;
        }
    }

    // Get worker input data from input stream
    internal class WorkerInputEnumerator : IEnumerator, IEnumerable
    {
        private static readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(WorkerInputEnumerator));

        private readonly Stream inputStream;
        private readonly string deserializedMode;

        // cache deserialized object read from input stream
        private object[] items = null;
        private int pos = 0;

        private readonly IFormatter formatter = new BinaryFormatter();
        private readonly Stopwatch watch = new Stopwatch();

        public WorkerInputEnumerator(Stream inputStream, string deserializedMode)
        {
            this.inputStream = inputStream;
            this.deserializedMode = deserializedMode;
        }

        public bool MoveNext()
        {
            watch.Start();
            bool hasNext;

            if ((items != null) && (pos < items.Length))
            {
                hasNext = true;
            }
            else
            {
                int messageLength = SerDe.ReadInt(inputStream);
                if (messageLength == (int)SpecialLengths.END_OF_DATA_SECTION)
                {
                    hasNext = false;
                    logger.LogInfo("END_OF_DATA_SECTION");
                }
                else if ((messageLength > 0) || (messageLength == (int)SpecialLengths.NULL))
                {
                    items = GetNext(messageLength);
                    Debug.Assert(items != null);
                    Debug.Assert(items.Any());
                    pos = 0;
                    hasNext = true;
                }
                else
                {
                    throw new Exception(string.Format("unexpected messageLength: {0}", messageLength));
                }
            }

            watch.Stop();
            return hasNext;
        }

        public object Current
        {
            get
            {
                int currPos = pos;
                pos++;
                return items[currPos];
            }
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public IEnumerator GetEnumerator()
        {
            return this;
        }

        public void LogStatistic()
        {
            logger.LogInfo(string.Format("total elapsed time: {0}", watch.ElapsedMilliseconds));
        }

        private object[] GetNext(int messageLength)
        {
            object[] result = null;
            switch (deserializedMode)
            {
                case "String":
                    {
                        result = new object[1];
                        if (messageLength > 0)
                        {
                            byte[] buffer = SerDe.ReadBytes(inputStream, messageLength);
                            result[0] = SerDe.ToString(buffer);
                        }
                        else
                        {
                            result[0] = null;
                        }
                        break;
                    }

                case "Row":
                    {
                        Debug.Assert(messageLength > 0);
                        byte[] buffer = SerDe.ReadBytes(inputStream, messageLength);
                        var unpickledObjects = PythonSerDe.GetUnpickledObjects(buffer);
                        var rows = unpickledObjects.Select(item => (item as RowConstructor).GetRow()).ToList();
                        result = rows.Cast<object>().ToArray();
                        break;
                    }

                case "Pair":
                    {
                        byte[] pairKey = (messageLength > 0) ? SerDe.ReadBytes(inputStream, messageLength) : null;
                        byte[] pairValue = null;

                        int valueLength = SerDe.ReadInt(inputStream);
                        if (valueLength > 0)
                        {
                            pairValue = SerDe.ReadBytes(inputStream, valueLength);
                        }
                        else if (valueLength == (int)SpecialLengths.NULL)
                        {
                            pairValue = null;
                        }
                        else
                        {
                            throw new Exception(string.Format("unexpected valueLength: {0}", valueLength));
                        }

                        result = new object[1];
                        result[0] = new KeyValuePair<byte[], byte[]>(pairKey, pairValue);
                        break;
                    }

                case "Byte":
                default:
                    {
                        result = new object[1];
                        if (messageLength > 0)
                        {
                            byte[] buffer = SerDe.ReadBytes(inputStream, messageLength);
                            var ms = new MemoryStream(buffer);
                            result[0] = formatter.Deserialize(ms);
                        }
                        else
                        {
                            result[0] = null;
                        }

                        break;
                    }
            }

            return result;
        }
    }
}
