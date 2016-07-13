// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Network;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql;
using Razorvine.Pickle;

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

        private static ILoggerService logger = null;

        private static SparkCLRAssemblyHandler assemblyHandler = new SparkCLRAssemblyHandler();

        public static void Main(string[] args)
        {
            AppDomain.CurrentDomain.AssemblyResolve += assemblyHandler.Handler;

            // can't initialize logger early because in MultiThreadWorker mode, JVM will read C#'s stdout via
            // pipe. When initialize logger, some unwanted info will be flushed to stdout. But we can still
            // use stderr
            Console.Error.WriteLine("input args: [{0}] SocketWrapper: [{1}]",
                string.Join(" ", args), SocketFactory.SocketWrapperType);

            if (args.Length != 2)
            {
                Console.Error.WriteLine("Wrong number of args: {0}, will exit", args.Count());
                Environment.Exit(-1);
            }

            if ("pyspark.daemon".Equals(args[1]))
            {
                if (SocketFactory.SocketWrapperType == SocketWrapperType.Rio)
                {
                    // In daemon mode, the socket will be used as server.
                    // Use ThreadPool to retrieve RIO socket results has good performance
                    // than a single thread.
                    RioNative.SetUseThreadPool(true);
                }
                
                var multiThreadWorker = new MultiThreadWorker();
                multiThreadWorker.Run();
            }
            else
            {
                RunSimpleWorker();
            }
        }

        /// <summary>
        /// The C# worker process is used to execute only one JVM Task. It will exit after the task is finished.
        /// </summary>
        private static void RunSimpleWorker()
        {
            try
            {
                InitializeLogger();
                logger.LogInfo("RunSimpleWorker ...");
                PrintFiles();

                int javaPort = int.Parse(Console.ReadLine()); //reading port number written from JVM
                logger.LogDebug("Port number used to pipe in/out data between JVM and CLR {0}", javaPort);
                var socket = InitializeSocket(javaPort);
                TaskRunner taskRunner = new TaskRunner(0, socket, false);
                taskRunner.Run();
            }
            catch (Exception e)
            {
                logger.LogError("RunSimpleWorker failed with exception, will Exit");
                logger.LogException(e);
                Environment.Exit(-1);
            }

            logger.LogInfo("RunSimpleWorker finished successfully");
        }

        public static void InitializeLogger()
        {
            try
            {
                // if there exists exe.config file, then use log4net
                if (File.Exists(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile))
                {
                    LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance);
                }
                logger = LoggerServiceFactory.GetLogger(typeof(Worker));
            }
            catch (Exception e)
            {
                Console.WriteLine("InitializeLogger exception {0}, will exit", e);
                Environment.Exit(-1);
            }
        }

        private static ISocketWrapper InitializeSocket(int javaPort)
        {
            var socket = SocketFactory.CreateSocket();
            socket.Connect(IPAddress.Loopback, javaPort);
            return socket;
        }

        public static bool ProcessStream(Stream networkStream, int splitIndex)
        {
            logger.LogInfo(string.Format("Start of stream processing, splitIndex: {0}", splitIndex));
            bool readComplete = true;   // Whether all input data from the socket is read though completely

            try
            {
                DateTime bootTime = DateTime.UtcNow;

                string ver = SerDe.ReadString(networkStream);
                logger.LogDebug("version: " + ver);

                //// initialize global state
                //shuffle.MemoryBytesSpilled = 0
                //shuffle.DiskBytesSpilled = 0

                // fetch name of workdir
                string sparkFilesDir = SerDe.ReadString(networkStream);
                logger.LogDebug("spark_files_dir: " + sparkFilesDir);
                //SparkFiles._root_directory = sparkFilesDir
                //SparkFiles._is_running_on_worker = True

                ProcessIncludesItems(networkStream);

                ProcessBroadcastVariables(networkStream);

                Accumulator.threadLocalAccumulatorRegistry = new Dictionary<int, Accumulator>();

                var formatter = ProcessCommand(networkStream, splitIndex, bootTime);

                // Mark the beginning of the accumulators section of the output
                SerDe.Write(networkStream, (int)SpecialLengths.END_OF_DATA_SECTION);

                WriteAccumulatorValues(networkStream, formatter);

                int end = SerDe.ReadInt(networkStream);

                // check end of stream
                if (end == (int)SpecialLengths.END_OF_STREAM)
                {
                    SerDe.Write(networkStream, (int)SpecialLengths.END_OF_STREAM);
                    logger.LogDebug("END_OF_STREAM: " + (int)SpecialLengths.END_OF_STREAM);
                }
                else
                {
                    // This may happen when the input data is not read completely, e.g., when take() operation is performed
                    logger.LogWarn(string.Format("**** unexpected read: {0}, not all data is read", end));
                    // write a different value to tell JVM to not reuse this worker
                    SerDe.Write(networkStream, (int)SpecialLengths.END_OF_DATA_SECTION);
                    readComplete = false;
                }

                networkStream.Flush();

                // log bytes read and write
                logger.LogDebug(string.Format("total read bytes: {0}", SerDe.totalReadNum));
                logger.LogDebug(string.Format("total write bytes: {0}", SerDe.totalWriteNum));

                logger.LogDebug("Stream processing completed successfully");
            }
            catch (Exception e)
            {
                logger.LogError("ProcessStream failed with exception:");
                logger.LogError(e.ToString());
                try
                {
                    logger.LogError("Trying to write error to stream");
                    SerDe.Write(networkStream, e.ToString());
                }
                catch (IOException)
                {
                    // JVM close the socket
                }
                catch (Exception ex)
                {
                    logger.LogError("Writing exception to stream failed with exception:");
                    logger.LogException(ex);
                }
                throw e;
            }

            logger.LogInfo(string.Format("Stop of stream processing, splitIndex: {0}, readComplete: {1}", splitIndex, readComplete));
            return readComplete;
        }

        private static void ProcessIncludesItems(Stream networkStream)
        {
            // fetch names of includes - not used //TODO - complete the impl
            int numberOfIncludesItems = SerDe.ReadInt(networkStream);
            logger.LogDebug("num_includes: " + numberOfIncludesItems);

            if (numberOfIncludesItems > 0)
            {
                for (int i = 0; i < numberOfIncludesItems; i++)
                {
                    string filename = SerDe.ReadString(networkStream);
                }
            }
        }

        private static void ProcessBroadcastVariables(Stream networkStream)
        {
            // fetch names and values of broadcast variables
            int numBroadcastVariables = SerDe.ReadInt(networkStream);
            logger.LogDebug("num_broadcast_variables: " + numBroadcastVariables);

            if (numBroadcastVariables > 0)
            {
                for (int i = 0; i < numBroadcastVariables; i++)
                {
                    long bid = SerDe.ReadLong(networkStream);
                    if (bid >= 0)
                    {
                        string path = SerDe.ReadString(networkStream);
                        Broadcast.broadcastRegistry[bid] = new Broadcast(path);
                    }
                    else
                    {
                        bid = -bid - 1;
                        Broadcast bc;
                        Broadcast.broadcastRegistry.TryRemove(bid, out bc);
                    }
                }
            }
        }

        private static IFormatter ProcessCommand(Stream networkStream, int splitIndex, DateTime bootTime)
        {
            int lengthOfCommandByteArray = SerDe.ReadInt(networkStream);
            logger.LogDebug("command length: " + lengthOfCommandByteArray);

            IFormatter formatter = new BinaryFormatter();

            if (lengthOfCommandByteArray > 0)
            {
                var commandProcessWatch = new Stopwatch();
                var funcProcessWatch = new Stopwatch();
                commandProcessWatch.Start();

                int stageId = ReadDiagnosticsInfo(networkStream);

                string deserializerMode = SerDe.ReadString(networkStream);
                logger.LogDebug("Deserializer mode: " + deserializerMode);

                string serializerMode = SerDe.ReadString(networkStream);
                logger.LogDebug("Serializer mode: " + serializerMode);

                string runMode = SerDe.ReadString(networkStream);
                logger.LogInfo("Run mode: " + runMode);

                if (runMode.Equals("shell", StringComparison.InvariantCultureIgnoreCase))
                {
                    int assembliesCount = SerDe.ReadInt(networkStream);
                    logger.LogInfo("Total received assemblies count: " + assembliesCount);
                    var assemblies = new Assembly[assembliesCount];
                    for (var i = 0; i < assembliesCount; i++)
                    {
                        assemblies[i] = Assembly.Load(SerDe.ReadBytes(networkStream, SerDe.ReadInt(networkStream)));
                    }
                    assemblyHandler.SetAssemblies(assemblies);
                }

                byte[] command = SerDe.ReadBytes(networkStream);

                logger.LogDebug("command bytes read: " + command.Length);
                var stream = new MemoryStream(command);

                var workerFunc = (CSharpWorkerFunc)formatter.Deserialize(stream);
                var func = workerFunc.Func;
                logger.LogDebug(
                    "------------------------ Printing stack trace of workerFunc for ** debugging ** ------------------------------");
                logger.LogDebug(workerFunc.StackTrace);
                logger.LogDebug(
                    "--------------------------------------------------------------------------------------------------------------");
                DateTime initTime = DateTime.UtcNow;

                // here we use low level API because we need to get perf metrics
                var inputEnumerator = new WorkerInputEnumerator(networkStream, deserializerMode);
                IEnumerable<dynamic> inputEnumerable = inputEnumerator.Cast<dynamic>();

                funcProcessWatch.Start();
                IEnumerable<dynamic> outputEnumerable = func(splitIndex, inputEnumerable);
                var outputEnumerator = outputEnumerable.GetEnumerator();
                funcProcessWatch.Stop();

                int count = 0;
                int nullMessageCount = 0;
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
                        nullMessageCount++;
                        continue;
                    }

                    WriteOutput(networkStream, serializerMode, message, formatter);
                    count++;
                }

                logger.LogDebug("Output entries count: " + count);
                logger.LogDebug("Null messages count: " + nullMessageCount);

                //if profiler:
                //    profiler.profile(process)
                //else:
                //    process()

                WriteDiagnosticsInfo(networkStream, bootTime, initTime);

                commandProcessWatch.Stop();

                // log statistics
                inputEnumerator.LogStatistic();
                logger.LogInfo(string.Format("func process time: {0}", funcProcessWatch.ElapsedMilliseconds));
                logger.LogInfo(string.Format("stage {0}, command process time: {1}", stageId, commandProcessWatch.ElapsedMilliseconds));
            }
            else
            {
                logger.LogWarn("lengthOfCommandByteArray = 0. Nothing to execute :-(");
            }

            return formatter;
        }

        private static void WriteOutput(Stream networkStream, string serializerMode, dynamic message, IFormatter formatter)
        {
            var buffer = GetSerializedMessage(serializerMode, message, formatter);
            if (buffer == null)
            {
                logger.LogError("Buffer is null");
            }

            if (buffer.Length <= 0)
            {
                logger.LogError("Buffer length {0} cannot be <= 0", buffer.Length);
            }

            //Debug.Assert(buffer != null);
            //Debug.Assert(buffer.Length > 0);
            SerDe.Write(networkStream, buffer.Length);
            SerDe.Write(networkStream, buffer);
        }

        private static byte[] GetSerializedMessage(string serializerMode, dynamic message, IFormatter formatter)
        {
            byte[] buffer;

            switch ((SerializedMode)Enum.Parse(typeof(SerializedMode), serializerMode))
            {
                case SerializedMode.None:
                    buffer = message as byte[];
                    break;

                case SerializedMode.String:
                    buffer = SerDe.ToBytes(message as string);
                    break;

                case SerializedMode.Row:
                    var pickler = new Pickler();
                    buffer = pickler.dumps(new ArrayList { message });
                    break;

                default:
                    try
                    {
                        var ms = new MemoryStream();
                        formatter.Serialize(ms, message);
                        buffer = ms.ToArray();
                    }
                    catch (Exception)
                    {
                        logger.LogError("Exception serializing output");
                        logger.LogError("{0} : {1}", message.GetType().Name, message.GetType().FullName);
                        throw;
                    }
                    break;
            }

            return buffer;
        }


        private static int ReadDiagnosticsInfo(Stream networkStream)
        {
            int rddId = SerDe.ReadInt(networkStream);
            int stageId = SerDe.ReadInt(networkStream);
            int partitionId = SerDe.ReadInt(networkStream);
            logger.LogInfo(string.Format("rddInfo: rddId {0}, stageId {1}, partitionId {2}", rddId, stageId, partitionId));
            return stageId;
        }

        private static void WriteDiagnosticsInfo(Stream networkStream, DateTime bootTime, DateTime initTime)
        {
            DateTime finishTime = DateTime.UtcNow;
            const string format = "MM/dd/yyyy hh:mm:ss.fff tt";
            logger.LogDebug(string.Format("bootTime: {0}, initTime: {1}, finish_time: {2}",
                bootTime.ToString(format), initTime.ToString(format), finishTime.ToString(format)));
            SerDe.Write(networkStream, (int)SpecialLengths.TIMING_DATA);
            SerDe.Write(networkStream, ToUnixTime(bootTime));
            SerDe.Write(networkStream, ToUnixTime(initTime));
            SerDe.Write(networkStream, ToUnixTime(finishTime));

            SerDe.Write(networkStream, 0L); //shuffle.MemoryBytesSpilled  
            SerDe.Write(networkStream, 0L); //shuffle.DiskBytesSpilled
        }

        private static void WriteAccumulatorValues(Stream networkStream, IFormatter formatter)
        {
            SerDe.Write(networkStream, Accumulator.threadLocalAccumulatorRegistry.Count);
            foreach (var item in Accumulator.threadLocalAccumulatorRegistry)
            {
                var ms = new MemoryStream();
                var value =
                    item.Value.GetType()
                        .GetField("value", BindingFlags.NonPublic | BindingFlags.Instance)
                        .GetValue(item.Value);
                logger.LogDebug(string.Format("({0}, {1})", item.Key, value));
                formatter.Serialize(ms, new KeyValuePair<int, dynamic>(item.Key, value));
                byte[] buffer = ms.ToArray();
                SerDe.Write(networkStream, buffer.Length);
                SerDe.Write(networkStream, buffer);
            }
        }

        public static void PrintFiles()
        {
            logger.LogDebug("Files available in executor");
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
                    logger.LogDebug("END_OF_DATA_SECTION");
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
                    //unexpected behavior. 
                    //Try moving on to the next item. This might throw exception but
                    //it might also fetch the next items successfully
                    //So it is better not to throw exception right way
                    return MoveNext();
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
            switch ((SerializedMode)Enum.Parse(typeof(SerializedMode), deserializedMode))
            {
                case SerializedMode.String:
                    {
                        result = new object[1];
                        if (messageLength > 0)
                        {
                            byte[] buffer = SerDe.ReadBytes(inputStream, messageLength);
                            if (buffer == null)
                            {
                                logger.LogDebug("Buffer is null. Message length is {0}", messageLength);
                            }
                            result[0] = SerDe.ToString(buffer);
                        }
                        else
                        {
                            result[0] = null;
                        }
                        break;
                    }

                case SerializedMode.Row:
                    {
                        Debug.Assert(messageLength > 0);
                        byte[] buffer = SerDe.ReadBytes(inputStream, messageLength);
                        var unpickledObjects = PythonSerDe.GetUnpickledObjects(buffer);
                        var rows = unpickledObjects.Select(item => (item as RowConstructor).GetRow()).ToList();
                        result = rows.Cast<object>().ToArray();
                        break;
                    }

                case SerializedMode.Pair:
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

                case SerializedMode.None: //just read raw bytes
                    {
                        result = new object[1];
                        if (messageLength > 0)
                        {
                            result[0] = SerDe.ReadBytes(inputStream, messageLength);
                        }
                        else
                        {
                            result[0] = null;
                        }
                        break;
                    }

                case SerializedMode.Byte:
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

    internal class SparkCLRAssemblyHandler
    {

        private readonly ConcurrentDictionary<string, Assembly> assemblyDict = new ConcurrentDictionary<string, Assembly>();

        public void SetAssemblies(Assembly[] assemblies)
        {
            foreach (var assembly in assemblies)
            {
                assemblyDict[assembly.FullName] = assembly;
            }
        }

        public Assembly Handler(object source, ResolveEventArgs e)
        {
            if (assemblyDict.ContainsKey(e.Name))
            {
                return assemblyDict[e.Name];
            }

            return null;
        }
    }
}
