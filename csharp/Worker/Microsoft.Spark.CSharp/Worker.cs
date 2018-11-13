// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Network;
using Microsoft.Spark.CSharp.Services;

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
        private static ILoggerService logger;
        private static SparkCLRAssemblyHandler assemblyHandler;

        public static void Main(string[] args)
        {
            assemblyHandler = new SparkCLRAssemblyHandler();
            AppDomain.CurrentDomain.AssemblyResolve += assemblyHandler.Handle;

            // can't initialize logger early because in MultiThreadWorker mode, JVM will read C#'s stdout via
            // pipe. When initialize logger, some unwanted info will be flushed to stdout. But we can still
            // use stderr
            Console.Error.WriteLine("CSharpWorker [{0}]: Input args [{1}] SocketWrapper [{2}]",
             Process.GetCurrentProcess().Id, string.Join(" ", args), SocketFactory.SocketWrapperType);

            if (args.Length != 2)
            {
                Console.Error.WriteLine("Wrong number of args: {0}, will exit", args.Length);
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
                //int javaPort = int.Parse(Console.ReadLine()); //reading port number written from JVM
	            var javaPort = int.Parse(Environment.GetEnvironmentVariable("PYTHON_WORKER_FACTORY_PORT"));
	            var secret = Environment.GetEnvironmentVariable("PYTHON_WORKER_FACTORY_SECRET");
				logger.LogDebug("Port and secret number used to pipe in/out data between JVM and CLR {0} {1}", javaPort, secret);
                var socket = InitializeSocket(javaPort);
	            //Microsoft.Spark.CSharp.Network.Utils.DoServerAuth(socket, secret);
				TaskRunner taskRunner = new TaskRunner(0, socket, false, secret);
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
            socket.Connect(IPAddress.Loopback, javaPort, null);
            return socket;
        }

        public static bool ProcessStream(Stream inputStream, Stream outputStream, int splitIndex)
        {
            logger.LogInfo("Start of stream processing, splitIndex: {0}", splitIndex);
            bool readComplete = true;   // Whether all input data from the socket is read though completely

            try
            {
                DateTime bootTime = DateTime.UtcNow;

                string ver = SerDe.ReadString(inputStream);
                logger.LogDebug("version: " + ver);

                //// initialize global state
                //shuffle.MemoryBytesSpilled = 0
                //shuffle.DiskBytesSpilled = 0
	            SerDe.ReadInt(inputStream);
				SerDe.ReadInt(inputStream);
				SerDe.ReadInt(inputStream);
				SerDe.ReadLong(inputStream);

				// fetch name of workdir
				string sparkFilesDir = SerDe.ReadString(inputStream);
                logger.LogDebug("spark_files_dir: " + sparkFilesDir);
                //SparkFiles._root_directory = sparkFilesDir
                //SparkFiles._is_running_on_worker = True

                ProcessIncludesItems(inputStream);

                ProcessBroadcastVariables(inputStream);

	            Accumulator.threadLocalAccumulatorRegistry = new Dictionary<int, Accumulator>();

                var formatter = ProcessCommand(inputStream, outputStream, splitIndex, bootTime);

                // Mark the beginning of the accumulators section of the output
                SerDe.Write(outputStream, (int)SpecialLengths.END_OF_DATA_SECTION);

                WriteAccumulatorValues(outputStream, formatter);

                int end = SerDe.ReadInt(inputStream);

                // check end of stream
                if (end == (int)SpecialLengths.END_OF_STREAM)
                {
                    SerDe.Write(outputStream, (int)SpecialLengths.END_OF_STREAM);
                    logger.LogDebug("END_OF_STREAM: " + (int)SpecialLengths.END_OF_STREAM);
                }
                else
                {
                    // This may happen when the input data is not read completely, e.g., when take() operation is performed
                    logger.LogWarn("**** unexpected read: {0}, not all data is read", end);
                    // write a different value to tell JVM to not reuse this worker
                    SerDe.Write(outputStream, (int)SpecialLengths.END_OF_DATA_SECTION);
                    readComplete = false;
                }

                outputStream.Flush();

                // log bytes read and write
                logger.LogDebug("total read bytes: {0}", SerDe.totalReadNum);
                logger.LogDebug("total write bytes: {0}", SerDe.totalWriteNum);

                logger.LogDebug("Stream processing completed successfully");
            }
            catch (Exception e)
            {
                logger.LogError("ProcessStream failed with exception:");
                logger.LogError(e.ToString());
                try
                {
                    logger.LogError("Trying to write error to stream");
                    SerDe.Write(outputStream, e.ToString());
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
                throw;
            }

            logger.LogInfo("Stop of stream processing, splitIndex: {0}, readComplete: {1}", splitIndex, readComplete);
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

        private static IFormatter ProcessCommand(Stream inputStream, Stream outputStream, int splitIndex, DateTime bootTime)
        {
            int isSqlUdf = SerDe.ReadInt(inputStream);
            logger.LogDebug("Is func Sql UDF = {0}", isSqlUdf);

            IFormatter formatter = new BinaryFormatter();
            UDFCommand command = null;

            if (isSqlUdf == 0)
            {
                command = ProcessNonUdfCommand(inputStream, outputStream, splitIndex, bootTime, formatter, isSqlUdf);
            }
            else
            {
                command = ProcessUdfCommand(inputStream, outputStream, splitIndex, bootTime, formatter, isSqlUdf);
            }

            if (command != null)
            {
                command.Execute();
            }

            return formatter;
        }

        private static UDFCommand ProcessNonUdfCommand(Stream inputStream, Stream outputStream, int splitIndex, 
            DateTime bootTime, IFormatter formatter, int isSqlUdf)
        {
            logger.LogDebug("Processing non-UDF command");
            int lengthOfCommandByteArray = SerDe.ReadInt(inputStream);
            logger.LogDebug("Command length: " + lengthOfCommandByteArray);

            UDFCommand command = null;
            if (lengthOfCommandByteArray > 0)
            {
                var commandProcessWatch = new Stopwatch();
                commandProcessWatch.Start();

                int stageId;
                string deserializerMode;
                string serializerMode;
                CSharpWorkerFunc cSharpWorkerFunc;
                ReadCommand(inputStream, formatter, out stageId, out deserializerMode, out serializerMode,
                    out cSharpWorkerFunc);

                command = new UDFCommand(inputStream, outputStream, splitIndex, bootTime, deserializerMode,
                    serializerMode, formatter, commandProcessWatch, isSqlUdf,
                    new List<WorkerFunc>() { new WorkerFunc(cSharpWorkerFunc, 0, null) }, stageId);

            }
            else
            {
                logger.LogWarn("lengthOfCommandByteArray = 0. Nothing to execute :-(");
            }

            return command;
        }

        private static UDFCommand ProcessUdfCommand(Stream inputStream, Stream outputStream, int splitIndex,
            DateTime bootTime, IFormatter formatter, int isSqlUdf)
        {
            logger.LogDebug("Processing UDF command");
            var udfCount = SerDe.ReadInt(inputStream);
            logger.LogDebug("Count of UDFs = {0}", udfCount);

            int stageId = -1;
            string deserializerMode = null;
            string serializerMode = null;
            var commandProcessWatch = new Stopwatch();
            List<WorkerFunc> workerFuncList = new List<WorkerFunc>();

            for(int udfIter = 0; udfIter < udfCount; udfIter++)
            { 
                CSharpWorkerFunc func = null;
                var argCount = SerDe.ReadInt(inputStream);
                logger.LogDebug("Count of args = {0}", argCount);

                List<int> argOffsets = new List<int>();
                for (int argIndex = 0; argIndex < argCount; argIndex++)
                {
                    var offset = SerDe.ReadInt(inputStream);
                    logger.LogDebug("UDF argIndex = {0}, Offset = {1}", argIndex, offset);
                    argOffsets.Add(offset);
                }

                var chainedFuncCount = SerDe.ReadInt(inputStream);
                logger.LogDebug("Count of chained func = {0}", chainedFuncCount);

                for (int funcIndex = 0; funcIndex < chainedFuncCount; funcIndex++)
                {
                    int lengthOfCommandByteArray = SerDe.ReadInt(inputStream);
                    logger.LogDebug("UDF command length: " + lengthOfCommandByteArray);

                    if (lengthOfCommandByteArray > 0)
                    {
                        CSharpWorkerFunc workerFunc;
                        ReadCommand(inputStream, formatter, out stageId, out deserializerMode, out serializerMode,
                            out workerFunc);

                        func = func == null ? workerFunc : CSharpWorkerFunc.Chain(func, workerFunc);
                    }
                    else
                    {
                        logger.LogWarn("UDF lengthOfCommandByteArray = 0. Nothing to execute :-(");
                    }
                }

                Debug.Assert(stageId != -1);
                Debug.Assert(deserializerMode != null);
                Debug.Assert(serializerMode != null);
                Debug.Assert(func != null);

                workerFuncList.Add(new WorkerFunc(func, argCount, argOffsets));
            }

            return new UDFCommand(inputStream, outputStream, splitIndex, bootTime, deserializerMode,
                    serializerMode, formatter, commandProcessWatch, isSqlUdf, workerFuncList, stageId);
        }

        private static void ReadCommand(Stream networkStream, IFormatter formatter, out int stageId,
            out string deserializerMode,
            out string serializerMode, out CSharpWorkerFunc workerFunc)
        {
            stageId = ReadDiagnosticsInfo(networkStream);

            deserializerMode = SerDe.ReadString(networkStream);
            logger.LogDebug("Deserializer mode: " + deserializerMode);
            serializerMode = SerDe.ReadString(networkStream);
            logger.LogDebug("Serializer mode: " + serializerMode);

            string runMode = SerDe.ReadString(networkStream);
            if ("R".Equals(runMode, StringComparison.InvariantCultureIgnoreCase))
            {
                var compilationDumpDir = SerDe.ReadString(networkStream);
                if (Directory.Exists(compilationDumpDir))
                {
                    assemblyHandler.LoadAssemblies(Directory.GetFiles(compilationDumpDir, "ReplCompilation.*",
                        SearchOption.TopDirectoryOnly));
                }
                else
                {
                    logger.LogError("Directory " + compilationDumpDir + " dose not exist.");
                }
            }


            byte[] command = SerDe.ReadBytes(networkStream);

            logger.LogDebug("command bytes read: " + command.Length);
            var stream = new MemoryStream(command);

            workerFunc = (CSharpWorkerFunc)formatter.Deserialize(stream);

            if (!logger.IsDebugEnabled) return;
            var sb = new StringBuilder(Environment.NewLine);
            sb.AppendLine(
                "------------------------ Printing stack trace of workerFunc for ** debugging ** ------------------------------");
            sb.AppendLine(workerFunc.StackTrace);
            sb.AppendLine(
                "--------------------------------------------------------------------------------------------------------------");
            logger.LogDebug(sb.ToString());
        }
                
        private static int ReadDiagnosticsInfo(Stream networkStream)
        {
            int rddId = SerDe.ReadInt(networkStream);
            int stageId = SerDe.ReadInt(networkStream);
            int partitionId = SerDe.ReadInt(networkStream);
            logger.LogInfo("rddInfo: rddId {0}, stageId {1}, partitionId {2}", rddId, stageId, partitionId);
            return stageId;
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
                logger.LogDebug("({0}, {1})", item.Key, value);
                formatter.Serialize(ms, new Tuple<int, dynamic>(item.Key, value));
                byte[] buffer = ms.ToArray();
                SerDe.Write(networkStream, buffer.Length);
                SerDe.Write(networkStream, buffer);
            }
        }

        public static void PrintFiles()
        {
            if (!logger.IsDebugEnabled) return;

            var folder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            var files = Directory.EnumerateFiles(folder).Select(Path.GetFileName).ToArray();
            var longest = files.Max(f => f.Length);
            var count = 0;
            var outfiles = new StringBuilder(Environment.NewLine);
            foreach (var file in files)
            {
                switch (count++ % 2)
                {
                    case 0:
                        outfiles.Append("   " + file.PadRight(longest + 2));
                        break;
                    default:
                        outfiles.AppendLine(file);
                        break;
                }
            }

            logger.LogDebug("Files available in executor");
            logger.LogDebug("Location: {0}{1}{2}", folder, Environment.NewLine, outfiles.ToString());
        }                

        internal class SparkCLRAssemblyHandler
        {
            private readonly ConcurrentDictionary<string, Assembly> assemblyDict = new ConcurrentDictionary<string, Assembly>();
            private readonly ConcurrentDictionary<string, bool> loadedFiles = new ConcurrentDictionary<string, bool>();

            public void LoadAssemblies(string[] files)
            {
                foreach (var assembly in from f in files.Where(f => new FileInfo(f).Length > 0).Select(Path.GetFullPath) where loadedFiles.TryAdd(f, true) select Assembly.Load(File.ReadAllBytes(f)))
                {
                    if (!assemblyDict.ContainsKey(assembly.FullName))
                    {
                        assemblyDict[assembly.FullName] = assembly;
                    }
                    else
                    {
                        Console.Error.WriteLine("Already loaded assembly " + assembly.FullName);
                    }
                }
            }

            public Assembly Handle(object source, ResolveEventArgs e)
            {
                if (assemblyDict.ContainsKey(e.Name))
                {
                    return assemblyDict[e.Name];
                }

                return null;
            }
        }
    }
}
