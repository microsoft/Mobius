// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Services;

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
                sock.Connect(IPAddress.Parse("127.0.0.1"), javaPort);
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

                    int lengthOCommandByteArray = SerDe.ReadInt(s);
                    logger.LogInfo("command_len: " + lengthOCommandByteArray);

                    IFormatter formatter = new BinaryFormatter();

                    if (lengthOCommandByteArray > 0)
                    {
                        string deserializerMode = SerDe.ReadString(s);
                        logger.LogInfo("Deserializer mode: " + deserializerMode);

                        string serializerMode = SerDe.ReadString(s);
                        logger.LogInfo("Serializer mode: " + serializerMode);

                        byte[] command = SerDe.ReadBytes(s);

                        logger.LogInfo("command bytes read: " + command.Length);
                        var stream = new MemoryStream(command);

                        var func = (Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>)formatter.Deserialize(stream);

                        DateTime initTime = DateTime.UtcNow;

                        int count = 0;
                        foreach (var message in func(splitIndex, GetIterator(s, deserializerMode)))
                        {
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
                        string format = "MM/dd/yyyy hh:mm:ss.fff tt";
                        logger.LogInfo(string.Format("bootTime: {0}, initTime: {1}, finish_time: {2}",
                            bootTime.ToString(format), initTime.ToString(format), finish_time.ToString(format)));
                        SerDe.Write(s, (int)SpecialLengths.TIMING_DATA);
                        SerDe.Write(s, ToUnixTime(bootTime));
                        SerDe.Write(s, ToUnixTime(initTime));
                        SerDe.Write(s, ToUnixTime(finish_time));

                        SerDe.Write(s, 0L); //shuffle.MemoryBytesSpilled  
                        SerDe.Write(s, 0L); //shuffle.DiskBytesSpilled
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
                    if (end == (int)SpecialLengths.END_OF_DATA_SECTION || end == (int)SpecialLengths.END_OF_STREAM)
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
                    
                    while (s.CanRead)
                        System.Threading.Thread.Sleep(1);
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

        private static IEnumerable<dynamic> GetIterator(Stream s, string serializedMode)
        {
            logger.LogInfo("Serialized mode in GetIterator: " + serializedMode);
            IFormatter formatter = new BinaryFormatter();
            int messageLength;
            while ((messageLength = SerDe.ReadInt(s)) != (int)SpecialLengths.END_OF_DATA_SECTION)
            {
                if (messageLength > 0 || serializedMode == "Pair")
                {
                    byte[] buffer = messageLength > 0 ? SerDe.ReadBytes(s, messageLength) : null;
                    switch (serializedMode)
                    {
                        case "String":
                            yield return SerDe.ToString(buffer);
                            break;

                        case "Row":
                            Unpickler unpickler = new Unpickler();
                            foreach (var item in (unpickler.loads(buffer) as object[]))
                            {
                                yield return item;
                            }
                            break;

                        case "Pair":
                            messageLength = SerDe.ReadInt(s);
                            if (messageLength > 0)
                            {
                                yield return new KeyValuePair<byte[], byte[]>(buffer, SerDe.ReadBytes(s, messageLength));
                            }
                            break;

                        case "Byte":
                        default:
                            var ms = new MemoryStream(buffer);
                            dynamic message = formatter.Deserialize(ms);
                            yield return message;
                            break;
                    }
                }
            }
        }
    }
}
