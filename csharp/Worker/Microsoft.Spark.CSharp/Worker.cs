// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;

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
        private const int END_OF_DATA_SECTION = -1;
        private const int DOTNET_EXCEPTION_THROWN = -2;
        private const int TIMING_DATA = -3;
        private const int END_OF_STREAM = -4;
        private const int NULL = -5;

        private static readonly DateTime UnixTimeEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        static void Main(string[] args)
        {
            PrintFiles();
            int javaPort = int.Parse(Console.ReadLine());
            Log("java_port: " + javaPort);
            var socket = new SparkCLRSocket();
            socket.Initialize(javaPort);

            using (socket)
            using (socket.InitializeStream())
            {
                try
                {
                    DateTime bootTime = DateTime.UtcNow;

                    int splitIndex = socket.ReadInt();
                    Log("split_index: " + splitIndex);
                    if (splitIndex == -1)
                        Environment.Exit(-1);

                    int versionLength = socket.ReadInt();
                    Log("ver_len: " + versionLength);

                    if (versionLength > 0)
                    {
                        string ver = socket.ReadString(versionLength);
                        Log("ver: " + ver);
                    }

                    //// initialize global state
                    //shuffle.MemoryBytesSpilled = 0
                    //shuffle.DiskBytesSpilled = 0
                    //_accumulatorRegistry.clear()

                    // fetch name of workdir
                    int sparkFilesDirectoryLength = socket.ReadInt();
                    Log("sparkFilesDirectoryLength: " + sparkFilesDirectoryLength);

                    if (sparkFilesDirectoryLength > 0)
                    {
                        string sparkFilesDir = socket.ReadString(sparkFilesDirectoryLength);
                        Log("spark_files_dir: " + sparkFilesDir);
                        //SparkFiles._root_directory = spark_files_dir
                        //SparkFiles._is_running_on_worker = True
                    }

                    // fetch names of includes - not used //TODO - complete the impl
                    int numberOfIncludesItems = socket.ReadInt();
                    Log("num_includes: " + numberOfIncludesItems);

                    if (numberOfIncludesItems > 0)
                    {
                        for (int i = 0; i < numberOfIncludesItems; i++)
                        {
                            string filename = socket.ReadString();
                        }
                    }

                    // fetch names and values of broadcast variables
                    int numBroadcastVariables = socket.ReadInt();
                    Log("num_broadcast_variables: " + numBroadcastVariables);

                    if (numBroadcastVariables > 0)
                    {
                        for (int i = 0; i < numBroadcastVariables; i++)
                        {
                            long bid = socket.ReadLong();
                            if (bid >= 0)
                            {
                                string path = socket.ReadString();
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

                    int lengthOCommandByteArray = socket.ReadInt();
                    Log("command_len: " + lengthOCommandByteArray);

                    IFormatter formatter = new BinaryFormatter();

                    if (lengthOCommandByteArray > 0)
                    {
                        int length = socket.ReadInt();
                        Log("Deserializer mode length: " + length);
                        string deserializerMode = socket.ReadString(length);
                        Log("Deserializer mode: " + deserializerMode);

                        length = socket.ReadInt();
                        Log("Serializer mode length: " + length);
                        string serializerMode = socket.ReadString(length);
                        Log("Serializer mode: " + serializerMode);

                        int lengthOfFunc = socket.ReadInt();
                        Log("Length of func: " + lengthOfFunc);
                        byte[] command = socket.ReadBytes(lengthOfFunc);

                        Log("command bytes read: " + command.Length);
                        var stream = new MemoryStream(command);

                        var func = (Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>)formatter.Deserialize(stream);

                        DateTime initTime = DateTime.UtcNow;

                        int count = 0;
                        foreach (var message in func(splitIndex, GetIterator(socket, deserializerMode)))
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
                                    Log(string.Format("{0} : {1}", message.GetType().Name, message.GetType().FullName));
                                    throw;
                                }
                            }

                            count++;
                            socket.Write(buffer.Length);
                            socket.Write(buffer);
                        }

                        //TODO - complete the impl
                        Log("Count: " + count);

                        //if profiler:
                        //    profiler.profile(process)
                        //else:
                        //    process()

                        DateTime finish_time = DateTime.UtcNow;

                        socket.Write(TIMING_DATA);
                        socket.Write(ToUnixTime(bootTime));
                        socket.Write(ToUnixTime(initTime));
                        socket.Write(ToUnixTime(finish_time));

                        socket.Write(0L); //shuffle.MemoryBytesSpilled  
                        socket.Write(0L); //shuffle.DiskBytesSpilled
                    }
                    else
                    {
                        Log("Nothing to execute :-(");
                    }

                    //// Mark the beginning of the accumulators section of the output
                    socket.Write(END_OF_DATA_SECTION);

                    socket.Write(Accumulator.accumulatorRegistry.Count);
                    foreach (var item in Accumulator.accumulatorRegistry)
                    {
                        var ms = new MemoryStream();
                        var value = item.Value.GetType().GetField("value", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(item.Value);
                        Log(string.Format("({0}, {1})", item.Key, value));
                        formatter.Serialize(ms, new KeyValuePair<int, dynamic>(item.Key, value));
                        byte[] buffer = ms.ToArray();
                        socket.Write(buffer.Length);
                        socket.Write(buffer);
                    }

                    int end = socket.ReadInt();

                    // check end of stream
                    if (end == END_OF_DATA_SECTION || end == END_OF_STREAM)
                    {
                        socket.Write(END_OF_STREAM);
                        Log("END_OF_STREAM: " + END_OF_STREAM);
                    }
                    else
                    {
                        // write a different value to tell JVM to not reuse this worker
                        socket.Write(END_OF_DATA_SECTION);
                        Environment.Exit(-1);
                    }
                    socket.Flush();
                    System.Threading.Thread.Sleep(1000); //TODO - not sure if this is really needed
                }
                catch (Exception e)
                {
                    Log(e.ToString());
                    try
                    {
                        socket.Write(e.ToString());
                    }
                    catch (IOException)
                    {
                        // JVM close the socket
                    }
                    catch (Exception ex)
                    {
                        LogError("CSharpWorker failed with exception:");
                        LogError(ex.ToString());
                    }
                    Environment.Exit(-1);
                }
            }
        }

        private static void PrintFiles()
        {
            Console.WriteLine("Files available in executor");
            var driverFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            var files = Directory.EnumerateFiles(driverFolder);
            foreach (var file in files)
            {
                Console.WriteLine(file);
            }
        }

        private static long ToUnixTime(DateTime dt)
        {
            return (long)(dt - UnixTimeEpoch).TotalMilliseconds;
        }

        private static IEnumerable<dynamic> GetIterator(ISparkCLRSocket socket, string serializedMode)
        {
            Log("Serialized mode in GetIterator: " + serializedMode);
            IFormatter formatter = new BinaryFormatter();
            int messageLength;
            do
            {
                messageLength = socket.ReadInt();

                if (messageLength > 0)
                {
                    byte[] buffer = socket.ReadBytes(messageLength);
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
                            messageLength = socket.ReadInt();
                            byte[] value = socket.ReadBytes(messageLength);
                            yield return new KeyValuePair<byte[], byte[]>(buffer, value);
                            break;

                        case "Byte":
                        default:
                            var ms = new MemoryStream(buffer);
                            dynamic message = formatter.Deserialize(ms);
                            yield return message;
                            break;
                    }
                }

            } while (messageLength >= 0);
        }

        private static void Log(string message)
        {
            Console.WriteLine(message);
        }

        private static void LogError(string message)
        {
            Console.WriteLine(message);
        }
    }
}
