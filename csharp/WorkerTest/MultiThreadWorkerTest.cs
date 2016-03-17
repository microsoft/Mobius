// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Specialized;
using System.Text;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Interop.Ipc;
using NUnit.Framework;

namespace WorkerTest
{
    /// <summary>
    /// Validates MultiThreadWorker by creating a TcpListener server to 
    /// simulate interactions between CSharpRDD and CSharpWorker
    /// </summary>
    [TestFixture]
    class MultiThreadWorkerTest
    {
        private int splitIndex = 0;
        private string ver = "1.0";
        private string sparkFilesDir = "";
        private int numberOfIncludesItems = 0;
        private int numBroadcastVariables = 0;
        private readonly byte[] command = SparkContext.BuildCommand(new CSharpWorkerFunc((pid, iter) => iter), SerializedMode.String, SerializedMode.String);

        // StringBuilder is not thread-safe, it shouldn't be used concurrently from different threads.
        // http://stackoverflow.com/questions/12645351/stringbuilder-tostring-throw-an-index-out-of-range-exception
        StringBuilder output = new StringBuilder();
        private readonly object syncLock = new object();

        private int CreateServer(out Process worker, bool sparkReuseWorker)
        {
            var exeLocation = Path.GetDirectoryName(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath) ?? ".";

            worker = new Process
            {
                StartInfo =
                {
                    FileName = Path.Combine(exeLocation, "CSharpWorker.exe"),
                    Arguments = "-m pyspark.daemon",
                    UseShellExecute = false,
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                }
            };
            if (sparkReuseWorker)
            {
                worker.StartInfo.EnvironmentVariables.Add("SPARK_REUSE_WORKER", "1");
            }

            lock (syncLock)
            {
                output.Clear();
            }

            Console.WriteLine("Starting worker process from {0}", worker.StartInfo.FileName);
            worker.Start();
            int serverPort = 0;
            serverPort = SerDe.ReadInt(worker.StandardOutput.BaseStream);

            StreamReader stdoutReader = worker.StandardOutput;
            Task.Run(() => {
                while (!stdoutReader.EndOfStream)
                {
                    string line = stdoutReader.ReadLine();
                    lock (syncLock)
                    {
                        output.Append(line);
                        output.Append("\n");
                    }
                }
            });

            StreamReader stderrReader = worker.StandardError;
            Task.Run(() =>
            {
                while (!stderrReader.EndOfStream)
                {
                    string line = stderrReader.ReadLine();
                    lock (syncLock)
                    {
                        output.Append(line);
                        output.Append("\n");
                    }
                }
            });

            return serverPort;
        }

        /// <summary>
        /// Create new socket to simulate interaction between JVM and C#
        /// </summary>
        /// <param name="s"></param>
        private Socket CreateSocket(int serverPort)
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(IPAddress.Loopback, serverPort);
            return socket;
        }

        /// <summary>
        /// write common header to worker
        /// </summary>
        /// <param name="s"></param>
        private void WritePayloadHeaderToWorker(Stream s)
        {
            SerDe.Write(s, splitIndex);
            SerDe.Write(s, ver);
            SerDe.Write(s, sparkFilesDir);
            SerDe.Write(s, numberOfIncludesItems);
            SerDe.Write(s, numBroadcastVariables);
            s.Flush();
        }

        /// <summary>
        /// read data from worker
        /// </summary>
        /// <param name="s"></param>
        /// <param name="expectedCount"></param>
        /// <returns></returns>
        private IEnumerable<byte[]> ReadWorker(Stream s, int expectedCount = 0)
        {
            int count = 0;
            while (true)
            {
                int length = SerDe.ReadInt(s);
                if (length > 0)
                {
                    yield return SerDe.ReadBytes(s, length);
                    if (expectedCount > 0 && ++count >= expectedCount)
                        break;
                }
                else if (length == (int)SpecialLengths.TIMING_DATA)
                {
                    var bootTime = SerDe.ReadLong(s);
                    var initTime = SerDe.ReadLong(s);
                    var finishTime = SerDe.ReadLong(s);
                    var memoryBytesSpilled = SerDe.ReadLong(s);
                    var diskBytesSpilled = SerDe.ReadLong(s);
                }
                else if (length == (int)SpecialLengths.DOTNET_EXCEPTION_THROWN)
                {
                    SerDe.ReadString(s);
                    break;
                }
                else if (length == (int)SpecialLengths.END_OF_DATA_SECTION)
                {
                    var numAccumulatorUpdates = SerDe.ReadInt(s);
                    SerDe.ReadInt(s);
                    break;
                }
            }
        }

        /// <summary>
        /// test worker has exited and with expected exit code
        /// </summary>
        /// <param name="exitCode"></param>
        private void AssertWorker(Process worker, int exitCode = 0, string errorMessage = null)
        {
            worker.WaitForExit(3000);
            string str;
            lock (syncLock)
            {
                str = output.ToString();
                Console.WriteLine("output from server: {0}", str);
            }
            Assert.IsTrue(worker.HasExited);
            Assert.AreEqual(exitCode, worker.ExitCode);
            Assert.IsTrue(errorMessage == null || str.Contains(errorMessage));
        }


        /// <summary>
        /// test when no errors, server receives data as expected and worker exit with 0
        /// </summary>
        [Test]
        public void TestMultiThreadWorkerSuccess()
        {
            Process worker;

            int serverPort = CreateServer(out worker, false);
            Console.WriteLine("serverPort: {0}", serverPort);

            using (var socket = CreateSocket(serverPort))
            using (var s = new NetworkStream(socket))
            {
                int taskRunnerId = SerDe.ReadInt(s);
                Console.WriteLine("taskRunnerId: {0}", taskRunnerId);

                WritePayloadHeaderToWorker(s);

                SerDe.Write(s, command.Length);
                SerDe.Write(s, command);

                for (int i = 0; i < 100; i++)
                    SerDe.Write(s, i.ToString());

                SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                s.Flush();

                int count = 0;
                foreach (var bytes in ReadWorker(s))
                {
                    Assert.AreEqual(count++.ToString(), Encoding.UTF8.GetString(bytes));
                }

                Assert.AreEqual(100, count);
            }

            using (Stream s = worker.StandardInput.BaseStream)
            {
                // use -1 to stop the server
                SerDe.Write(s, -1);
                s.Flush();
            }
            AssertWorker(worker);
        }

        /// <summary>
        /// test multiple TaskRunners scenario
        /// </summary>
        [Test]
        public void TestMultiThreadWorkerMultiTaskRunners()
        {
            Process worker;

            int serverPort = CreateServer(out worker, true);
            Console.WriteLine("serverPort: {0}", serverPort);

            int num = 2;
            var sockets = new Socket[2];
            var taskRunnerIds = new int[2];

            for (int index = 0; index < num; index++)
            {
                sockets[index] = CreateSocket(serverPort);
            }

            for (int index = 0; index < num; index++)
            {
                using (var s = new NetworkStream(sockets[index]))
                {
                    taskRunnerIds[index] = SerDe.ReadInt(s);

                    WritePayloadHeaderToWorker(s);

                    SerDe.Write(s, command.Length);
                    SerDe.Write(s, command);

                    for (int i = 0; i < 100; i++)
                        SerDe.Write(s, i.ToString());

                    SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                    SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                    s.Flush();

                    int count = 0;
                    foreach (var bytes in ReadWorker(s))
                    {
                        Assert.AreEqual(count++.ToString(), Encoding.UTF8.GetString(bytes));
                    }
                    Assert.AreEqual(100, count);
                }
            }

            using (Stream s = worker.StandardInput.BaseStream)
            {
                SerDe.Write(s, taskRunnerIds[0]);
                s.Flush();

                Thread.Sleep(200);
                sockets[0].Close();

                Thread.Sleep(200);
                SerDe.Write(s, taskRunnerIds[1]);
                s.Flush();

                Thread.Sleep(200);
                sockets[1].Close();

                SerDe.Write(s, (byte)1); // use -1 to stop the server
                s.Flush();
            }
            AssertWorker(worker, -1, "try to stop TaskRunner"); 
        }

    }
}
