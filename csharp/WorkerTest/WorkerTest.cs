// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using NUnit.Framework;

namespace WorkerTest
{
    /// <summary>
    /// Validates CSharpWorker by creating a TcpListener server to 
    /// simulate interactions between CSharpRDD and CSharpWorker
    /// </summary>
    [TestFixture]
    public class WorkerTest
    {
        private int splitIndex = 0;
        private string ver = "1.0";
        private string sparkFilesDir = "";
        private int numberOfIncludesItems = 0;
        private int numBroadcastVariables = 0;
        private readonly byte[] command = SparkContext.BuildCommand(new CSharpWorkerFunc((pid, iter) => iter), SerializedMode.String, SerializedMode.String);

        private TcpListener CreateServer(StringBuilder output, out Process worker)
        {
            TcpListener tcpListener = new TcpListener(IPAddress.Loopback, 0);
            tcpListener.Start();
            int port = (tcpListener.LocalEndpoint as IPEndPoint).Port;

            string exeLocation = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ?? ".";

            worker = new Process
            {
                StartInfo =
                {
                    FileName = Path.Combine(exeLocation, "CSharpWorker.exe"),
                    UseShellExecute = false,
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true
                }
            };
            worker.OutputDataReceived += new DataReceivedEventHandler((sender, e) =>
            {
                if (!String.IsNullOrEmpty(e.Data))
                {
                    Debug.WriteLine(e.Data);
                    output.AppendLine(e.Data);
                }
            });
            Console.WriteLine("Starting worker process from {0}", worker.StartInfo.FileName);
            worker.Start();
            worker.BeginOutputReadLine();
            worker.StandardInput.WriteLine(port);
            
            return tcpListener;
        }

        /// <summary>
        /// write common header to worker
        /// </summary>
        /// <param name="s"></param>
        private void WriteWorker(Stream s)
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
        private void AssertWorker(Process worker, StringBuilder output, int exitCode = 0, string errorMessage = null)
        {
            worker.WaitForExit(3000);
            Assert.IsTrue(worker.HasExited);
            Assert.AreEqual(exitCode, worker.ExitCode);
            Assert.IsTrue(errorMessage == null || output.ToString().Contains(errorMessage));
        }

        /// <summary>
        /// test when no errors, server receives data as expected and worker exit with 0
        /// </summary>
        [Test]
        public void TestWorkerSuccess()
        {
            StringBuilder output = new StringBuilder();
            Process worker;
            TcpListener CSharpRDD_SocketServer = CreateServer(output, out worker);

            using (var serverSocket = CSharpRDD_SocketServer.AcceptSocket())
            using (var s = new NetworkStream(serverSocket))
            {
                WriteWorker(s);

                SerDe.Write(s, command.Length);
                SerDe.Write(s, command);

                for (int i = 0; i < 100; i++)
                    SerDe.Write(s, i.ToString());

                SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                s.Flush();

                int count = 0;
                foreach(var bytes in ReadWorker(s))
                {
                    Assert.AreEqual(count++.ToString(), Encoding.UTF8.GetString(bytes));
                }

                Assert.AreEqual(100, count);
            }

            AssertWorker(worker, output);

            CSharpRDD_SocketServer.Stop();
        }

        /// <summary>
        /// test when server transfers less bytes than expected, worker exits with -1 and expected error message
        /// </summary>
        [Test]
        public void TestWorkerIncompleteBytes()
        {
            StringBuilder output = new StringBuilder();
            Process worker;
            TcpListener CSharpRDD_SocketServer = CreateServer(output, out worker);

            using (var serverSocket = CSharpRDD_SocketServer.AcceptSocket())
            using (var s = new NetworkStream(serverSocket))
            {
                WriteWorker(s);

                SerDe.Write(s, command.Length);
                s.Write(command, 0, command.Length / 2);
            }

            AssertWorker(worker, output, -1, "System.ArgumentException: Incomplete bytes read: ");

            CSharpRDD_SocketServer.Stop();
        }

        /// <summary>
        /// test when missing END_OF_DATA_SECTION, worker exits with exit -1 and expected error message
        /// </summary>
        [Test]
        public void TestWorkerIncompleteData()
        {
            StringBuilder output = new StringBuilder();
            Process worker;
            TcpListener CSharpRDD_SocketServer = CreateServer(output, out worker);

            using (var serverSocket = CSharpRDD_SocketServer.AcceptSocket())
            using (var s = new NetworkStream(serverSocket))
            {
                WriteWorker(s);

                SerDe.Write(s, command.Length);
                s.Write(command, 0, command.Length);

                for (int i = 0; i < 100; i++)
                    SerDe.Write(s, i.ToString());

                int count = 0;
                foreach (var bytes in ReadWorker(s, 100))
                {
                    Assert.AreEqual(count++.ToString(), Encoding.UTF8.GetString(bytes));
                }

                Assert.AreEqual(100, count);
            }

            AssertWorker(worker, output, -1, "System.NullReferenceException: Object reference not set to an instance of an object.");

            CSharpRDD_SocketServer.Stop();
        }
    }
}
