// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Network;
using NUnit.Framework;
using Razorvine.Pickle;
using Tests.Common;

namespace WorkerTest
{
    /// <summary>
    /// Validates CSharpWorker by creating a ISocketWrapper server to 
    /// simulate interactions between CSharpRDD and CSharpWorker
    /// </summary>
    [TestFixture("Normal")]
    [TestFixture("Rio")]
    [TestFixture("Saea")]
    public class WorkerTest
    {
        private int splitIndex = 0;
        private string ver = "1.0";
        private string sparkFilesDir = "";
        private int numberOfIncludesItems = 0;
        private int numBroadcastVariables = 0;
        private readonly byte[] command = SparkContext.BuildCommand(new CSharpWorkerFunc((pid, iter) => iter), SerializedMode.String, SerializedMode.String);
        private readonly string socketWrapperType;

        public WorkerTest(string sockType)
        {
            if (sockType.Equals("Rio") && !SocketFactory.IsRioSockSupported())
            {
                Assert.Ignore("Omitting TestFixture due to missing Riosock.dll. It might caused by no VC++ build tool or running on an OS that not supports Windows RIO socket.");
            }

            // Set Socket wrapper for test
            socketWrapperType = sockType;
            Environment.SetEnvironmentVariable(ConfigurationService.CSharpSocketTypeEnvName, socketWrapperType);
            SocketFactory.SocketWrapperType = SocketWrapperType.None;
        }

        [OneTimeTearDown]
        public void CleanUpSocketWrapper()
        {
            if (socketWrapperType.Equals("Rio") && SocketFactory.IsRioSockSupported())
            {
                RioNative.UnloadRio();
            }
        }

        // StringBuilder is not thread-safe, it shouldn't be used concurrently from different threads.
        // http://stackoverflow.com/questions/12645351/stringbuilder-tostring-throw-an-index-out-of-range-exception
        StringBuilder output = new StringBuilder();
        private readonly object syncLock = new object();

        private ISocketWrapper CreateServer(out Process worker)
        {
            var tcpListener = SocketFactory.CreateSocket();
            tcpListener.Listen();
            int port = (tcpListener.LocalEndPoint as IPEndPoint).Port;

            var exeLocation = Path.GetDirectoryName(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath) ?? ".";

            worker = new Process
            {
                StartInfo =
                {
                    FileName = Path.Combine(exeLocation, "CSharpWorker.exe"),
                    Arguments = "-m pyspark.worker",
                    UseShellExecute = false,
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true
                }
            };

            lock (syncLock)
            {
                output.Clear();
            }

            worker.OutputDataReceived += new DataReceivedEventHandler((sender, e) =>
            {
                if (!String.IsNullOrEmpty(e.Data))
                {
                    Debug.WriteLine(e.Data);
                    lock (syncLock)
                    {
                        output.AppendLine(e.Data);
                    }
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
        private void AssertWorker(Process worker, int exitCode = 0, string assertMessage = null)
        {
            if (!worker.WaitForExit(3000))
            {
                worker.Kill();
            }
            Assert.IsTrue(worker.HasExited);
            Assert.AreEqual(exitCode, worker.ExitCode);
            string str;
            lock (syncLock)
            {
                str = output.ToString();
            }
            Assert.IsTrue(assertMessage == null || str.Contains(assertMessage));
        }

        /// <summary>
        /// test when no errors, server receives data as expected and worker exit with 0
        /// </summary>
        [Test]
        public void TestWorkerSuccess()
        {
            Process worker;
            var CSharpRDD_SocketServer = CreateServer(out worker);

            using (var serverSocket = CSharpRDD_SocketServer.Accept())
            using (var s = serverSocket.GetStream())
            {
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

            AssertWorker(worker);

            CSharpRDD_SocketServer.Close();
        }

        /// <summary>
        /// test when socket read incomplet and worker exit with 0
        /// </summary>
        [Test]
        public void TestWorkerReadIncomplete()
        {
            Process worker;
            var CSharpRDD_SocketServer = CreateServer(out worker);

            const int num = 10;
            byte[] takeCommand = SparkContext.BuildCommand(new CSharpWorkerFunc((pid, iter) => iter.Take(num)),
                SerializedMode.String, SerializedMode.String);

            using (var serverSocket = CSharpRDD_SocketServer.Accept())
            using (var s = serverSocket.GetStream())
            {
                WritePayloadHeaderToWorker(s);

                SerDe.Write(s, takeCommand.Length);
                SerDe.Write(s, takeCommand);

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

                Assert.AreEqual(num, count);
            }

            AssertWorker(worker, 0, "not all data is read");

            CSharpRDD_SocketServer.Close();
        }

        /// <summary>
        /// test when server transfers less bytes than expected, worker exits with -1 and expected error message
        /// </summary>
        [Test]
        public void TestWorkerIncompleteBytes()
        {
            Process worker;
            var CSharpRDD_SocketServer = CreateServer(out worker);

            using (var serverSocket = CSharpRDD_SocketServer.Accept())
            using (var s = serverSocket.GetStream())
            {
                WritePayloadHeaderToWorker(s);

                SerDe.Write(s, command.Length);
                s.Write(command, 0, command.Length / 2);
            }

            AssertWorker(worker, 0, "System.ArgumentException: Incomplete bytes read: ");

            CSharpRDD_SocketServer.Close();
        }

        /// <summary>
        /// test when missing END_OF_DATA_SECTION, worker exits with exit -1 and expected error message
        /// </summary>
        [Test]
        public void TestWorkerIncompleteData()
        {
            Process worker;
            var CSharpRDD_SocketServer = CreateServer(out worker);

            using (var serverSocket = CSharpRDD_SocketServer.Accept())
            using (var s = serverSocket.GetStream())
            {
                WritePayloadHeaderToWorker(s);

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

            AssertWorker(worker, 0, "System.NullReferenceException: Object reference not set to an instance of an object.");

            CSharpRDD_SocketServer.Close();
        }

        /// <summary>
        /// test when deserializedMode is set to Row, and serializedMode is set to bytes.
        /// </summary>
        [Test]
        public void TestWorkerWithRowDeserializedModeAndBytesSerializedMode()
        {
            Process worker;
            var CSharpRDD_SocketServer = CreateServer(out worker);

            const int expectedCount = 5;
            using (var serverSocket = CSharpRDD_SocketServer.Accept())
            using (var s = serverSocket.GetStream())
            {
                WritePayloadHeaderToWorker(s);
                byte[] commandWithRowDeserializeMode =
                    SparkContext.BuildCommand(new CSharpWorkerFunc((pid, iter) => iter), SerializedMode.Row);
                SerDe.Write(s, commandWithRowDeserializeMode.Length);
                SerDe.Write(s, commandWithRowDeserializeMode);

                new StructTypePickler().Register();
                new RowPickler().Register();
                Pickler pickler = new Pickler();

                for (int i = 0; i < expectedCount; i++)
                {
                    byte[] pickleBytes = pickler.dumps(new[] { RowHelper.BuildRowForBasicSchema(i) });
                    SerDe.Write(s, pickleBytes.Length);
                    SerDe.Write(s, pickleBytes);
                }

                SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                s.Flush();

                int count = 0;
                var formatter = new BinaryFormatter();
                foreach (var bytes in ReadWorker(s))
                {
                    var ms = new MemoryStream(bytes);
                    var rows = new ArrayList { formatter.Deserialize(ms) }.Cast<Row>().ToArray();
                    Assert.AreEqual(1, rows.Count());
                    Assert.AreEqual(count, rows[0].Get("age"));
                    count++;

                }

                Assert.AreEqual(expectedCount, count);
            }

            AssertWorker(worker);
            CSharpRDD_SocketServer.Close();
        }

        [Test]
        public void TestWorkerWithRawDeserializedModeAndBytesSerializedMode()
        {
            Process worker;
            var CSharpRDD_SocketServer = CreateServer(out worker);

            using (var serverSocket = CSharpRDD_SocketServer.Accept())
            using (var s = serverSocket.GetStream())
            {
                WritePayloadHeaderToWorker(s);
                byte[] commandWithRawDeserializeMode = SparkContext.BuildCommand(new CSharpWorkerFunc((pid, iter) => iter), SerializedMode.None, SerializedMode.None);
                SerDe.Write(s, commandWithRawDeserializeMode.Length);
                SerDe.Write(s, commandWithRawDeserializeMode);

                var payloadCollection = new string[] {"A", "B", "C", "D", "E"};
                foreach (var payloadElement in payloadCollection)
                {
                    var payload = Encoding.UTF8.GetBytes(payloadElement);
                    SerDe.Write(s, payload.Length);
                    SerDe.Write(s, payload);
                }

                SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                s.Flush();

                lock (syncLock)
                {
                    Console.WriteLine(output);
                }

                int receivedElementIndex = 0;
                foreach (var bytes in ReadWorker(s))
                {
                    var receivedPayload = SerDe.ToString(bytes);
                    Assert.AreEqual(payloadCollection[receivedElementIndex++], receivedPayload);
                }

                Assert.AreEqual(payloadCollection.Length, receivedElementIndex);

            }

            AssertWorker(worker);
            CSharpRDD_SocketServer.Close();
        }


        /// <summary>
        /// test when deserializedMode is set to Byte, and serializedMode is set to Row.
        /// </summary>
        [Test]
        public void TestWorkerWithBytesDeserializedModeAndRowSerializedMode()
        {
            const int expectedCount = 100;
            Process worker;
            var CSharpRDD_SocketServer = CreateServer(out worker);

            using (var serverSocket = CSharpRDD_SocketServer.Accept())
            using (var s = serverSocket.GetStream())
            {
                WritePayloadHeaderToWorker(s);
                byte[] command = SparkContext.BuildCommand(new CSharpWorkerFunc((pid, iter) => iter), SerializedMode.Byte, SerializedMode.Row);
                SerDe.Write(s, command.Length);
                SerDe.Write(s, command);

                var formatter = new BinaryFormatter();
                for (int i = 0; i < expectedCount; i++)
                {
                    var ms = new MemoryStream();
                    formatter.Serialize(ms, i);
                    var buffer = ms.ToArray();
                    SerDe.Write(s, buffer.Length);
                    SerDe.Write(s, ms.ToArray());
                }

                SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                s.Flush();

                lock (syncLock)
                {
                    Console.WriteLine(output);
                }

                int count = 0;
                Unpickler unpickler = new Unpickler();
                foreach (var bytes in ReadWorker(s))
                {
                    var objects = unpickler.loads(bytes) as ArrayList;
                    Assert.IsNotNull(objects);
                    Assert.IsTrue(objects.Count == 1);
                    Assert.AreEqual(count++, (int)objects[0]);
                }

                Assert.AreEqual(expectedCount, count);
            }

            AssertWorker(worker);
            CSharpRDD_SocketServer.Close();
        }

        /// <summary>
        /// test when deserializedMode is set to Pair, and serializedMode is set to None.
        /// </summary>
        [Test]
        public void TestWorkerWithPairDeserializedModeAndNoneSerializedMode()
        {
            const int expectedCount = 100;
            Process worker;
            var CSharpRDD_SocketServer = CreateServer(out worker);

            using (var serverSocket = CSharpRDD_SocketServer.Accept())
            using (var s = serverSocket.GetStream())
            {
                WritePayloadHeaderToWorker(s);
                byte[] command = SparkContext.BuildCommand(
                    new CSharpWorkerFunc((pid, iter) => iter.Cast<KeyValuePair<byte[], byte[]>>().Select(pair => pair.Key)),
                    SerializedMode.Pair, SerializedMode.None);

                SerDe.Write(s, command.Length);
                SerDe.Write(s, command);

                for (int i = 0; i < expectedCount; i++)
                {
                    SerDe.Write(s, i.ToString());
                    if (i % 2 == 0)
                    {
                        SerDe.Write(s, i.ToString());
                    }
                    else
                    {
                        // write null value
                        SerDe.Write(s, (int)SpecialLengths.NULL);
                    }
                }

                SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                s.Flush();

                lock (syncLock)
                {
                    Console.WriteLine(output);
                }

                int count = 0;
                foreach (var bytes in ReadWorker(s))
                {
                    Assert.IsNotNull(bytes);
                    Assert.AreEqual(count++, Convert.ToInt32(Encoding.UTF8.GetString(bytes)));
                }

                Assert.AreEqual(expectedCount, count);
            }

            AssertWorker(worker);
            CSharpRDD_SocketServer.Close();
        }

        /// <summary>
        /// test broadcast variables in worker. 
        /// </summary>
        [Test]
        public void TestBroadcastVariablesInWorker()
        {
            Process worker;
            var CSharpRDD_SocketServer = CreateServer(out worker);
            string assertMessage;

            using (var serverSocket = CSharpRDD_SocketServer.Accept())
            using (var s = serverSocket.GetStream())
            {
                SerDe.Write(s, splitIndex);
                SerDe.Write(s, ver);
                SerDe.Write(s, sparkFilesDir);
                SerDe.Write(s, numberOfIncludesItems);

                // broadcastVariablesToAdd and broadcastVariablesToDelete are used to trigger broadcast variables operation(register and remove) in worker side,
                // after worker exists, check wheather expected number of broadcast variables are processed.
                var broadcastVariablesToAdd = new long[] { 101L, 102L, 103L };
                var broadcastVariablesToDelete = new long[] { 10L, 20L };
                SerDe.Write(s, broadcastVariablesToAdd.Length + broadcastVariablesToDelete.Length);

                broadcastVariablesToAdd.ToList().ForEach(bid => { SerDe.Write(s, bid); SerDe.Write(s, "path" + bid); });
                broadcastVariablesToDelete.ToList().ForEach(bid => SerDe.Write(s, -bid - 1));

                byte[] command = SparkContext.BuildCommand(new CSharpWorkerFunc((pid, iter) => iter), SerializedMode.String, SerializedMode.String);

                SerDe.Write(s, command.Length);
                SerDe.Write(s, command);

                for (int i = 0; i < 100; i++)
                {
                    SerDe.Write(s, i.ToString());
                }

                SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                s.Flush();

                int count = 0;
                foreach (var bytes in ReadWorker(s))
                {
                    Assert.AreEqual(count++.ToString(), Encoding.UTF8.GetString(bytes));
                }

                Assert.AreEqual(100, count);

                // TODO verification should not depends on the output of worker
                // we postpone the test of assertMessage after worker exit
                assertMessage = "num_broadcast_variables: " + (broadcastVariablesToAdd.Length + broadcastVariablesToDelete.Length);
            }
           
            AssertWorker(worker, 0, assertMessage);
            CSharpRDD_SocketServer.Close();
        }

        /// <summary>
        /// read only data section from worker
        /// </summary>
        private IEnumerable<byte[]> ReadDataSection(Stream s, int expectedCount = 0)
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
                    break;
                }
            }
        }

        /// <summary>
        /// read accumulator
        /// </summary>
        private IEnumerable<KeyValuePair<int, dynamic>> ReadAccumulator(Stream s, int expectedCount = 0)
        {
            int count = 0;
            var formatter = new BinaryFormatter();
            while (true)
            {
                int length = SerDe.ReadInt(s);
                if (length > 0)
                {
                    var ms = new MemoryStream(SerDe.ReadBytes(s, length));
                    yield return (KeyValuePair<int, dynamic>)formatter.Deserialize(ms);

                    if (expectedCount > 0 && ++count >= expectedCount)
                    {
                        break;
                    }  
                }
                else if (length == (int)SpecialLengths.END_OF_STREAM)
                {
                    break;
                }
            }
        }

        /// <summary>
        /// test accumulator variables in worker. 
        /// </summary>
        [Test]
        public void TestAccumulatorInWorker()
        {
            Process worker;
            var CSharpRDD_SocketServer = CreateServer(out worker);

            using (var serverSocket = CSharpRDD_SocketServer.Accept())
            using (var s = serverSocket.GetStream())
            {
                WritePayloadHeaderToWorker(s);
                const int accumulatorId = 1001;
                var accumulator = new Accumulator<int>(accumulatorId, 0);
                byte[] command = SparkContext.BuildCommand(new CSharpWorkerFunc(new AccumulatorHelper(accumulator).Execute),
                    SerializedMode.String, SerializedMode.String);

                SerDe.Write(s, command.Length);
                SerDe.Write(s, command);

                const int expectedCount = 100;
                for (int i = 0; i < expectedCount; i++)
                {
                    SerDe.Write(s, i.ToString());
                }

                SerDe.Write(s, (int)SpecialLengths.END_OF_DATA_SECTION);
                SerDe.Write(s, (int)SpecialLengths.END_OF_STREAM);
                s.Flush();

                int count = 0;
                foreach (var bytes in ReadDataSection(s))
                {
                    Assert.AreEqual(count++.ToString(), Encoding.UTF8.GetString(bytes));
                }

                Assert.AreEqual(expectedCount, count);

                // read accumulator
                int accumulatorsCount = SerDe.ReadInt(s);
                Assert.IsTrue(accumulatorsCount == 1);
                var accumulatorFromWorker = ReadAccumulator(s, accumulatorsCount).First();
                Assert.AreEqual(accumulatorId, accumulatorFromWorker.Key);
                Assert.AreEqual(expectedCount, accumulatorFromWorker.Value);

                SerDe.ReadInt(s);
            }

            AssertWorker(worker);
            CSharpRDD_SocketServer.Close();
        }
    }

    [Serializable]
    internal class AccumulatorHelper
    {
        private Accumulator<int> accumulator;
        internal AccumulatorHelper(Accumulator<int> accumulator)
        {
            this.accumulator = accumulator;
        }

        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> iter)
        {
            return iter.Select(e =>
            {
                accumulator += 1;
                return e;
            });
        }
    }
}
