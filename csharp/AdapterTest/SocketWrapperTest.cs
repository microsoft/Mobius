// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Network;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validates SaeaSocketWrapper by creating a ISocketWrapper server to 
    /// simulate interactions between CSharpRDD and CSharpWorker
    /// </summary>
    [TestFixture]
    public class SocketWrapperTest
    {
        private void SocketTest(ISocketWrapper serverSocket)
        {
            serverSocket.Listen();
            if (serverSocket is RioSocketWrapper)
            {
                // Do nothing for second listen operation.
                Assert.DoesNotThrow(() => serverSocket.Listen(int.MaxValue));
            }

            var port = ((IPEndPoint)serverSocket.LocalEndPoint).Port;

            var clientMsg = "Hello Message from client";
            var clientMsgBytes = Encoding.UTF8.GetBytes(clientMsg);

            Task.Run(() =>
            {
                var bytes = new byte[1024];
                using (var socket = serverSocket.Accept())
                {
                    using (var s = socket.GetStream())
                    {
                        // Receive data
                        var bytesRec = s.Read(bytes, 0, bytes.Length);
                        // send echo message.
                        s.Write(bytes, 0, bytesRec);
                        s.Flush();

                        // Receive one byte
                        var oneByte = s.ReadByte();

                        // Send echo one byte
                        byte[] oneBytes = { (byte)oneByte };
                        s.Write(oneBytes, 0, oneBytes.Length);
                        
                        Thread.SpinWait(0);

                        // Keep sending to ensure no memory leak
                        var longBytes = Encoding.UTF8.GetBytes(new string('x', 8192));
                        for (int i = 0; i < 1000; i++)
                        {
                            s.Write(longBytes, 0, longBytes.Length);
                        }
                        byte[] msg = Encoding.ASCII.GetBytes("This is a test<EOF>");
                        s.Write(msg, 0, msg.Length);

                        // Receive echo byte.
                        s.ReadByte();
                    }
                }
            });


            var clientSock = SocketFactory.CreateSocket();

            // Valid invalid operation
            Assert.Throws<InvalidOperationException>(() => clientSock.GetStream());
            Assert.Throws<InvalidOperationException>(() => clientSock.Receive());
            Assert.Throws<InvalidOperationException>(() => clientSock.Send(null));
            Assert.Throws<SocketException>(() => clientSock.Connect(IPAddress.Any, 1024));

            clientSock.Connect(IPAddress.Loopback, port);

            // Valid invalid operation
            var byteBuf = ByteBufPool.Default.Allocate();
            Assert.Throws<ArgumentException>(() => clientSock.Send(byteBuf));
            byteBuf.Release();

            Assert.Throws<SocketException>(() => clientSock.Listen());
            if (clientSock is RioSocketWrapper)
            {
                Assert.Throws<InvalidOperationException>(() => clientSock.Accept());
            }

            using (var s = clientSock.GetStream())
            {
                // Send message
                s.Write(clientMsgBytes, 0, clientMsgBytes.Length);
                // Receive echo message
                var bytes = new byte[1024];
                var bytesRec = s.Read(bytes, 0, bytes.Length);
                Assert.AreEqual(clientMsgBytes.Length, bytesRec);
                var recvStr = Encoding.UTF8.GetString(bytes, 0, bytesRec);
                Assert.AreEqual(clientMsg, recvStr);

                // Send one byte
                byte[] oneBytes = { 1 };
                s.Write(oneBytes, 0, oneBytes.Length);

                // Receive echo message
                var oneByte = s.ReadByte();
                Assert.AreEqual((byte)1, oneByte);

                // Keep receiving to ensure no memory leak.
                while (true)
                {
                    bytesRec = s.Read(bytes, 0, bytes.Length);
                    recvStr = Encoding.UTF8.GetString(bytes, 0, bytesRec);
                    if (recvStr.IndexOf("<EOF>", StringComparison.OrdinalIgnoreCase) > -1)
                    {
                        break;
                    }
                }
                // send echo bytes
                s.Write(oneBytes, 0, oneBytes.Length);
            }

            clientSock.Close();
            // Verify invalid operation
            Assert.Throws<ObjectDisposedException>(() => clientSock.Receive());

            serverSocket.Close();
        }

        [Test]
        public void TestSaeaSocket()
        {
            // Set Socket wrapper to Saea
            Environment.SetEnvironmentVariable(ConfigurationService.CSharpSocketTypeEnvName, "Saea");
            SocketFactory.SocketWrapperType = SocketWrapperType.None;
            var serverSocket = SocketFactory.CreateSocket();
            Assert.IsTrue(serverSocket is SaeaSocketWrapper);
            SocketTest(serverSocket);
            
            // Reset socket wrapper type
            Environment.SetEnvironmentVariable(ConfigurationService.CSharpSocketTypeEnvName, string.Empty);
            SocketFactory.SocketWrapperType = SocketWrapperType.None;
        }

        [Test]
        public void TestRioSocket()
        {
            if (!SocketFactory.IsRioSockSupported())
            {
                Assert.Ignore("Omitting due to missing Riosock.dll. It might caused by no VC++ build tool or running on an OS that not supports Windows RIO socket.");
            }

            // Set Socket wrapper to Rio
            Environment.SetEnvironmentVariable(ConfigurationService.CSharpSocketTypeEnvName, "Rio");
            SocketFactory.SocketWrapperType = SocketWrapperType.None;
            RioSocketWrapper.rioRqGrowthFactor = 1;
            var serverSocket = SocketFactory.CreateSocket();
            Assert.IsTrue(serverSocket is RioSocketWrapper);
            SocketTest(serverSocket);
            
            // Reset socket wrapper type
            Environment.SetEnvironmentVariable(ConfigurationService.CSharpSocketTypeEnvName, string.Empty);
            SocketFactory.SocketWrapperType = SocketWrapperType.None;
            RioNative.UnloadRio();
        }


        [Test]
        public void TestUseThreadPoolForRioNative()
        {
            if (!SocketFactory.IsRioSockSupported())
            {
                Assert.Ignore("Omitting due to missing Riosock.dll. It might caused by no VC++ build tool or running on an OS that not supports Windows RIO socket.");
            }

            RioNative.SetUseThreadPool(true);
            RioNative.EnsureRioLoaded();
            Assert.AreEqual(Environment.ProcessorCount, RioNative.GetWorkThreadNumber());
            RioNative.UnloadRio();
            RioNative.SetUseThreadPool(false);
        }

        [Test]
        public void TestUseSingleThreadForRioNative()
        {
            if (!SocketFactory.IsRioSockSupported())
            {
                Assert.Ignore("Omitting due to missing Riosock.dll. It might caused by no VC++ build tool or running on an OS that not supports Windows RIO socket.");
            }

            RioNative.SetUseThreadPool(false);
            RioNative.EnsureRioLoaded();
            Assert.AreEqual(1, RioNative.GetWorkThreadNumber());
            RioNative.UnloadRio();
        }
    }
}