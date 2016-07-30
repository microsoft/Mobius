// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// SaeaSocketWrapper class is a wrapper of a socket that use SocketAsyncEventArgs class
    /// to implement socket operations.
    /// </summary>
    internal class SaeaSocketWrapper : ISocketWrapper
    {
        private const int MaxDataCacheSize = 4096; // The max size of data caching in the queue.

        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SaeaSocketWrapper));
        private readonly BlockingCollection<ByteBuf> receivedDataQueue =
            new BlockingCollection<ByteBuf>(new ConcurrentQueue<ByteBuf>(), MaxDataCacheSize);
        private readonly BlockingCollection<int> sendStatusQueue =
            new BlockingCollection<int>(new ConcurrentQueue<int>(), MaxDataCacheSize);

        private readonly ConcurrentQueue<SocketAsyncEventArgs> poolOfAcceptEvents =
            new ConcurrentQueue<SocketAsyncEventArgs>();
        private readonly ConcurrentQueue<SocketAsyncEventArgs> poolOfRecvSendEvents =
            new ConcurrentQueue<SocketAsyncEventArgs>(); 

        private readonly SaeaSocketWrapper parent;
        private Socket innerSocket;
        private bool isCleanedUp;
        private bool isConnected;

        /// <summary>
        /// Default ctor that creates a new instance of SaeaSocketWrapper class.
        /// The instance binds to loop-back address with port 0.
        /// </summary>
        public SaeaSocketWrapper()
        {
            innerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            var localEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
            innerSocket.Bind(localEndPoint);
        }

        /// <summary>
        /// Initializes a SaeaSocketWrapper instance for an accepted socket.
        /// </summary>
        private SaeaSocketWrapper(SaeaSocketWrapper parent, Socket acceptedSocket)
        {
            this.parent = parent;
            innerSocket = acceptedSocket;
            isConnected = true;
        }

        /// <summary>
        /// Finalizer.
        /// </summary>
        ~SaeaSocketWrapper()
        {
            Dispose(false);
        }

        /// <summary>
        /// Indicates whether there are data that has been received from the network and is available to be read.
        /// </summary>
        public bool HasData
        {
            get
            {
                EnsureAccessible();
                return receivedDataQueue.Count > 0;
            }
        }

        /// <summary>
        /// Returns the local endpoint.
        /// </summary>
        public EndPoint LocalEndPoint { get { return innerSocket.LocalEndPoint; } }

        /// <summary>
        /// Returns the remote endpoint
        /// </summary>
        public EndPoint RemoteEndPoint { get { return innerSocket.RemoteEndPoint; } }

        /// <summary>
        /// Accepts a incoming connection request.
        /// </summary>
        /// <returns>A ISocket instance used to send and receive data</returns>
        public ISocketWrapper Accept()
        {
            var socket = innerSocket.Accept();
            var clientSock = new SaeaSocketWrapper(this, socket);
            logger.LogDebug("Accepted connection from {0} to {1}", clientSock.RemoteEndPoint, clientSock.LocalEndPoint);
            DoReceive(clientSock);
            return clientSock;
        }

        /// <summary>
        /// Close the ISocket connections and releases all associated resources.
        /// </summary>
        public void Close()
        {
            Dispose(true);
        }

        /// <summary>
        /// Establishes a connection to a remote host that is specified by an IP address and a port number
        /// </summary>
        /// <param name="remoteaddr">The IP address of the remote host</param>
        /// <param name="port">The port number of the remote host</param>
        public void Connect(IPAddress remoteaddr, int port)
        {
            var remoteEndPoint = new IPEndPoint(remoteaddr, port);
            innerSocket.Connect(remoteEndPoint);
            isConnected = true;

            DoReceive(this);
        }

        /// <summary>
        /// Releases all resources used by the current instance of the SaeaSocketWrapper class.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Returns a stream used to send and receive data.
        /// </summary>
        /// <returns>The underlying Stream instance that be used to send and receive data</returns>
        public Stream GetStream()
        {
            EnsureAccessible();
            if (!isConnected)
            {
                throw new InvalidOperationException("The operation is not allowed on non-connected sockets.");
            }

            return new SocketStream(this);
        }

        /// <summary>
        /// Starts listening for incoming connections requests
        /// </summary>
        /// <param name="backlog">The maximum length of the pending connections queue. </param>
        public void Listen(int backlog = 16)
        {
            innerSocket.Listen(backlog);
        }

        /// <summary>
        /// Receives network data from this socket, and returns a ByteBuf that contains the received data.
        /// </summary>
        /// <returns>A ByteBuf object that contains received data.</returns>
        public ByteBuf Receive()
        {
            EnsureAccessible();
            if (!isConnected)
            {
                throw new InvalidOperationException("The operation is not allowed on non-connected sockets.");
            }

            var data = receivedDataQueue.Take();
            if (data.Status == (int) SocketError.Success) return data;

            // throw a SocketException if theres is an error.
            data.Release();
            Dispose(true);
            var socketException = new SocketException(data.Status);
            throw socketException;
        }

        /// <summary>
        /// Sends data to this socket with a ByteBuf object that contains data to be sent.
        /// </summary>
        /// <param name="data">A ByteBuf object that contains data to be sent</param>
        public void Send(ByteBuf data)
        {
            EnsureAccessible();
            if (!isConnected)
            {
                throw new InvalidOperationException("The operation is not allowed on non-connected sockets.");
            }

            if (!data.IsReadable())
            {
                throw new ArgumentException("The parameter {0} must contain one or more elements.", "data");
            }

            var dataToken = new SockDataToken(this, data);
            if (parent != null)
            {
                parent.DoSend(dataToken);
            }
            else
            {
                DoSend(dataToken);
            }
            var status = sendStatusQueue.Take();
            if (status == (int) SocketError.Success) return;

            // throw a SocketException if theres is an error.
            dataToken.Reset();
            Dispose(true);
            var socketException = new SocketException(status);
            throw socketException;
        }

        /// <summary>
        /// Implementation of the Dispose pattern.
        /// </summary>
        private void Dispose(bool disposing)
        {
            // Mark this as disposed before changing anything else.
            var cleanedUp = isCleanedUp;
            isCleanedUp = true;
            if (cleanedUp || !disposing) return;

            if (innerSocket != null)
            {
                try
                {
                    // Gracefully shut down socket first.
                    innerSocket.Shutdown(SocketShutdown.Both);
                }
                catch (Exception)
                {
                    // Ignore exceptions from Shutdown function
                }
                finally
                {
                    innerSocket.Dispose();
                }
                innerSocket = null;
            }

            SocketAsyncEventArgs eventArgs;
            while (poolOfAcceptEvents.Count > 0)
            {
                poolOfAcceptEvents.TryDequeue(out eventArgs);
                eventArgs.Dispose();
            }

            while (poolOfRecvSendEvents.Count > 0)
            {
                poolOfRecvSendEvents.TryDequeue(out eventArgs);
                if (eventArgs.UserToken != null)
                {
                    var dataToken = (SockDataToken) eventArgs.UserToken;
                    if (dataToken.ClientSocket != null)
                    {
                        dataToken.ClientSocket.Dispose();
                    }
                    dataToken.Reset();
                }
                eventArgs.Dispose();
            }

            while (receivedDataQueue.Count > 0)
            {
                ByteBuf data;
                receivedDataQueue.TryTake(out data);
                data.Release();
            }

            GC.SuppressFinalize(this);

            isCleanedUp = true;
            isConnected = false;
        }

        private void EnsureAccessible()
        {
            if (isCleanedUp)
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }

        /// <summary>
        /// The callback is called whenever a receive or send operation completes. However,
        /// it is not be called if the receive/send operation completes synchronously.
        /// </summary>
        private void IoCompleted(object sender, SocketAsyncEventArgs e)
        {
            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Receive:
                    ProcessReceive(e);
                    break;

                case SocketAsyncOperation.Send:
                    ProcessSend(e);
                    break;

                default:
                    throw new ArgumentException(
                        "The last operation completed on the socket was not a receive or send");
            }
        }

        /// <summary>
        /// Post a receive operation
        /// </summary>
        private void DoReceive(SaeaSocketWrapper clientSocket)
        {
            // Prepares the SocketAsyncEventArgs for receive operation.
            SocketAsyncEventArgs recvEventArg;
            if (!poolOfRecvSendEvents.TryDequeue(out recvEventArg))
            {
                recvEventArg = new SocketAsyncEventArgs();
                recvEventArg.Completed += IoCompleted;
            }
            
            // Allocate buffer for the receive operation.
            var dataBuf = ByteBufPool.Default.Allocate();
            recvEventArg.UserToken = new SockDataToken(clientSocket, dataBuf);
            recvEventArg.SetBuffer(dataBuf.Array, dataBuf.WriterIndexOffset, dataBuf.WritableBytes);

            // Post async receive operation on the socket
            var socket = clientSocket.innerSocket;
            recvEventArg.AcceptSocket = socket;
            if (clientSocket.isCleanedUp || socket == null)
            {
                // do nothing if client socket already disposed.
                dataBuf.Status = (int) SocketError.NetworkDown;
                dataBuf.Release();
                recvEventArg.UserToken = null;
                clientSocket.receivedDataQueue.Add(dataBuf);
                return;
            }

            var willRaiseEvent = socket.ReceiveAsync(recvEventArg);
            if (!willRaiseEvent)
            {
                // The operation completed synchronously, we need to call ProcessReceive method directly
                ProcessReceive(recvEventArg);
            }
        }

        /// <summary>
        /// This method is invoked by the IoCompleted method when an asynchronous receive
        /// operation completes. If the remote host closed the connection, then the socket
        /// is closed. Otherwise, we process the received data.
        /// </summary>
        private void ProcessReceive(SocketAsyncEventArgs recvEventArg)
        {
            var userToken = (SockDataToken) recvEventArg.UserToken;
            var recvData = userToken.DetachData();
            var clientSocket = userToken.ClientSocket;
            //update write index according to the BytesTransferred
            recvData.WriterIndex += recvEventArg.BytesTransferred;
            recvData.Status = (int)recvEventArg.SocketError;
            clientSocket.receivedDataQueue.Add(recvData);

            if (recvEventArg.SocketError != SocketError.Success)
            {
                return;
            }

            // Recycle the EventArgs
            recvEventArg.UserToken = null;
            poolOfRecvSendEvents.Enqueue(recvEventArg);
            
            // Post another receive operation
            DoReceive(clientSocket);
        }

        /// <summary>
        /// Posts a send operation with a SockDataToken which contains the data to be sent.
        /// </summary>
        private void DoSend(SockDataToken dataToken)
        {
            // Prepare the SocketAsyncEventArgs for send operation
            SocketAsyncEventArgs sendEventArg;
            if (!poolOfRecvSendEvents.TryDequeue(out sendEventArg))
            {
                sendEventArg = new SocketAsyncEventArgs();
                sendEventArg.Completed += IoCompleted;
            }
            sendEventArg.UserToken = dataToken;
            sendEventArg.AcceptSocket = dataToken.ClientSocket.innerSocket;
            DoSend(sendEventArg);
        }

        /// <summary>
        /// Posts a send operation with a send EventArgs
        /// </summary>
        private void DoSend(SocketAsyncEventArgs sendEventArg)
        {
            // Set buffer for the SocketAsyncEventArgs
            var dataToken = (SockDataToken)sendEventArg.UserToken;
            var data = dataToken.Data;
            sendEventArg.SetBuffer(data.Array, data.ReaderIndexOffset, data.ReadableBytes);

            //post asynchronous send operation
            var socket = dataToken.ClientSocket.innerSocket;
            sendEventArg.AcceptSocket = socket;
            if (dataToken.ClientSocket.isCleanedUp || socket == null)
            {
                // do nothing if client socket already disposed.
                var clientSocket = dataToken.ClientSocket;
                dataToken.Reset();
                clientSocket.sendStatusQueue.Add((int)SocketError.NetworkDown);
                return;
            }

            var willRaiseEvent = socket.SendAsync(sendEventArg);
            if (!willRaiseEvent)
            {
                // The operation completed synchronously, we need to call ProcessSend method directly
                ProcessSend(sendEventArg);
            }
        }

        /// <summary>
        /// This method is called by IoCompleted() when an asynchronous send completes.  
        /// If all of the data has NOT been sent, then it calls PostSend to send more data. 
        /// </summary>
        private void ProcessSend(SocketAsyncEventArgs sendEventArg)
        {
            var sendToken = (SockDataToken)sendEventArg.UserToken;
            sendToken.ClientSocket.sendStatusQueue.Add((int)sendEventArg.SocketError);
            if (sendEventArg.SocketError != SocketError.Success)
            {
                sendToken.Reset();
                sendEventArg.UserToken = null;
                if (parent != null)
                {
                    parent.poolOfRecvSendEvents.Enqueue(sendEventArg);
                }
                return;
            }
            
            var data = sendToken.Data;
            data.ReaderIndex += sendEventArg.BytesTransferred;
            if (data.IsReadable())
            {
                // If some of the bytes in the message have NOT been sent,
                // then we will need to post another send operation.
                DoSend(sendEventArg);
            }
            else
            {
                // All the bytes in the message have been sent. 
                sendToken.Reset();
                sendEventArg.UserToken = null;
                if (parent != null)
                {
                    parent.poolOfRecvSendEvents.Enqueue(sendEventArg);
                    return;
                }

                poolOfRecvSendEvents.Enqueue(sendEventArg);
            }
        }
    }
}