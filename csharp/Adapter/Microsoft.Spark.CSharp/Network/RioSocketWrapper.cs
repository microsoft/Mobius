// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// RioSocketWrapper class is a wrapper of a socket that use Windows RIO socket with IO
    /// completion ports to implement socket operations.
    /// </summary>
    internal class RioSocketWrapper : ISocketWrapper
    {
        private const int Ipv4AddressSize = 16; // Default buffer size for IP v4 address
        private const int MaxDataCacheSize = 4096; // The max size of data caching in the queue.
        private static readonly int InitialCqRoom = 2 * rioRqGrowthFactor; // initial room allocated from completion queue.
        internal static int rioRqGrowthFactor = 2; // Growth factor used to grow the RIO request queue.

        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(RioSocketWrapper));
        private readonly BlockingCollection<ByteBuf> receivedDataQueue = 
            new BlockingCollection<ByteBuf>(new ConcurrentQueue<ByteBuf>(), MaxDataCacheSize);
        private readonly ConcurrentDictionary<long, RequestContext> requestContexts = 
            new ConcurrentDictionary<long, RequestContext>();
        private readonly BlockingCollection<int> sendStatusQueue =
            new BlockingCollection<int>(new ConcurrentQueue<int>(), MaxDataCacheSize);

        private long connectionId;
        private bool isCleanedUp;
        private bool isConnected;
        private bool isListening;
        private IntPtr rioRqHandle;
        private uint rqReservedSize = 2 * (uint)rioRqGrowthFactor;
        private uint rqUsed;

        /// <summary>
        /// Default ctor that creates a new instance of RioSocketWrapper class.
        /// The instance binds to loop-back address with port 0.
        /// </summary>
        public unsafe RioSocketWrapper()
        {
            RioNative.EnsureRioLoaded();

            // Creates a socket handle by calling native method.
            var sockaddlen = Ipv4AddressSize;
            var addrbuf = stackalloc byte[(sockaddlen / IntPtr.Size + 2) * IntPtr.Size];
            var sockaddr = (IntPtr)addrbuf;
            SockHandle = RioNative.CreateRIOSocket(sockaddr, ref sockaddlen);
            if (SockHandle == new IntPtr(-1))
            {
                // if the native call fails we'll throw a SocketException
                var socketException = new SocketException();
                logger.LogError("Native CreateRIOSocket() failed with error {0}", socketException.ErrorCode);
                throw socketException;
            }

            // Generate the local IP endpoint from the returned raw socket address data.
            LocalEndPoint = CreateIpEndPoint(sockaddr);
        }

        /// <summary>
        /// Initializes a RioSocketWrapper instance for an accepted socket.
        /// </summary>
        private RioSocketWrapper(IntPtr socketHandle, EndPoint localEp, EndPoint remoteEp)
        {
            SockHandle = socketHandle;
            LocalEndPoint = localEp;
            RemoteEndPoint = remoteEp;
            CreateRequestQueue();
            isConnected = true;
            // Post a receive operation from the connected RIO socket.
            DoReceive();
        }

        /// <summary>
        /// Finalizer
        /// </summary>
        ~RioSocketWrapper()
        {
            Dispose(false);
        }

        /// <summary>
        /// Indicates whether there are data that has been received from the network and is available to be read.
        /// </summary>
        public bool HasData { get { return receivedDataQueue.Count > 0; } }

        /// <summary>
        /// Returns the local endpoint.
        /// </summary>
        public EndPoint LocalEndPoint { get; private set; }

        /// <summary>
        /// Returns the remote endpoint
        /// </summary>
        public EndPoint RemoteEndPoint { get; private set; }

        /// <summary>
        /// Returns the handle of native socket.
        /// </summary>
        internal IntPtr SockHandle { get; private set; }

        /// <summary>
        /// Accepts a incoming connection request.
        /// </summary>
        /// <returns>A ISocket instance used to send and receive data</returns>
        public unsafe ISocketWrapper Accept()
        {
            EnsureAccessible();
            if (!isListening)
            {
                throw new InvalidOperationException("You must call the Listen method before performing this operation.");
            }

            var sockaddrlen = Ipv4AddressSize;
            var addrbuf = stackalloc byte[(sockaddrlen / IntPtr.Size + 2) * IntPtr.Size]; //sizeof DWORD
            var sockaddr = (IntPtr)addrbuf;

            var acceptedSockHandle = RioNative.accept(SockHandle, sockaddr, ref sockaddrlen);
            if (acceptedSockHandle == new IntPtr(-1))
            {
                // if the native call fails we'll throw a SocketException
                var socketException = new SocketException();
                logger.LogError("Native accept() failed with error {0}", socketException.NativeErrorCode);
                throw socketException;
            }

            var remoteEp = CreateIpEndPoint(sockaddr);
            var socket = new RioSocketWrapper(acceptedSockHandle, LocalEndPoint, remoteEp);
            logger.LogDebug("Accepted connection from {0} to {1}", socket.RemoteEndPoint, socket.LocalEndPoint);
            return socket;
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
            EnsureAccessible();

            var remoteEp = new IPEndPoint(remoteaddr, port);
            int sockaddrlen;
            var sockaddr = GetNativeSocketAddress(remoteEp, out sockaddrlen);
            var errorCode = RioNative.connect(SockHandle, sockaddr, sockaddrlen);
            if (errorCode != 0)
            {
                // if the native call fails we'll throw a SocketException
                var socketException = new SocketException();
                logger.LogError("Native connect() failed with error {0}", socketException.ErrorCode);
                throw socketException;
            }

            CreateRequestQueue();
            isConnected = true;
            RemoteEndPoint = remoteEp;

            // Post a receive operation from the connected RIO socket.
            DoReceive();
        }

        /// <summary>
        /// Releases all resources used by the current instance of the RioSocketWrapper class.
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
            EnsureAccessible();
            if (isListening) return;

            var errorCode = RioNative.listen(SockHandle, backlog);
            if (errorCode != 0)
            {
                var socketException = new SocketException();
                logger.LogError("Native listen() failed with error {0}", socketException.ErrorCode);
                throw socketException;
            }
            isListening = true;
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

            // Throw exception if there is an error.
            data.Release();
            Dispose(true);
            SocketException sockException = new SocketException(data.Status);
            throw sockException;
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

            var context = new RequestContext(SocketOperation.Send, data);
            DoSend(GenerateUniqueKey(), context);

            var status = sendStatusQueue.Take();
            if (status == (int)SocketError.Success) return;

            // throw a SocketException if theres is an error.
            Dispose(true);
            var socketException = new SocketException(status);
            throw socketException;
        }

        /// <summary>
        /// IO completion callback
        /// </summary>
        internal void IoCompleted(long requestId, int status, uint byteTransferred)
        {
            if (isCleanedUp) return;
            ReleaseRequest();

            RequestContext context;
            if (!requestContexts.TryRemove(requestId, out context)) return;

            switch (context.Operation)
            {
                case SocketOperation.Receive:
                    ProcessReceive(context, status, byteTransferred);
                    return;

                case SocketOperation.Send:
                    ProcessSend(requestId, context, status, byteTransferred);
                    return;
                
                default:
                    throw new InvalidOperationException("Invalid socket operation - not a receive / send operation.");
            }
        }

        /// <summary>
        /// Allocates a room form request queue for this next IO.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        private int AllocateRequest()
        {
            var newRqUsed = rqUsed + 1;
            if (newRqUsed > rqReservedSize)
            {
                var newRqReservedSize = rqReservedSize + rioRqGrowthFactor;
                if (!RioNative.AllocateRIOCompletion((uint)rioRqGrowthFactor))
                {
                    var errorCode = Marshal.GetLastWin32Error();
                    logger.LogError(
                        "Failed to allocate completions to this socket. AllocateRIOCompletion() returns error {0}",
                        errorCode);
                    return errorCode;
                }

                // Resize the RQ.
                if (!RioNative.ResizeRIORequestQueue(rioRqHandle, (uint)newRqReservedSize >> 1, (uint)newRqReservedSize >> 1))
                {
                    var errorCode = Marshal.GetLastWin32Error();
                    logger.LogError("Failed to resize the request queue. ResizeRIORequestQueue() returns error {0}",
                        errorCode);
                    RioNative.ReleaseRIOCompletion((uint)rioRqGrowthFactor);
                    return errorCode;
                }

                // since it succeeded, update reserved with the new size
                rqReservedSize = (uint)newRqReservedSize;
            }

            // everything succeeded - update rqUsed with the new slots being used for this next IO
            rqUsed = newRqUsed;
            return 0;
        }

        /// <summary>
        /// Creates a request queue for this socket operation
        /// </summary>
        private void CreateRequestQueue()
        {
            // Allocate completion from completion queue
            if (!RioNative.AllocateRIOCompletion((uint)InitialCqRoom))
            {
                var socketException = new SocketException();
                logger.LogError(
                    "AllocateRIOCompletion() failed to allocate completions to this socket. Returns error {0}",
                    socketException.ErrorCode);
                throw socketException;
            }

            // Create the RQ for this socket.
            connectionId = GenerateUniqueKey();
            while (!RioNative.ConnectionTable.TryAdd(connectionId, this))
            {
                connectionId = GenerateUniqueKey();
            }

            rioRqHandle = RioNative.CreateRIORequestQueue(SockHandle, rqReservedSize >> 1, rqReservedSize >> 1, connectionId);
            if (rioRqHandle != IntPtr.Zero) return;

            // Error Handling
            var sockException = new SocketException();
            RioNative.ReleaseRIOCompletion((uint)InitialCqRoom);
            RioSocketWrapper sock;
            RioNative.ConnectionTable.TryRemove(connectionId, out sock);
            logger.LogError("CreateRIORequestQueue() returns error {0}", sockException.ErrorCode);
            throw sockException;
        }

        private void Dispose(bool disposing)
        {
            // Mark this as disposed before changing anything else.
            var cleanedUp = isCleanedUp;
            isCleanedUp = true;
            if (!cleanedUp && disposing)
            {
                try
                {
                    // Release room back to Completion Queue
                    RioNative.ReleaseRIOCompletion((uint)InitialCqRoom);

                    // Remove this socket from the connected socket table.
                    RioSocketWrapper socket;
                    RioNative.ConnectionTable.TryRemove(connectionId, out socket);
                }
                catch (Exception)
                {
                    logger.LogDebug("RioNative default instance already disposed.");
                }

                // Remove all pending socket operations
                if (!requestContexts.IsEmpty)
                {
                    foreach (var keyValuePair in requestContexts)
                    {
                        // Release the data buffer of the pending operation
                        keyValuePair.Value.Data.Release();
                    }
                    requestContexts.Clear();
                }

                // Remove received Data from the queue, and release buffer back to byte pool.
                while (receivedDataQueue.Count > 0)
                {
                    ByteBuf data;
                    receivedDataQueue.TryTake(out data);
                    data.Release();
                }

                // Close the socket handle. No need to release Request Queue handle that
                // will be gone once the socket handle be closed.
                if (SockHandle != IntPtr.Zero)
                {
                    RioNative.closesocket(SockHandle);
                    SockHandle = IntPtr.Zero;
                }

                GC.SuppressFinalize(this);
            }

            isConnected = false;
            isListening = false;
        }

        private void EnsureAccessible()
        {
            if (isCleanedUp)
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }

        /// <summary>
        /// Posts a receive operation to this socket
        /// </summary>
        private unsafe void DoReceive()
        {
            // Make a room from Request Queue of this socket for operation.
            var errorStatus = AllocateRequest();
            if (errorStatus != 0)
            {
                logger.LogError("Cannot post receive operation due to no room in Request Queue.");
                receivedDataQueue.Add(ByteBuf.NewErrorStatusByteBuf(errorStatus));
                return;
            }

            // Allocate buffer to receive incoming network data.
            var dataBuffer = ByteBufPool.UnsafeDefault.Allocate();
            if (dataBuffer == null)
            {
                logger.LogError("Failed to allocate ByteBuf at DoReceive().");
                receivedDataQueue.Add(ByteBuf.NewErrorStatusByteBuf((int)SocketError.NoBufferSpaceAvailable));
                return;
            }
            var context = new RequestContext(SocketOperation.Receive, dataBuffer);
            var recvId = GenerateUniqueKey();

            // Add the operation context to request table for completion callback.
            while (!requestContexts.TryAdd(recvId, context))
            {
                // Generate another key, if the key is duplicated.
                recvId = GenerateUniqueKey();
            }

            // Post a receive operation via native method.
            var rioBuf = dataBuffer.GetInputRioBuf();
            if (RioNative.PostRIOReceive(rioRqHandle, &rioBuf, 1, 0, recvId)) return;

            requestContexts.TryRemove(recvId, out context);
            context.Data.Release();

            if (isCleanedUp)
            {
                logger.LogDebug("Socket is already disposed. DoReceive() do nothing.");
                receivedDataQueue.Add(ByteBuf.NewErrorStatusByteBuf((int)SocketError.NetworkDown));
                return;
            }

            // Log exception, if post receive operation failed.
            var socketException = new SocketException();
            logger.LogError("Failed to call DoReceive() with error code [{0}], error message: {1}",
                socketException.ErrorCode, socketException.Message);
            context.Data.Status = socketException.ErrorCode;
            receivedDataQueue.Add(context.Data);
        }

        /// <summary>
        /// This method is invoked by the IoCompleted method to process the receive completion.
        /// </summary>
        private void ProcessReceive(RequestContext context, int status, uint byteTransferred)
        {
            context.Data.Status = status;
            var data = context.Data;
            data.WriterIndex += (int)byteTransferred;
            receivedDataQueue.Add(data);

            if (status != (int) SocketError.Success)
            {
                logger.LogError("Socket receive operation failed with error {0}", status);
                context.Data.Release();
                return;
            }

            // Posts another receive operation
            DoReceive();
        }

        /// <summary>
        /// Posts a send operation to this socket.
        /// </summary>
        private unsafe void DoSend(long sendId, RequestContext context)
        {
            // Make a room from Request Queue of this socket for operation.
            var errorStatus = AllocateRequest();
            if ( errorStatus != 0)
            {
                logger.LogError("Cannot post send operation due to no room in Request Queue.");
                sendStatusQueue.Add(errorStatus);
                return;
            }

            // Add the operation context to request table for completion callback.
            while (!requestContexts.TryAdd(sendId, context))
            {
                // Generate another key, if the key is duplicated.
                sendId = GenerateUniqueKey();
            }

            // Post a send operation via native method.
            var rioBuf = context.Data.GetOutputRioBuf();
            if (RioNative.PostRIOSend(rioRqHandle, &rioBuf, 1, 0, sendId)) return;

            requestContexts.TryRemove(sendId, out context);
            context.Data.Release();

            if (isCleanedUp)
            {
                logger.LogDebug("Socket is already disposed. PostSend() do nothing.");
                receivedDataQueue.Add(ByteBuf.NewErrorStatusByteBuf((int)SocketError.NetworkDown));
                return;
            }

            // Log exception, if post send operation failed.
            var socketException = new SocketException();
            logger.LogError("Failed to call PostRIOSend() with error code [{0}]. Error message: {1}",
                socketException.ErrorCode, socketException.Message);
            sendStatusQueue.Add(socketException.ErrorCode);
        }

        /// <summary>
        /// This method is invoked by the IoCompleted method to process the send completion.
        /// </summary>
        private void ProcessSend(long requestId, RequestContext context, int status, uint byteTransferred)
        {
            sendStatusQueue.Add(status);
            if (status != (int)SocketError.Success)
            {
                logger.LogError("Socket send operation failed with error {0}", status);
                context.Data.Release();
                return;
            }

            var data = context.Data;
            data.ReaderIndex += (int)byteTransferred;
            if (data.IsReadable())
            {
                // If some of the bytes in the message have NOT been sent,
                // then we need to post another send operation.
                context.Data = data;
                DoSend(requestId, context);
            }
            else
            {
                // All the bytes in the data have been sent, 
                // release the buffer back to pool. 
                data.Release();
            }
        }

        /// <summary>
        /// Release room back to request queue.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        private void ReleaseRequest()
        {
            rqUsed -= 1;
        }

        private static unsafe IPEndPoint CreateIpEndPoint(IntPtr addr)
        {
            var addrBuf = (byte*)addr;
            var address = ((addrBuf[4] & 0x000000FF) |
                           (addrBuf[5] << 8 & 0x0000FF00) |
                           (addrBuf[6] << 16 & 0x00FF0000) |
                           (addrBuf[7] << 24)) & 0x00000000FFFFFFFF;

            var ipAddr = new IPAddress(address);
            var port = (addrBuf[2] << 8 & 0xFF00) | addrBuf[3];
            return new IPEndPoint(ipAddr, port);
        }

        /// <summary>
        /// Generates a unique key from a GUID.
        /// </summary>
        private static long GenerateUniqueKey()
        {
            Debug.Assert(IntPtr.Size == 8); // For x64 bits.
            var buffer = Guid.NewGuid().ToByteArray();
            return BitConverter.ToInt64(buffer, 0);
        }

        private static byte[] GetNativeSocketAddress(IPEndPoint ipEp, out int sockaddrLen)
        {
            var sockaddr = ipEp.Serialize();
            sockaddrLen = sockaddr.Size;
            var addrbuf = new byte[(sockaddrLen / IntPtr.Size + 2) * IntPtr.Size]; //sizeof DWORD

            // Address Family serialization
            addrbuf[0] = sockaddr[0];
            addrbuf[1] = sockaddr[1];

            // Port serialization
            addrbuf[2] = sockaddr[2];
            addrbuf[3] = sockaddr[3];

            // IPv4 Address serialization
            addrbuf[4] = sockaddr[4];
            addrbuf[5] = sockaddr[5];
            addrbuf[6] = sockaddr[6];
            addrbuf[7] = sockaddr[7];
            return addrbuf;
        }
    }
    
    internal class RequestContext
    {
        public RequestContext(SocketOperation operation, ByteBuf data)
        {
            Operation = operation;
            Data = data;
        }

        public SocketOperation Operation { get; private set; }
        public ByteBuf Data { get; set; }
    }

    internal enum SocketOperation
    {
        None = 0,
        Receive,
        Send
    }
}