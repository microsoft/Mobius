// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// A simple wrapper of System.Net.Sockets.Socket class.
    /// </summary>
    public class DefaultSocketWrapper : ISocketWrapper
    {
        private readonly Socket innerSocket;

        /// <summary>
        /// Default constructor that creates a new instance of DefaultSocket class which represents
        /// a traditional socket (System.Net.Socket.Socket).
        /// 
        /// This socket is bound to Loopback with port 0.
        /// </summary>
        public DefaultSocketWrapper()
        {
            innerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            var localEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
            innerSocket.Bind(localEndPoint);
        }

        /// <summary>
        /// Initializes a instance of DefaultSocket class using the specified System.Net.Socket.Socket object.
        /// </summary>
        /// <param name="socket">The existing socket</param>
        private DefaultSocketWrapper(Socket socket)
        {
            innerSocket = socket;
        }

        /// <summary>
        /// Accepts a incoming connection request.
        /// </summary>
        /// <returns>A DefaultSocket instance used to send and receive data</returns>
        public ISocketWrapper Accept()
        {
            var socket = innerSocket.Accept();
            return new DefaultSocketWrapper(socket);
        }

        /// <summary>
        /// Close the socket connections and releases all associated resources.
        /// </summary>
        public void Close()
        {
            innerSocket.Close();
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
        }

        /// <summary>
        /// Returns the NetworkStream used to send and receive data.
        /// </summary>
        /// <returns>The underlying Stream instance that be used to send and receive data</returns>
        /// <remarks>
        /// GetStream returns a NetworkStream that you can use to send and receive data. You must close/dispose
        /// the NetworkStream by yourself. Closing DefaultSocketWrapper does not release the NetworkStream
        /// </remarks>
        public Stream GetStream()
        {
            return new NetworkStream(innerSocket);
        }

        /// <summary>
        /// Starts listening for incoming connections requests
        /// </summary>
        /// <param name="backlog">The maximum length of the pending connections queue. </param>
        public void Listen(int backlog = (int)SocketOptionName.MaxConnections)
        {
            innerSocket.Listen(backlog);
        }

        /// <summary>
        /// Disposes the resources used by this instance of the DefaultSocket class.
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                innerSocket.Dispose();
            }
        }

        /// <summary>
        /// Releases all resources used by the current instance of the DefaultSocket class.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Frees resources used by DefaultSocket class
        /// </summary>
        ~DefaultSocketWrapper()
        {
            Dispose(false);
        }

        /// <summary>
        /// Returns the local endpoint.
        /// </summary>
        public EndPoint LocalEndPoint
        {
            get
            {
                return innerSocket.LocalEndPoint;
            }
        }
    }
}
