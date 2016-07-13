// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Net;

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// ISocketWrapper interface defines the common methods to operate a socket (traditional socket or 
    /// Windows Registered IO socket)
    /// </summary>
    internal interface ISocketWrapper : IDisposable
    {
        /// <summary>
        /// Accepts a incoming connection request.
        /// </summary>
        /// <returns>A ISocket instance used to send and receive data</returns>
        ISocketWrapper Accept();

        /// <summary>
        /// Close the ISocket connections and releases all associated resources.
        /// </summary>
        void Close();

        /// <summary>
        /// Establishes a connection to a remote host that is specified by an IP address and a port number
        /// </summary>
        /// <param name="remoteaddr">The IP address of the remote host</param>
        /// <param name="port">The port number of the remote host</param>
        void Connect(IPAddress remoteaddr, int port);

        /// <summary>
        /// Returns a stream used to send and receive data.
        /// </summary>
        /// <returns>The underlying Stream instance that be used to send and receive data</returns>
        Stream GetStream();

        /// <summary>
        /// Starts listening for incoming connections requests
        /// </summary>
        /// <param name="backlog">The maximum length of the pending connections queue. </param>
        void Listen(int backlog = 16);

        /// <summary>
        /// Receives network data from this socket, and returns a ByteBuf that contains the received data.
        /// </summary>
        /// <returns>A ByteBuf object that contains received data.</returns>
        ByteBuf Receive();

        /// <summary>
        /// Sends data to this socket with a ByteBuf object that contains data to be sent.
        /// </summary>
        /// <param name="data">A ByteBuf object that contains data to be sent</param>
        void Send(ByteBuf data);

        /// <summary>
        /// Indicates whether there are data that has been received from the network and is available to be read.
        /// </summary>
        bool HasData { get ; }

        /// <summary>
        /// Returns the local endpoint.
        /// </summary>
        EndPoint LocalEndPoint { get; }

        /// <summary>
        /// Returns the remote endpoint
        /// </summary>
        EndPoint RemoteEndPoint { get; }
    }
}