// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// SockDataToken class is used to associate with the SocketAsyncEventArgs object.
    /// Primarily, it is a way to pass state to the event handler.
    /// </summary>
    internal class SockDataToken
    {
        /// <summary>
        /// Initializes a SockDataToken instance with a client socket and a dataBuf 
        /// </summary>
        /// <param name="clientSocket">The client socket</param>
        /// <param name="dataBuf">A data buffer that holds the data</param>
        public SockDataToken(SaeaSocketWrapper clientSocket, ByteBuf dataBuf)
        {
            ClientSocket = clientSocket;
            Data = dataBuf;
        }

        /// <summary>
        /// Reset this token
        /// </summary>
        public void Reset()
        {
            ClientSocket = null;
            if (Data != null) Data.Release();
            Data = null;
        }

        /// <summary>
        /// Detach the data ownership.
        /// </summary>
        public ByteBuf DetachData()
        {
            var retData = Data;
            Data = null;
            return retData;
        }

        /// <summary>
        /// Gets and sets the data
        /// </summary>
        public ByteBuf Data { get; private set; }

        /// <summary>
        /// Gets and sets the client socket.
        /// </summary>
        public SaeaSocketWrapper ClientSocket { get; private set; }
    }
}