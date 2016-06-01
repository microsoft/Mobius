// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// SocketFactory is used to create ISocketWrapper instance based on a configuration and OS version.
    /// 
    /// The ISocket instance can be RioSocket object, if the configuration is set to RioSocket and
    /// only the application is running on a Windows OS that supports Registered IO socket.
    /// </summary>
    public static class SocketFactory
    {
        /// <summary>
        /// Creates a ISocket instance based on the configuration and OS version.
        /// </summary>
        /// <returns>
        /// A RioSocket instance, if the configuration is set to RioSocket and only the application
        /// is running on a Window OS that supports Registered IO socket. By default, it returns
        /// DefaultSocket instance which wraps System.Net.Sockets.Socket.
        /// </returns>
        public static ISocketWrapper CreateSocket()
        {
            return new DefaultSocketWrapper();
        }
    }
}