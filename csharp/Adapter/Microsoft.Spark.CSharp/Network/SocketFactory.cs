// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Spark.CSharp.Configuration;

[assembly: InternalsVisibleTo("CSharpWorker")]
[assembly: InternalsVisibleTo("Tests.Common")]
[assembly: InternalsVisibleTo("AdapterTest")]
[assembly: InternalsVisibleTo("WorkerTest")]
namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// SocketFactory is used to create ISocketWrapper instance based on a configuration and OS version.
    /// 
    /// The ISocket instance can be RioSocket object, if the configuration is set to RioSocket and
    /// only the application is running on a Windows OS that supports Registered IO socket.
    /// </summary>
    internal static class SocketFactory
    {
        private const string RiosockDll = "Riosock.dll";
        private static SocketWrapperType sockWrapperType = SocketWrapperType.None;

        /// <summary>
        /// Set socket wrapper type only for internal use (unit test)
        /// </summary>
        internal static SocketWrapperType SocketWrapperType
        {
            get
            {
                if (sockWrapperType != SocketWrapperType.None)
                {
                    return sockWrapperType;
                }

                sockWrapperType = SocketWrapperType.Normal;
                SocketWrapperType sockType;
                if (!Enum.TryParse(Environment.GetEnvironmentVariable(ConfigurationService.CSharpSocketTypeEnvName), out sockType))
                    return sockWrapperType;

                switch (sockType)
                {
                    case SocketWrapperType.Rio:
                        if (IsRioSockSupported())
                        {
                            sockWrapperType = SocketWrapperType.Rio;
                        }
                        break;
                    case SocketWrapperType.Saea:
                        sockWrapperType = sockType;
                        break;
                }

                return sockWrapperType;
            }
            set { sockWrapperType = value; }
        }

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
            switch (SocketWrapperType)
            {
                case SocketWrapperType.Normal:
                    return new DefaultSocketWrapper();

                case SocketWrapperType.Rio:
                    return new RioSocketWrapper();

                case SocketWrapperType.Saea:
                    return new SaeaSocketWrapper();

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        /// <summary>
        /// Indicates whether current OS supports RIO socket.
        /// </summary>
        public static bool IsRioSockSupported()
        {
            // Check is running on Windows
            var os = Environment.OSVersion;
            var p = (int) os.Platform;
            var isWindows = (p != 4) && (p != 6) && (p != 128);
            if (!isWindows) return false;

            // Check windows version, RIO is only supported on Win8 and above
            var osVersion = os.Version;
            if (osVersion.Major <= 6 && (osVersion.Major != 6 || osVersion.Minor < 2)) return false;

            // Check whether Riosock.dll exists, the Riosock.dll should be in the same folder with current assembly.
            var localDir = Path.GetDirectoryName(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath) ?? ".";
            return File.Exists(Path.Combine(localDir, RiosockDll));
        }
    }

    /// <summary>
    /// SocketWrapperType defines the socket wrapper type be used in transport.
    /// </summary>
    internal enum SocketWrapperType
    {
        /// <summary>
        /// None
        /// </summary>
        None,

        /// <summary>
        /// Indicates CSharp code use System.Net.Sockets.Socket as transport
        /// </summary>
        Normal,

        /// <summary>
        /// Indicates CSharp code use Windows RIO socket as transport
        /// </summary>
        Rio,

        /// <summary>
        /// Indicates CSharp code use System.Net.Sockets.Socket with SocketAsyncEventArgs as transport
        /// </summary>
        Saea
    }
}