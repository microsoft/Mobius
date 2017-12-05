// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// RioNative class imports and initializes RIOSock.dll for use with RIO socket APIs.
    /// It also provided a simple thread pool that retrieves the results from IO completion port.
    /// </summary>
    internal class RioNative : IDisposable
    {
        private const int DefaultResultSize = 32; // Default RIO result size that be used to dequeue RIO results from IOCP
        private static readonly Lazy<RioNative> Default = new Lazy<RioNative>(() => new RioNative());
        private static readonly ILoggerService Logger = LoggerServiceFactory.GetLogger(typeof(RioNative));
        private static bool useThreadPool;

        private readonly ConcurrentDictionary<long, RioSocketWrapper> connectedSocks =
            new ConcurrentDictionary<long, RioSocketWrapper>();
        private volatile bool keepRunning = true;
        private bool disposed;
        private Thread[] workThreadPoolOfRecv;
        private Thread[] workThreadPoolOfSend;

        private RioNative()
        {
            Init();
        }

        /// <summary>
        /// Finalizer
        /// </summary>
        ~RioNative()
        {
            Dispose(false);
        }

        /// <summary>
        /// Release all resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        internal static int GetWorkThreadNumber()
        {
            return Default.Value.workThreadPoolOfRecv.Length;
        }

        /// <summary>
        /// Sets whether use thread pool to query RIO socket results, 
        /// it must be called before calling EnsureRioLoaded()
        /// </summary>
        internal static void SetUseThreadPool(bool toUseThreadPool)
        {
            useThreadPool = toUseThreadPool;
        }

        /// <summary>
        /// Gets the connection table that contains all connections.
        /// </summary>
        internal static ConcurrentDictionary<long, RioSocketWrapper> ConnectionTable
        {
            get { return Default.Value.connectedSocks; }
        }

        /// <summary>
        /// Ensures that the native dll of RIO socket is loaded and initialized.
        /// </summary>
        internal static void EnsureRioLoaded()
        {
            if (Default.Value == null)
            {
                throw new Exception("Failed to load RIOSOCK.dll and initialize it.");
            }

            if (Default.Value.disposed)
            {
                Default.Value.Init();
            }
        }

        /// <summary>
        /// Explicitly unload the native dll of RIO socket, and release resources.
        /// </summary>
        internal static void UnloadRio()
        {
            if (!Default.IsValueCreated) return;
            Default.Value.Dispose(false);
        }

        private void Dispose(bool disposing)
        {
            if (disposed) return;

            keepRunning = false;
            RIOSockUninitialize();
            disposed = true;

            if (disposing)
            {
                GC.SuppressFinalize(this);
            }
            Logger.LogDebug("Disposed RioNative instance.");
        }

        /// <summary>
        /// Initializes RIOSock native library.
        /// </summary>
        private void Init()
        {
            // Initializes the RIOSock
            var lastError = RIOSockInitialize();
            if (lastError < 0)
            {
                Logger.LogError("RIOSockInitialize() failed with error {0}.", lastError);
                Marshal.ThrowExceptionForHR(lastError);
            }

            // Create a thread pool for RIO socket
            var maxThreads = 1;
            if (useThreadPool)
            {
                var executorCores = int.Parse(Environment.GetEnvironmentVariable(ConfigurationService.ExecutorCoresEnvName) ?? "2");
                // In useThreadPool mode, the background threads should be 2 at least for each
                // receive /send completion procedure. If the value of the executorCores is more
                // than 2, We halve it for the maxThreads. So that the total number of background
                // threads equals to executorCores.
                maxThreads = executorCores > 2 ? (executorCores + 1) >> 1 : 2;
            }

            workThreadPoolOfRecv = new Thread[maxThreads];
            workThreadPoolOfSend = new Thread[maxThreads];
            for (var i = 0; i < maxThreads; i++)
            {
                // Start background threads for processing receive completion
                var worker = new Thread(WorkThreadFunc)
                {
                    Name = "RIOThread-Recv" + i,
                    IsBackground = true
                };
                workThreadPoolOfRecv[i] = worker;
                worker.Start(true);

                // Start background threads for processing send completion
                worker = new Thread(WorkThreadFunc)
                {
                    Name = "RIOThread-Send" + i,
                    IsBackground = true
                };
                workThreadPoolOfSend[i] = worker;
                worker.Start(false);
            }

            // if everything succeeds, post a Notify to catch the first set of IO
            if (!RegisterRIONotify(true/*Recv CQ*/) || !RegisterRIONotify(false/*Send CQ*/))
            {
                // Failed to post a NOTIFY.
                var socketException = new SocketException();
                Logger.LogError("RegisterRIONotify() failed with error {0}.", socketException.ErrorCode);

                // Stop threads and clean up resources.
                keepRunning = false;
                RIOSockUninitialize();
                throw socketException;
            }

            disposed = false;
        }

        private unsafe void WorkThreadFunc(object isRecv)
        {
            string operation = (bool) isRecv ? "Receive" : "Send";
            RioResult* results = stackalloc RioResult[DefaultResultSize];
            while (keepRunning)
            {
                if (!GetRIOCompletionStatus((bool)isRecv))
                {
                    var socketException = new SocketException();
                    Logger.LogError("GetRIOCompletionStatus([{0}]) with error {1}. Error Message: {2}",
                        operation, socketException.ErrorCode, socketException.Message);
                    //this one is not normal error. might need to debug this issue.
                    continue;
                }

                var resultCount = DequeueRIOResults((bool)isRecv, (IntPtr)results, DefaultResultSize);
                if (resultCount == 0 || resultCount == 0xFFFFFFFF /*RIO_CORRUPT_CQ*/)
                {
                    // We were notified there were completions, but we can't dequeue any IO
                    // Something has gone horribly wrong - likely our CQ is corrupt.
                    Logger.LogError(
                        "DequeueRIOResults([{0}]) returned [{1}] : expected to have dequeued IO after being signaled",
                        operation, resultCount);
                    continue;
                }

                for (uint i = 0; i < resultCount; ++i)
                {
                    var result = results[i];
                    RioSocketWrapper socket;
                    if (connectedSocks.TryGetValue(result.ConnectionId, out socket))
                    {
                        socket.IoCompleted(result.RequestId, result.Status, result.BytesTransferred);
                    }
                    else
                    {
                        if (result.Status == 0 && result.BytesTransferred == 0)
                        {
                            // Already normally removed from SocketTable.
                            break;
                        }

                        if (result.Status == (int)SocketError.ConnectionAborted)
                        {
                            Logger.LogDebug(
                                "The correlated socket [{0}] already disposed and removed from SocketTable.",
                                result.ConnectionId);
                            break;
                        }

                        var socketException = new SocketException(result.Status);
                        Logger.LogWarn("Failed to lookup socket [{0}] from SocketTable with status [{1}] and BytesTransferred [{2}] - Error Message: {3}.",
                            result.ConnectionId, result.Status, result.BytesTransferred, socketException.Message);
                    }
                }
            }
        }

        #region PInvoke

        private const string RioSockDll = "RIOSock.dll";
        private const string Ws2Dll = "WS2_32.dll";

        //
        // Private functions
        //

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        private static extern int RIOSockInitialize();

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        private static extern void RIOSockUninitialize();

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        private static extern bool RegisterRIONotify([In] bool isRecvCq);

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        private static extern bool GetRIOCompletionStatus([In] bool isRecvCq);

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        private static extern uint DequeueRIOResults([In] bool isRecvCq, [Out] IntPtr rioResults, [In] uint rioResultSize);

        //
        // Internal functions
        //

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern IntPtr CreateRIOSocket([In, Out] IntPtr localAddr, [In, Out] ref int addrLen);

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern IntPtr CreateRIORequestQueue(
            [In] IntPtr socket,
            [In] uint maxOutstandingReceive,
            [In] uint maxOutstandingSend,
            [In] long socketCorrelation);

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern unsafe bool PostRIOReceive(
            [In] IntPtr socketQueue,
            [In] RioBuf* pData,
            [In] uint dataBufferCount,
            [In] uint flags,
            [In] long requestCorrelation);

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern unsafe bool PostRIOSend(
            [In] IntPtr socketQueue,
            [In] RioBuf* pData,
            [In] uint dataBufferCount,
            [In] uint flags,
            [In] long requestCorrelation);

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern bool AllocateRIOCompletion([In] uint numCompletions);

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern bool ReleaseRIOCompletion([In] uint numCompletion);

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern bool ResizeRIORequestQueue(
            [In] IntPtr rq,
            [In] uint maxOutstandingReceive,
            [In] uint maxOutstandingSend);


        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern IntPtr RegisterRIOBuffer([In] IntPtr dataBuffer, [In] uint dataLength);

        [DllImport(RioSockDll, CharSet = CharSet.Unicode, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern void DeregisterRIOBuffer([In] IntPtr bufferId);

        [DllImport(Ws2Dll, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern IntPtr accept([In] IntPtr s, [In, Out] IntPtr addr, [In, Out] ref int addrlen);

        [DllImport(Ws2Dll, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern int connect([In] IntPtr s, [In] byte[] addr, [In] int addrlen);

        [DllImport(Ws2Dll, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern int closesocket([In] IntPtr s);

        [DllImport(Ws2Dll, SetLastError = true)]
        [SuppressUnmanagedCodeSecurity]
        internal static extern int listen([In] IntPtr s, [In] int backlog);

        #endregion
    }

    /// <summary>
    /// The RioResult structure contains data used to indicate request completion results used with RIO socket
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct RioResult
    {
        public int Status;
        public uint BytesTransferred;
        public long ConnectionId;
        public long RequestId;
    }
}