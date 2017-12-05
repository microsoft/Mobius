// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Network;
using Microsoft.Spark.CSharp.Services;

[assembly: InternalsVisibleTo("WorkerTest")]
namespace Microsoft.Spark.CSharp
{
    /// <summary>
    /// TaskRunner is used to run Spark task assigned by JVM side. It uses a TCP socket to
    /// communicate with JVM side. This socket may be reused to run multiple Spark tasks.
    /// </summary>
    internal class TaskRunner
    {
        private static ILoggerService logger;
        private static ILoggerService Logger
        {
            get
            {
                if (logger != null) return logger;
                logger = LoggerServiceFactory.GetLogger(typeof(TaskRunner));
                return logger;
            }
        }

        private readonly ISocketWrapper socket;  // Socket to communicate with JVM
        private volatile bool stop;
        private readonly bool socketReuse; // whether the socket can be reused to run multiple Spark tasks

        /// <summary>
        /// Task runner Id
        /// </summary>
        public int TaskId { get; private set; }

        public TaskRunner(int trId, ISocketWrapper socket, bool socketReuse)
        {
            TaskId = trId;
            this.socket = socket;
            this.socketReuse = socketReuse;
        }

        public void Run()
        {
            Logger.LogInfo("TaskRunner [{0}] is running ...", TaskId);

            try
            {
                while (!stop)
                {
                    using (var inputStream = socket.GetInputStream())
                    using (var outputStream = socket.GetOutputStream())
                    {
                        byte[] bytes = SerDe.ReadBytes(inputStream, sizeof(int));
                        if (bytes != null)
                        {
                            int splitIndex = SerDe.ToInt(bytes);
                            bool readComplete = Worker.ProcessStream(inputStream, outputStream, splitIndex);
                            outputStream.Flush();
                            if (!readComplete) // if the socket is not read through completely, then it can't be reused
                            {
                                stop = true;
                                // wait for server to complete, otherwise server may get 'connection reset' exception
                                Logger.LogInfo("Sleep 500 millisecond to close socket ...");
                                Thread.Sleep(500);
                            }
                            else if (!socketReuse)
                            {
                                stop = true;
                                // wait for server to complete, otherwise server gets 'connection reset' exception
                                // Use SerDe.ReadBytes() to detect java side has closed socket properly
                                // ReadBytes() will block until the socket is closed
                                Logger.LogInfo("waiting JVM side to close socket...");
                                SerDe.ReadBytes(inputStream);
                                Logger.LogInfo("JVM side has closed socket");
                            }
                        }
                        else
                        {
                            stop = true;
                            Logger.LogWarn("read null splitIndex, socket is closed by JVM");
                        }
                    }
                }
            }
            catch (Exception e)
            {
                stop = true;
                Logger.LogError("TaskRunner [{0}] exeption, will dispose this TaskRunner", TaskId);
                Logger.LogException(e);
            }
            finally
            {
                try
                {
                    socket.Close();
                }
                catch (Exception ex)
                {
                    Logger.LogWarn("close socket exception: {0}", ex);
                }
                Logger.LogInfo("TaskRunner [{0}] finished", TaskId);
            }
        }

        public void Stop()
        {
            Logger.LogInfo("try to stop TaskRunner [{0}]", TaskId);
            stop = true;
        }
    }
}
