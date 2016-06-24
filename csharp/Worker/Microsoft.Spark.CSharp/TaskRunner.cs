// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Threading;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Network;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp
{
    /// <summary>
    /// TaskRunner is used to run Spark task assigned by JVM side. It uses a TCP socket to
    /// communicate with JVM side. This socket may be reused to run multiple Spark tasks.
    /// </summary>
    internal class TaskRunner
    {
        private static ILoggerService logger = null;
        private ILoggerService Logger
        {
            get
            {
                if (logger == null)
                {
                    logger = LoggerServiceFactory.GetLogger(typeof(TaskRunner));
                }
                return logger;
            }
        }

        public int trId;  // task runner Id
        private ISocketWrapper socket;  // socket to communicate with JVM

        private volatile bool stop = false;

        // whether the socket can be reused to run multiple Spark tasks
        private bool socketReuse;

        public TaskRunner(int trId, ISocketWrapper socket, bool socketReuse)
        {
            this.trId = trId;
            this.socket = socket;
            this.socketReuse = socketReuse;
        }

        public void Run()
        {
            Logger.LogInfo(string.Format("TaskRunner [{0}] is running ...", trId));

            try
            {
                while (!stop)
                {
                    using (var networkStream = socket.GetStream())
                    {
                        byte[] bytes = SerDe.ReadBytes(networkStream, sizeof(int));
                        if (bytes != null)
                        {
                            int splitIndex = SerDe.ToInt(bytes);
                            bool readComplete = Worker.ProcessStream(networkStream, splitIndex);
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
                                SerDe.ReadBytes(networkStream);
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
                Logger.LogError(string.Format("TaskRunner [{0}] exeption, will dispose this TaskRunner", trId));
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
                    Logger.LogWarn(string.Format("close socket exception: ex", ex));
                }
                Logger.LogInfo(string.Format("TaskRunner [{0}] finished", trId));
            }
        }

        public void Stop()
        {
            Logger.LogInfo(string.Format("try to stop TaskRunner [{0}]", trId));
            stop = true;
        }
    }
}
