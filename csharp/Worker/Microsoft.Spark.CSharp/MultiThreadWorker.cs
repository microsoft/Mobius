// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Services;


namespace Microsoft.Spark.CSharp
{
    /// <summary>
    /// The C# worker process is shared by multiple JVM Tasks. A daemon thread listens to the server socket
    /// and establish new TCP connection. Multiple work threads pick up the TCP connection, use this connection
    /// to commuicate with JVM and do the actual work.
    /// </summary>
    public class MultiThreadWorker
    {
        private static ILoggerService logger = null;

        /// <summary>
        /// All TaskRunners that are not finished
        /// </summary>
        private ConcurrentDictionary<int, TaskRunner> taskRunnerRegistry = new ConcurrentDictionary<int, TaskRunner>();

        /// <summary>
        /// TaskRunners that are waiting to be picked up
        /// </summary>
        private BlockingCollection<TaskRunner> waitingTaskRunners = new BlockingCollection<TaskRunner>(new ConcurrentQueue<TaskRunner>());

        public void Run()
        {
            try
            {
                // start TCP listening server 
                TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();

                // get the local port and write it back to JVM side
                IPEndPoint endPoint = (IPEndPoint)listener.LocalEndpoint;
                int localPort = endPoint.Port;
                byte[] bytes = SerDe.ToBytes(localPort);
                Stream outputStream = Console.OpenStandardOutput();
                outputStream.Write(bytes, 0, sizeof(int));

                // can not initialize logger earlier to avoid unwanted stdout ouput 
                InitializeLogger();
                logger.LogInfo("Run MultiThreadWorker ...");
                logger.LogDebug("Port number used to pipe in/out data between JVM and CLR {0}", localPort);
                Worker.PrintFiles();

                // start daemon server to listen to socket
                new Thread(() =>
                {
                    StartDaemonServer(listener);
                }).Start();

                // read from JVM via pipe, stop TaskRunner according to instruction from JVM side
                Stream inputStream = Console.OpenStandardInput();
                while (true)
                {
                    int length = inputStream.Read(bytes, 0, sizeof(int));

                    if (length != sizeof(int))
                    {
                        logger.LogError(string.Format("read error, length: {0}, will exit", length));
                        Environment.Exit(-1);
                    }
                    int trId = SerDe.ToInt(bytes);
                    if (trId < 0)
                    {
                        // This branch is used in Unit test to stop MultiThreadServer
                        logger.LogInfo("receive negative trId: {0}, will exit", trId);
                        Environment.Exit(0);
                    }
                    else
                    {
                        logger.LogInfo(string.Format("try to stop taskRunner [{0}]", trId));
                        if (taskRunnerRegistry.ContainsKey(trId))
                        {
                            TaskRunner tr = taskRunnerRegistry[trId];
                            tr.Stop();
                        }
                        else
                        {
                            logger.LogWarn(string.Format("can't find taskRunner [{0}] in TaskRunnerRegistery. Maybe it has exited already?", trId));
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.LogError("RunDaemonWorker exception, will Exit");
                logger.LogException(e);
                Environment.Exit(-1);
            }
        }

        /// <summary>
        /// Listen to the server socket and accept new TCP connection from JVM side. Then create new TaskRunner instance and
        /// add it to waitingTaskRunners queue.
        /// </summary>
        private void StartDaemonServer(TcpListener listener)
        {
            logger.LogInfo("StartDaemonServer ...");

            bool sparkReuseWorker = false;
            string envVar = Environment.GetEnvironmentVariable("SPARK_REUSE_WORKER"); // this envVar is set in JVM side
            if ((envVar != null) && envVar.Equals("1"))
            {
                sparkReuseWorker = true;
            }

            try
            {
                int trId = 1;
                int workThreadNum = 0;

                while (true)
                {
                    Socket socket = listener.AcceptSocket();
                    logger.LogInfo("Connection accepted for taskRunnerId: {0}", trId);
                    using (NetworkStream s = new NetworkStream(socket))
                    {
                        SerDe.Write(s, trId); // write taskRunnerId to JVM side
                        s.Flush();
                    }
                    TaskRunner taskRunner = new TaskRunner(trId, socket, sparkReuseWorker);
                    waitingTaskRunners.Add(taskRunner);
                    taskRunnerRegistry[trId] = taskRunner;
                    trId++;

                    int taskRunnerNum = taskRunnerRegistry.Count();
                    while (workThreadNum < taskRunnerNum)  // launch new work thread as appropriate
                    {
                        // start threads that do the actual work of running tasks, there are several options here:
                        // Option 1. TPL - Task Parallel Library
                        // Option 2. ThreadPool
                        // Option 3. Self managed threads group
                        // Option 3 is selected after testing in real cluster because it can get the best performance.
                        // When using option 1 or 2, it is observered that the boot time may be as large as 50 ~ 60s.
                        // But it is always less than 1s for option 3. Perhaps this is because TPL and ThreadPool are not
                        // suitable for long running threads.
                        new Thread(FetchAndRun).Start();
                        workThreadNum++;
                    }
                }
            }
            catch (Exception e)
            {
                logger.LogError("StartDaemonServer exception, will exit");
                logger.LogException(e);
                Environment.Exit(-1);
            }
        }

        /// <summary>
        /// Fetch TaskRunner from activeTaskRunners queue and run it.
        /// </summary>
        private void FetchAndRun()
        {
            logger.LogInfo("FetchAndRun ...");

            try
            {
                while (true)
                {
                    TaskRunner tr;
                    if (waitingTaskRunners.TryTake(out tr, Timeout.Infinite))
                    {
                        tr.Run();

                        // remove it from registry
                        int trId = tr.trId;
                        TaskRunner tmp;
                        taskRunnerRegistry.TryRemove(tr.trId, out tmp);
                    }
                }
            }
            catch (Exception e)
            {
                logger.LogError("FetchAndRun exception, will exit");
                logger.LogException(e);
                Environment.Exit(-1);
            }
        }

        private static void InitializeLogger()
        {
            if (logger == null)
            {
                Worker.InitializeLogger();
                logger = LoggerServiceFactory.GetLogger(typeof(MultiThreadWorker));
            }
        }
    }
}
