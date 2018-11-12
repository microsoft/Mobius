// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql;
using Razorvine.Pickle;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.Spark.CSharp
{
    /// <summary>
    /// This class execute user defined methods.    
    /// </summary>

    internal class UDFCommand
    {
        private readonly DateTime UnixTimeEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private ILoggerService logger;
        private Stream inputStream;
        private Stream outputStream;
        private int splitIndex;
        private DateTime bootTime;
        private string deserializerMode;
        private string serializerMode;
        private IFormatter formatter;
        private Stopwatch commandProcessWatch;
        private int isSqlUdf;
        private List<WorkerFunc> workerFuncList;
        private int stageId;

        public UDFCommand(Stream inputStream, Stream outputStream, int splitIndex, DateTime bootTime, 
            string deserializerMode, string serializerMode, IFormatter formatter, 
            Stopwatch commandProcessWatch, int isSqlUdf, List<WorkerFunc> workerFuncList, int stageId)
        {
            this.inputStream = inputStream;
            this.outputStream = outputStream;
            this.splitIndex = splitIndex;
            this.bootTime = bootTime;
            this.deserializerMode = deserializerMode;
            this.serializerMode = serializerMode;
            this.formatter = formatter;
            this.commandProcessWatch = commandProcessWatch;
            this.isSqlUdf = isSqlUdf;
            this.workerFuncList = workerFuncList;
            this.stageId = stageId;

            InitializeLogger();
        }

        private void InitializeLogger()
        {
            try
            {
                // if there exists exe.config file, then use log4net
                if (File.Exists(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile))
                {
                    LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance);
                }

                logger = LoggerServiceFactory.GetLogger(typeof(UDFCommand));
            }
            catch (Exception e)
            {
                Console.WriteLine("InitializeLogger exception {0}, will exit", e);
                Environment.Exit(-1);
            }
        }

        internal void Execute()
        {
            if (isSqlUdf == 0)
            {
                ExecuteNonSqlUDF();
            }
            else
            {
                ExecuteSqlUDF();
            }
        }

        private void ExecuteNonSqlUDF()
        {
            int count = 0;
            int nullMessageCount = 0;
            logger.LogDebug("Beginning to execute non sql func");
            WorkerFunc workerFunc = workerFuncList[0];
            var func = workerFunc.CharpWorkerFunc.Func;

            var funcProcessWatch = Stopwatch.StartNew();
            DateTime initTime = DateTime.UtcNow;
            foreach (var message in func(splitIndex, GetIterator(inputStream, deserializerMode, isSqlUdf)))
            {
                funcProcessWatch.Stop();

                if (object.ReferenceEquals(null, message))
                {
                    nullMessageCount++;
                    continue;
                }

                try
                {
                    WriteOutput(outputStream, serializerMode, message, formatter);
                }
                catch (Exception ex)
                {
                    logger.LogError("WriteOutput() failed at iteration {0}, execption {1}", count, ex);
                    throw;
                }

                count++;
                funcProcessWatch.Start();
            }

            logger.LogInfo("Output entries count: " + count);
            logger.LogDebug("Null messages count: " + nullMessageCount);

            WriteDiagnosticsInfo(outputStream, bootTime, initTime);

            commandProcessWatch.Stop();

            // log statistics
            logger.LogInfo("func process time: {0}", funcProcessWatch.ElapsedMilliseconds);
            logger.LogInfo("stage {0}, command process time: {1}", stageId, commandProcessWatch.ElapsedMilliseconds);
        }

        private void ExecuteSqlUDF()
        {
            int count = 0;
            int nullMessageCount = 0;
            logger.LogDebug("Beginning to execute sql func");

            var funcProcessWatch = Stopwatch.StartNew();
            DateTime initTime = DateTime.UtcNow;

            foreach (var row in GetIterator(inputStream, deserializerMode, isSqlUdf))
            {                               
                List<Object> messages = new List<Object>();
               
                foreach (WorkerFunc workerFunc in workerFuncList)
                {
                    List<Object> args = new List<Object>();
                    foreach (int offset in workerFunc.ArgOffsets)
                    {                        
                        args.Add(row[offset]);
                    }

                    foreach (var message in workerFunc.CharpWorkerFunc.Func(splitIndex, new[] { args.ToArray()}))
                    {
                        funcProcessWatch.Stop();

                        if (object.ReferenceEquals(null, message))
                        {
                            nullMessageCount++;
                            continue;
                        }

                        messages.Add(message);
                    }
                }

                try
                {
                    dynamic res = messages.ToArray();
                    if (messages.Count == 1)
                    {
                        res = messages[0];
                    }

                    WriteOutput(outputStream, serializerMode, res, formatter);
                }
                catch (Exception ex)
                {
                    logger.LogError("WriteOutput() failed at iteration {0}, exception error {1}", count, ex.Message);
                    throw;
                }

                count++;
                funcProcessWatch.Start();
            }

            logger.LogInfo("Output entries count: " + count);
            logger.LogDebug("Null messages count: " + nullMessageCount);

            WriteDiagnosticsInfo(outputStream, bootTime, initTime);

            commandProcessWatch.Stop();

            // log statistics
            logger.LogInfo("func process time: {0}", funcProcessWatch.ElapsedMilliseconds);
            logger.LogInfo("stage {0}, command process time: {0}", stageId, commandProcessWatch.ElapsedMilliseconds);
        }

        private void WriteOutput(Stream networkStream, string serializerMode, dynamic message, IFormatter formatter)
        {
            var buffer = GetSerializedMessage(serializerMode, message, formatter);
            if (buffer == null)
            {
                logger.LogError("Buffer is null");
            }

            if (buffer.Length <= 0)
            {
                logger.LogError("Buffer length {0} cannot be <= 0", buffer.Length);
            }

            SerDe.Write(networkStream, buffer.Length);
            SerDe.Write(networkStream, buffer);
        }

        private byte[] GetSerializedMessage(string serializerMode, dynamic message, IFormatter formatter)
        {
            byte[] buffer;

            switch ((SerializedMode)Enum.Parse(typeof(SerializedMode), serializerMode))
            {
                case SerializedMode.None:
                    buffer = message as byte[];
                    break;

                case SerializedMode.String:
                    buffer = SerDe.ToBytes(message as string);
                    break;

                case SerializedMode.Row:
                    var pickler = new Pickler();
                    buffer = pickler.dumps(new ArrayList { message });
                    break;

                default:
                    try
                    {
                        var ms = new MemoryStream();
                        formatter.Serialize(ms, message);
                        buffer = ms.ToArray();
                    }
                    catch (Exception ex)
                    {
                        logger.LogError("Exception serializing output: " + ex);
                        logger.LogError("{0} : {1}", message.GetType().Name, message.GetType().FullName);
                        throw;
                    }
                    break;
            }

            return buffer;
        }

        private void WriteDiagnosticsInfo(Stream networkStream, DateTime bootTime, DateTime initTime)
        {
            DateTime finishTime = DateTime.UtcNow;
            const string format = "MM/dd/yyyy hh:mm:ss.fff tt";

            logger.LogDebug("bootTime: {0}, initTime: {1}, finish_time: {2}",
                bootTime.ToString(format), initTime.ToString(format), finishTime.ToString(format));

            SerDe.Write(networkStream, (int)SpecialLengths.TIMING_DATA);
            SerDe.Write(networkStream, ToUnixTime(bootTime));
            SerDe.Write(networkStream, ToUnixTime(initTime));
            SerDe.Write(networkStream, ToUnixTime(finishTime));

            SerDe.Write(networkStream, 0L); //shuffle.MemoryBytesSpilled  
            SerDe.Write(networkStream, 0L); //shuffle.DiskBytesSpilled
        }

        private long ToUnixTime(DateTime dt)
        {
            return (long)(dt - UnixTimeEpoch).TotalMilliseconds;
        }

        private IEnumerable<dynamic> GetIterator(Stream inputStream, string serializedMode, int isFuncSqlUdf)
        {
            logger.LogInfo("Serialized mode in GetIterator: " + serializedMode);           
            IFormatter formatter = new BinaryFormatter();
            var mode = (SerializedMode)Enum.Parse(typeof(SerializedMode), serializedMode);
            int messageLength;
            Stopwatch watch = Stopwatch.StartNew();
            Row tempRow = null;

            while ((messageLength = SerDe.ReadInt(inputStream)) != (int)SpecialLengths.END_OF_DATA_SECTION)
            {
                watch.Stop();
                if (messageLength > 0 || messageLength == (int)SpecialLengths.NULL)
                {
                    watch.Start();
                    byte[] buffer = messageLength > 0 ? SerDe.ReadBytes(inputStream, messageLength) : null;
                    watch.Stop();
                    switch (mode)
                    {
                        case SerializedMode.String:
                            {
                                if (messageLength > 0)
                                {
                                    if (buffer == null)
                                    {
                                        logger.LogDebug("Buffer is null. Message length is {0}", messageLength);
                                    }
                                    yield return SerDe.ToString(buffer);
                                }
                                else
                                {
                                    yield return null;
                                }
                                break;
                            }

                        case SerializedMode.Row:
                            {
                                Debug.Assert(messageLength > 0);
                                var unpickledObjects = PythonSerDe.GetUnpickledObjects(buffer);

                                if (isFuncSqlUdf == 0)
                                {
                                    foreach (var row in unpickledObjects.Select(item => (item as RowConstructor).GetRow()))
                                    {
                                        yield return row;
                                    }
                                }
                                else
                                {
                                    foreach (var row in unpickledObjects)
                                    {
                                        yield return row;
                                    }
                                }

                                break;
                            }

                        case SerializedMode.Pair:
                            {
                                byte[] pairKey = buffer;
                                byte[] pairValue;

                                watch.Start();
                                int valueLength = SerDe.ReadInt(inputStream);
                                if (valueLength > 0)
                                {
                                    pairValue = SerDe.ReadBytes(inputStream, valueLength);
                                }
                                else if (valueLength == (int)SpecialLengths.NULL)
                                {
                                    pairValue = null;
                                }
                                else
                                {
                                    throw new Exception(string.Format("unexpected valueLength: {0}", valueLength));
                                }
                                watch.Stop();

                                yield return new Tuple<byte[], byte[]>(pairKey, pairValue);
                                break;
                            }

                        case SerializedMode.None: //just return raw bytes
                            {
                                yield return buffer;
                                break;
                            }

                        default:
                            {
                                if (buffer != null)
                                {
                                    var ms = new MemoryStream(buffer);
                                    yield return formatter.Deserialize(ms);
                                }
                                else
                                {
                                    yield return null;
                                }
                                break;
                            }
                    }
                }
                watch.Start();
            }

            logger.LogInfo("total receive time: {0}", watch.ElapsedMilliseconds);
        }
    }
}
