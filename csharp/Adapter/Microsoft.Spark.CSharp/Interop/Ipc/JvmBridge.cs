// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Implementation of thread safe IPC bridge between JVM & CLR
    /// throught a concourrent socket connection queue (lightweight synchronisation mechanism)
    /// supporting async JVM calls like StreamingContext.AwaitTermination()
    /// </summary>
    internal class JvmBridge : IJvmBridge
    {
        private int portNumber;
        private ConcurrentQueue<Socket> sockets = new ConcurrentQueue<Socket>();
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(JvmBridge));

        public void Initialize(int portNumber)
        {
            this.portNumber = portNumber;
        }

        private Socket GetConnection()
        {
            Socket socket;
            if (!sockets.TryDequeue(out socket))
            {
                socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                socket.Connect(IPAddress.Parse("127.0.0.1"), portNumber);
            }
            return socket;
        }

        public JvmObjectReference CallConstructor(string className, params object[] parameters)
        {
            return new JvmObjectReference(CallJavaMethod(true, className, "<init>", parameters).ToString());
        }

        public object CallStaticJavaMethod(string className, string methodName, params object[] parameters)
        {
            return CallJavaMethod(true, className, methodName, parameters);
        }

        public object CallStaticJavaMethod(string className, string methodName)
        {
            return CallJavaMethod(true, className, methodName, new object[] { });
        }

        public object CallNonStaticJavaMethod(JvmObjectReference objectId, string methodName, params object[] parameters)
        {
            return CallJavaMethod(false, objectId, methodName, parameters);
        }

        public object CallNonStaticJavaMethod(JvmObjectReference objectId, string methodName)
        {
            return CallJavaMethod(false, objectId, methodName, new object[] { });
        }

        private object CallJavaMethod(bool isStatic, object classNameOrJvmObjectReference, string methodName, params object[] parameters)
        {
            object returnValue = null;
            try
            {
                var overallPayload = PayloadHelper.BuildPayload(isStatic, classNameOrJvmObjectReference, methodName, parameters);

                Socket socket = GetConnection();
                using (NetworkStream s = new NetworkStream(socket))
                {
                    SerDe.Write(s, overallPayload);

                    var isMethodCallFailed = SerDe.ReadInt(s);
                    //TODO - add boolean instead of int in the backend
                    if (isMethodCallFailed != 0)
                    {
                        var jvmFullStackTrace = SerDe.ReadString(s);
                        string errorMessage = string.Format("JVM method execution failed: {0}", methodName);
                        logger.LogError(errorMessage);
                        logger.LogError(jvmFullStackTrace);
                        throw new Exception(errorMessage);
                    }

                    var typeAsChar = Convert.ToChar(s.ReadByte());
                    switch (typeAsChar) //TODO - add support for other types
                    {
                        case 'n':
                            break;

                        case 'j':
                            returnValue = SerDe.ReadString(s);
                            break;

                        case 'c':
                            returnValue = SerDe.ReadString(s);
                            break;

                        case 'i':
                            returnValue = SerDe.ReadInt(s);
                            break;

                        case 'd':
                            returnValue = SerDe.ReadDouble(s);
                            break;

                        case 'b':
                            returnValue = Convert.ToBoolean(s.ReadByte());
                            break;

                        case 'l':
                            returnValue = ReadJvmObjectReferenceCollection(s);

                            break;

                        default:
                            // convert typeAsChar to UInt32 because the char may be non-printable
                            throw new NotSupportedException(string.Format("Identifier for type 0x{0:X} not supported", Convert.ToUInt32(typeAsChar)));

                    }
                }
                sockets.Enqueue(socket);
            }
            catch (Exception e)
            {
                logger.LogException(e);
                throw;
            }

            return returnValue;

        }

        private object ReadJvmObjectReferenceCollection(NetworkStream s)
        {
            object returnValue;
            var listItemTypeAsChar = Convert.ToChar(s.ReadByte());
            switch (listItemTypeAsChar)
            {
                case 'j':
                    var jvmObjectReferenceList = new List<JvmObjectReference>();
                    var numOfItemsInList = SerDe.ReadInt(s);
                    for (int itemIndex = 0; itemIndex < numOfItemsInList; itemIndex++)
                    {
                        var itemIdentifier = SerDe.ReadString(s);
                        jvmObjectReferenceList.Add(new JvmObjectReference(itemIdentifier));
                    }
                    returnValue = jvmObjectReferenceList;
                    break;

                default:
                    // convert listItemTypeAsChar to UInt32 because the char may be non-printable
                    throw new NotSupportedException(
                        string.Format("Identifier for list item type 0x{0:X} not supported",
                            Convert.ToUInt32(listItemTypeAsChar)));
            }
            return returnValue;
        }

        public void Dispose()
        {
            Socket socket;
            while (sockets.TryDequeue(out socket))
            {
                if (socket != null)
                {
                    socket.Dispose();
                    socket = null;
                }
            }
        }
    }
}
