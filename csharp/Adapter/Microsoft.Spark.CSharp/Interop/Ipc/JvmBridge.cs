// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Implementation of thread safe IPC bridge between JVM & CLR
    /// throught a concourrent socket connection queue (lightweight synchronisation mechanism)
    /// supporting async JVM calls like StreamingContext.AwaitTermination()
    /// </summary>
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class JvmBridge : IJvmBridge
    {
        private int portNumber;
        private readonly ConcurrentQueue<Socket> sockets = new ConcurrentQueue<Socket>();
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
                socket.Connect(IPAddress.Loopback, portNumber);
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
                        var errorMessage = BuildErrorMessage(isStatic, classNameOrJvmObjectReference, methodName, parameters);
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

        private string BuildErrorMessage(bool isStatic, object classNameOrJvmObjectReference, string methodName, object[] parameters)
        {
            var errorMessage = new StringBuilder("JVM method execution failed: ");
            const string constructorFormat = "Constructor failed for class {0}";
            const string staticMethodFormat = "Static method {0} failed for class {1}";
            const string nonStaticMethodFormat = "Nonstatic method {0} failed for class {1}";

            try
            {
                if (isStatic)
                {
                    if (methodName.Equals("<init>")) //<init> is hardcoded in CSharpBackend
                    {
                        errorMessage.AppendFormat(constructorFormat, classNameOrJvmObjectReference);
                    }
                    else
                    {
                        errorMessage.AppendFormat(staticMethodFormat, methodName, classNameOrJvmObjectReference);
                    }
                }
                else
                {
                    errorMessage.AppendFormat(nonStaticMethodFormat, methodName, classNameOrJvmObjectReference);
                }

                if (parameters.Length == 0)
                {
                    errorMessage.Append(" when called with no parameters");
                }
                else
                {
                    errorMessage.AppendFormat(" when called with {0} parameters ({1})", parameters.Length, GetParamsAsString(parameters));
                }
            }
            catch (Exception)
            {
                errorMessage.Append("Exception when converting building error message");
            }

            return errorMessage.ToString();
        }

        private string GetParamsAsString(IEnumerable<object> parameters)
        {
            var paramsString = new StringBuilder();

            try
            {
                int index = 1;
                foreach (var parameter in parameters)
                {
                    var paramValue = "null";
                    var paramType = "null";
                    if (parameter != null)
                    {
                        paramValue = parameter.ToString();
                        paramType = parameter.GetType().Name;
                    }

                    paramsString.AppendFormat("[Index={0}, Type={1}, Value={2}], ", index++, paramType, paramValue);
                }
            }
            catch (Exception)
            {
                paramsString.Append("Exception when converting parameters to string");
            }

            return paramsString.ToString();
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
