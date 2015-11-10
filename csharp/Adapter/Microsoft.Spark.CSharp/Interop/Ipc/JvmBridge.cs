// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Implementation of IPC bridge between JVM & CLR
    /// </summary>
    internal class JvmBridge : IJvmBridge
    {
        private ISparkCLRSocket socket;
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof (JvmBridge));

        public void Initialize(int portNumber)
        {
            socket = new SparkCLRSocket();
            socket.Initialize(portNumber);
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
            return CallJavaMethod(true, className, methodName, new object[] {});
        }

        public object CallNonStaticJavaMethod(JvmObjectReference objectId, string methodName, params object[] parameters)
        {
            return CallJavaMethod(false, objectId, methodName, parameters);
        }

        public object CallNonStaticJavaMethod(JvmObjectReference objectId, string methodName)
        {
            return CallJavaMethod(false, objectId, methodName, new object[] {});
        }

        private object CallJavaMethod(bool isStatic, object classNameOrJvmObjectReference, string methodName, params object[] parameters)
        {
            object returnValue = null;
            try
            {
                var overallPayload = PayloadHelper.BuildPayload(isStatic, classNameOrJvmObjectReference, methodName, parameters);

                using (socket.InitializeStream())
                {
                    socket.Write(overallPayload);
                    
                    var isMethodCallFailed = socket.ReadInt();
                    //TODO - add boolean instead of int in the backend
                    if (isMethodCallFailed != 0)
                    {
                        var jvmFullStackTrace = socket.ReadString();
                        const string errorMessage = @"JVM method execution failed";
                        logger.LogError(errorMessage);
                        logger.LogError(jvmFullStackTrace);
                        throw new Exception(errorMessage);
                    }

                    var typeAsChar = socket.ReadChar();
                    switch (typeAsChar) //TODO - add support for other types
                    {
                        case 'n':
                            break;

                        case 'j':
                            returnValue = socket.ReadString();
                            break;

                        case 'c':
                            returnValue = socket.ReadString();
                            break;

                        case 'i':
                            returnValue = socket.ReadInt();
                            break;

                        case 'd':
                            returnValue = socket.ReadDouble();
                            break;

                        case 'b':
                            returnValue = socket.ReadBoolean();
                            break;

                        case 'l':
                            returnValue = ReadJvmObjectReferenceCollection();

                            break;

                        default:
                            throw new NotSupportedException(string.Format("Identifier for type {0} not supported", typeAsChar));

                    }
                }
            }
            catch (Exception e)
            {
                logger.LogException(e);
                throw;
            }

            return returnValue;

        }

        private object ReadJvmObjectReferenceCollection()
        {
            object returnValue;
            var listItemTypeAsChar = socket.ReadChar();
            switch (listItemTypeAsChar)
            {
                case 'j':
                    var jvmObjectReferenceList = new List<JvmObjectReference>();
                    var numOfItemsInList = socket.ReadInt();
                    for (int itemIndex = 0; itemIndex < numOfItemsInList; itemIndex++)
                    {
                        var itemIdentifier = socket.ReadString();
                        jvmObjectReferenceList.Add(new JvmObjectReference(itemIdentifier));
                    }
                    returnValue = jvmObjectReferenceList;
                    break;

                default:
                    throw new NotSupportedException(
                        string.Format("Identifier for list item type {0} not supported",
                            listItemTypeAsChar));
            }
            return returnValue;
        }

        public void Dispose()
        {
            if (socket != null)
            {
                socket.Dispose();
                socket = null;
            }
        }
    }
}
