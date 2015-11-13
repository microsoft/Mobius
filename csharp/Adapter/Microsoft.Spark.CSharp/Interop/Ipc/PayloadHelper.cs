// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Help build the IPC payload for JVM calls from CLR
    /// </summary>
    internal class PayloadHelper
    {
        internal static byte[] BuildPayload(bool isStaticMethod, object classNameOrJvmObjectReference, string methodName, object[] parameters)
        {
            var isStaticMethodAsBytes = SerDe.ToBytes(isStaticMethod);
            var objectOrClassIdBytes = ToPayloadBytes(classNameOrJvmObjectReference.ToString()); //class name or objectId sent as string
            var methodNameBytes = ToPayloadBytes(methodName);
            var parameterCountBytes = SerDe.ToBytes(parameters.Length);

            var parametersBytes = ConvertParametersToBytes(parameters);

            var payloadBytes = new List<byte[]>
            {
                isStaticMethodAsBytes,
                objectOrClassIdBytes,
                methodNameBytes,
                parameterCountBytes,
                parametersBytes
            };

            var payloadLength = GetPayloadLength(payloadBytes);
            var payload = GetPayload(payloadBytes);
            var payloadLengthBytes = SerDe.ToBytes(payloadLength);

            var headerAndPayloadBytes = new byte[payloadLengthBytes.Length + payload.Length];
            Array.Copy(payloadLengthBytes, 0, headerAndPayloadBytes, 0, payloadLengthBytes.Length);
            Array.Copy(payload, 0, headerAndPayloadBytes, payloadLengthBytes.Length, payload.Length);

            return headerAndPayloadBytes;
        }

        internal static byte[] ToPayloadBytes(string value)
        {
            var inputAsBytes = SerDe.ToBytes(value);
            var byteRepresentationofInputLength = SerDe.ToBytes(inputAsBytes.Length);
            var sendPayloadBytes = new byte[byteRepresentationofInputLength.Length + inputAsBytes.Length];

            Array.Copy(byteRepresentationofInputLength, 0, sendPayloadBytes, 0, byteRepresentationofInputLength.Length);
            Array.Copy(inputAsBytes, 0, sendPayloadBytes, byteRepresentationofInputLength.Length, inputAsBytes.Length);

            return sendPayloadBytes;
        }

        internal static int GetPayloadLength(List<byte[]> payloadBytesList)
        {
            return payloadBytesList.Sum(payloadBytes => payloadBytes.Length);
        }

        internal static byte[] GetPayload(List<byte[]> payloadBytesList)
        {
            return payloadBytesList.SelectMany(byteArray => byteArray).ToArray();
        }

        internal static byte[] ConvertParametersToBytes(object[] parameters, bool addTypeIdPrefix = true)
        {
            var paramtersBytes = new List<byte[]>();
            foreach (var parameter in parameters)
            {
                if (parameter != null)
                {
                    if (addTypeIdPrefix) paramtersBytes.Add(GetTypeId(parameter.GetType()));

                    if (parameter is int)
                    {
                        paramtersBytes.Add(SerDe.ToBytes((int)parameter));
                    }
                    else if (parameter is long)
                    {
                        paramtersBytes.Add(SerDe.ToBytes((long)parameter));
                    }
                    else if (parameter is string)
                    {
                        paramtersBytes.Add(ToPayloadBytes(parameter.ToString()));
                    }
                    else if (parameter is bool)
                    {
                        paramtersBytes.Add(SerDe.ToBytes((bool)parameter));
                    }
                    else if (parameter is double)
                    {
                        paramtersBytes.Add(SerDe.ToBytes((double)parameter));
                    }
                    else if (parameter is byte[])
                    {
                        paramtersBytes.Add(SerDe.ToBytes(((byte[])parameter).Length));
                        paramtersBytes.Add((byte[])parameter);
                    }
                    else if (parameter is int[])
                    {
                        paramtersBytes.Add(GetTypeId(typeof(int)));
                        paramtersBytes.Add(SerDe.ToBytes(((int[])parameter).Length));
                        paramtersBytes.AddRange(((int[])parameter).Select(x => SerDe.ToBytes(x)));
                    }
                    else if (parameter is long[])
                    {
                        paramtersBytes.Add(GetTypeId(typeof(long)));
                        paramtersBytes.Add(SerDe.ToBytes(((long[])parameter).Length));
                        paramtersBytes.AddRange(((long[])parameter).Select(x => SerDe.ToBytes(x)));
                    }
                    else if (parameter is double[])
                    {
                        paramtersBytes.Add(GetTypeId(typeof(double)));
                        paramtersBytes.Add(SerDe.ToBytes(((double[])parameter).Length));
                        paramtersBytes.AddRange(((double[])parameter).Select(x => SerDe.ToBytes(x)));
                    }
                    else if (parameter is IEnumerable<byte[]>)
                    {
                        paramtersBytes.Add(GetTypeId(typeof(byte[])));
                        paramtersBytes.Add(SerDe.ToBytes(((IEnumerable<byte[]>)parameter).Count())); //TODO - Count() will traverse the collection - change interface?
                        foreach (var byteArray in (IEnumerable<byte[]>)parameter)
                        {
                            paramtersBytes.Add(SerDe.ToBytes(byteArray.Length));
                            paramtersBytes.Add(byteArray);
                        }
                    }
                    else if (parameter is IEnumerable<string>)
                    {
                        paramtersBytes.Add(GetTypeId(typeof(string)));
                        paramtersBytes.Add(SerDe.ToBytes(((IEnumerable<string>)parameter).Count())); //TODO - Count() will traverse the collection - change interface?
                        paramtersBytes.AddRange(from stringVal in (IEnumerable<string>)parameter select ToPayloadBytes(stringVal));
                    }
                    else if (parameter is IEnumerable<JvmObjectReference>)
                    {
                        paramtersBytes.Add(GetTypeId(typeof(JvmObjectReference)));
                        paramtersBytes.Add(SerDe.ToBytes(((IEnumerable<JvmObjectReference>)parameter).Count())); //TODO - Count() will traverse the collection - change interface?
                        paramtersBytes.AddRange(from jObj in (IEnumerable<JvmObjectReference>)parameter select ToPayloadBytes(jObj.Id));
                    }
                    else if (IsDictionary(parameter.GetType()))
                    {
                        // Generic Dictionary 'parameter' passed into this function is a object which lost its Generic Type T (namely Dictionary<T, T>). 
                        // Cannot neither cast to Dictionary<T, T> nor Dictionary<dynamic, dynamic>, so we need to first cast to Non-Generic interface IDictionary and then
                        // rebuild a Dictionary<dynamic, dynamic>. 
                        var nonGenericDict = (IDictionary)parameter;
                        var dict = new Dictionary<dynamic, dynamic>();
                        foreach (var k in nonGenericDict.Keys)
                        {
                            dict[k] = nonGenericDict[k];
                        }

                        Type keyType = parameter.GetType().GetGenericArguments()[0];

                        // Below serialization is coressponding to deserialization method ReadMap() of SerDe.scala
                        paramtersBytes.Add(SerDe.ToBytes(dict.Count())); // dictionary's length
                        paramtersBytes.Add(GetTypeId(keyType)); // keys' data type
                        paramtersBytes.Add(SerDe.ToBytes(dict.Count())); // keys' length, same as dictionary's length
                        paramtersBytes.AddRange(from kv in dict select ConvertParametersToBytes(new object[] { kv.Key }, false)); // keys, do not need type prefix
                        paramtersBytes.Add(SerDe.ToBytes(dict.Count())); // values' length, same as dictionary's length
                        paramtersBytes.AddRange(from kv in dict select ConvertParametersToBytes(new object[] { kv.Value })); // values, need type prefix
                    }
                    else if (parameter is JvmObjectReference)
                    {
                        paramtersBytes.Add(ToPayloadBytes((parameter as JvmObjectReference).Id));
                    }
                    else
                    {
                        throw new NotSupportedException(string.Format("Type {0} is not supported", parameter.GetType()));
                    }
                }
                else
                {
                    paramtersBytes.Add(new [] { Convert.ToByte('n') });
                }

            }

            return paramtersBytes.SelectMany(byteArray => byteArray).ToArray();
        }

        internal static byte[] GetTypeId(Type type) //TODO - support other types
        {
            if (type == typeof(int))
            {
                return new [] { Convert.ToByte('i') };
            }

            if (type == typeof(long))
            {
                return new[] { Convert.ToByte('g') };
            }

            if (type == typeof(string))
            {
                return new [] { Convert.ToByte('c') };
            }

            if (type == typeof(bool))
            {
                return new [] { Convert.ToByte('b') };
            }

            if (type == typeof(double))
            {
                return new[] { Convert.ToByte('d') };
            }

            if (type == typeof(JvmObjectReference))
            {
                return new [] { Convert.ToByte('j') };
            }

            if (type == typeof(byte[]))
            {
                return new [] { Convert.ToByte('r') };
            }

            if (type == typeof(int[]) || type == typeof(long[]) || type == typeof(double[]))
            {
                return new[] { Convert.ToByte('l') };
            }

            if (typeof(IEnumerable<byte[]>).IsAssignableFrom(type))
            {
                return new [] { Convert.ToByte('l') };
            }

            if (typeof(IEnumerable<string>).IsAssignableFrom(type))
            {
                return new [] { Convert.ToByte('l') };
            }

            if (IsDictionary(type))
            {
                return new [] { Convert.ToByte('e') };
            }

            if (typeof(IEnumerable<JvmObjectReference>).IsAssignableFrom(type))
            {
                return new [] { Convert.ToByte('l') };
            }

            throw new NotSupportedException(string.Format("Type {0} not supported yet", type));
        }

        private static bool IsDictionary(Type type)
        {
            return type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>));
        }
    }
}
