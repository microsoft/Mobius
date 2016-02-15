// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validates the IPC payload built by PayloadHelper for JVM calls from CLR
    /// </summary>
    [TestFixture]
    class PayloadHelperTest
    {
        [Test]
        public void Test()
        {
            const string expectedClassName = "class1";
            const string expectedMethod = "method1";
            object[] expectedParams =
            {
                1, // int
                int.MinValue, // int
                2L, // long
                long.MinValue, // long
                3D, // double
                double.MinValue, // double
                "holo", // string
                true,  // bool
                new JvmObjectReference(Guid.NewGuid().ToString()), // jvm object reference
                new byte[] { 1, 2 }, // byte[]
                new int[] {100, 200}, // int[]
                new int[] {-100, -200}, // int[]
                new long[] {300L, 500L}, // long[]
                new long[] {-300L, -500L}, // long[]
                new double[] {99d, 1000d}, // double[]
                new double[] {-99d, -1000d}, // double[]
                new List<byte[]>() {SerDe.ToBytes(100), SerDe.ToBytes(200)}, // IEnumerable<byte[]>
                new string[] {"Hello", "World!"}, // IEnumerable<string>
                new JvmObjectReference[]  // IEnumerable<JvmObjectReference>
                {
                    new JvmObjectReference(Guid.NewGuid().ToString()), 
                    new JvmObjectReference(Guid.NewGuid().ToString())
                },

                new Dictionary<string, string>
                {
                    { "test", "test" },
                    { "test2", "test2" }
                },
                null
            };
            TestBuildPayload(expectedClassName, expectedMethod, expectedParams, true);
        }

        [Test]
        public void TestWithUnsupportedType()
        {
            const string expectedClassName = "class1";
            const string expectedMethod = "method1";
            object[] expectedParams =
            {
                new object[] { SerDe.ToBytes(int.MaxValue), SerDe.ToBytes(int.MinValue) }
            };
            Assert.Throws<NotSupportedException>(() => TestBuildPayload(expectedClassName, expectedMethod, expectedParams, true));
        }

        public void TestBuildPayload(string expectedClassName, string expectedMethod, object[] expectedParams, bool expectedMethodType)
        {
            var payloadBytes = PayloadHelper.BuildPayload(expectedMethodType, expectedClassName, expectedMethod, expectedParams);
            Assert.IsNotNull(payloadBytes);
            Assert.IsTrue(payloadBytes.Length > 0);

            bool isStatic;
            int payloadBytesLength;
            string className;
            string methodName;
            object[] parameters;
            ParsePayload(payloadBytes, out payloadBytesLength, out isStatic, out className, out methodName, out parameters);

            Assert.AreEqual(payloadBytesLength + 4, payloadBytes.Length);
            Assert.AreEqual(expectedMethodType, isStatic);
            Assert.AreEqual(expectedClassName, className);
            Assert.AreEqual(expectedMethod, methodName);
            Assert.IsNotNull(parameters);
            Assert.AreEqual(expectedParams, parameters);
        }


        internal void ParsePayload(byte[] payloadBytes, out int payloadBytesLength, out bool isStatic, out string className,
            out string methodName, out object[] parameters)
        {
            var stream = new MemoryStream(payloadBytes);
            payloadBytesLength = SerDe.ReadInt(stream);
            isStatic = ReadBoolean(stream);
            className = SerDe.ReadString(stream);
            methodName = SerDe.ReadString(stream);
            var parameterCount = SerDe.ReadInt(stream);
            parameters = new object[parameterCount];
            for (var i = 0; i < parameterCount; i++)
            {
                parameters[i] = ReadObject(stream);
            }
        }

        internal static bool ReadBoolean(Stream s)
        {
            byte[] buffer = SerDe.ReadBytes(s, 1);
            return BitConverter.ToBoolean(buffer, 0);
        }

        internal static Dictionary<object, object> ReadDictionary(Stream s)
        {
            Dictionary<object, object> dict = new Dictionary<object, object>();
            var len = SerDe.ReadInt(s);
            if (len == 0)
            {
                return dict;
            }

            var keysType = ReadObjectType(s);
            var keysLen = SerDe.ReadInt(s);
            var keys = new object[keysLen];
            for (var i = 0; i < keys.Length; i++)
            {
                keys[i] = ReadTypedObject(s, keysType);
            }

            var valueLen = SerDe.ReadInt(s);
            var values = new object[valueLen];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = ReadTypedObject(s, ReadObjectType(s));
            }

            for (var i = 0; i < keys.Length; i++)
            {
                dict[keys[i]] = values[i];
            }

            return dict;
        }

        internal static char ReadObjectType(Stream s)
        {
            return SerDe.ToChar(SerDe.ReadBytes(s, 1)[0]);
        }

        internal static object ReadArray(Stream s)
        {
            var type = ReadObjectType(s);
            switch (type)
            {
                case 'i':
                    return ReadIntArray(s);

                case 'g':
                    return ReadLongArray(s);

                case 'c':
                    return ReadStringArray(s);

                case 'd':
                    return ReadDoubleArray(s);

                case 'b':
                    return ReadBooleanArray(s);

                case 'j':
                    return ReadJvmObjectReferenceArray(s);

                case 'r':
                    return ReadByteList(s);

                default:
                    throw new ArgumentException("Invalid type " + type);
            }
        }

        internal static List<byte[]> ReadByteList(Stream s)
        {
            var count = SerDe.ReadInt(s);
            var a = new List<byte[]>(count);

            for (var i = 0; i < count; i++)
            {
                a.Add(SerDe.ReadBytes(s));
            }

            return a;
        }

        internal static int[] ReadIntArray(Stream s)
        {
            var a = new int[SerDe.ReadInt(s)];
            for (var i = 0; i < a.Length; i++)
            {
                a[i] = SerDe.ReadInt(s);
            }

            return a;
        }

        internal static long[] ReadLongArray(Stream s)
        {
            var a = new long[SerDe.ReadInt(s)];
            for (var i = 0; i < a.Length; i++)
            {
                a[i] = SerDe.ReadLong(s);
            }

            return a;
        }

        internal static string[] ReadStringArray(Stream s)
        {
            var a = new string[SerDe.ReadInt(s)];
            for (var i = 0; i < a.Length; i++)
            {
                a[i] = SerDe.ReadString(s);
            }

            return a;
        }

        internal static JvmObjectReference[] ReadJvmObjectReferenceArray(Stream s)
        {
            var a = new JvmObjectReference[SerDe.ReadInt(s)];
            for (var i = 0; i < a.Length; i++)
            {
                a[i] = new JvmObjectReference(SerDe.ReadString(s));
            }

            return a;
        }

        internal static double[] ReadDoubleArray(Stream s)
        {
            var a = new double[SerDe.ReadInt(s)];
            for (var i = 0; i < a.Length; i++)
            {
                a[i] = SerDe.ReadDouble(s);
            }

            return a;
        }

        internal static bool[] ReadBooleanArray(Stream s)
        {
            var a = new bool[SerDe.ReadInt(s)];
            for (var i = 0; i < a.Length; i++)
            {
                a[i] = ReadBoolean(s);
            }

            return a;
        }

        internal static DateTime ReadDate(Stream s)
        {
            // TODO
            return default(DateTime);
        }

        internal static DateTime ReadTime(Stream s)
        {
            // TODO
            return default(DateTime);
        }

        internal static object ReadObject(Stream s)
        {
            char type = ReadObjectType(s);
            return ReadTypedObject(s, type);
        }

        internal static object ReadTypedObject(Stream s, char type)
        {
            switch (type)
            {
                case 'n':
                    return null;

                case 'i':
                    return SerDe.ReadInt(s);

                case 'g':
                    return SerDe.ReadLong(s);

                case 'd':
                    return SerDe.ReadDouble(s);

                case 'b':
                    return ReadBoolean(s);

                case 'c':
                    return SerDe.ReadString(s);

                case 'e':
                    return ReadDictionary(s);

                case 'r':
                    return SerDe.ReadBytes(s);

                case 'l':
                    return ReadArray(s);

                case 'D':
                    return ReadDate(s);

                case 't':
                    return ReadTime(s);

                case 'j':
                    return new JvmObjectReference(SerDe.ReadString(s));

                default:
                    throw new ArgumentException("Invalid type " + type);
            }
        }
    }
}
