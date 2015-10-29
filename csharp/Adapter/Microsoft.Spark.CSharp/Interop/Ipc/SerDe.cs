// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Text;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Serialization and Deserialization of data types between JVM & CLR
    /// </summary>
    public class SerDe //TODO - add ToBytes() for other types
    {
        public static byte[] ToBytes(bool value)
        {
            return new[] { System.Convert.ToByte(value) };
        }

        public static byte[] ToBytes(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        public static byte[] ToBytes(int value)
        {
            var byteRepresentationofInputLength = BitConverter.GetBytes(value);
            Array.Reverse(byteRepresentationofInputLength);
            return byteRepresentationofInputLength;
        }
        public static byte[] ToBytes(long value)
        {
            var byteRepresentationofInputLength = BitConverter.GetBytes(value);
            Array.Reverse(byteRepresentationofInputLength);
            return byteRepresentationofInputLength;
        }

        public static byte[] ToBytes(double value)
        {
            var byteRepresentationofInputLength = BitConverter.GetBytes(value);
            Array.Reverse(byteRepresentationofInputLength);
            return byteRepresentationofInputLength;
        }

        public static char ToChar(byte value)
        {
            return System.Convert.ToChar(value);
        }

        public static string ToString(byte[] value)
        {
            return Encoding.UTF8.GetString(value);
        }

        public static int ToInt(byte[] value)
        {
            return BitConverter.ToInt32(value, 0);
        }

        public static int Convert(int value)
        {
            var buffer = BitConverter.GetBytes(value);
            Array.Reverse(buffer); //Netty byte order is BigEndian
            return BitConverter.ToInt32(buffer, 0);
        }

        public static long Convert(long value)
        {
            var buffer = BitConverter.GetBytes(value);
            Array.Reverse(buffer); //Netty byte order is BigEndian
            return BitConverter.ToInt64(buffer, 0);
        }

        public static double Convert(double value)
        {
            var buffer = BitConverter.GetBytes(value);
            Array.Reverse(buffer); //Netty byte order is BigEndian
            return BitConverter.ToDouble(buffer, 0);
        }
    }
}
