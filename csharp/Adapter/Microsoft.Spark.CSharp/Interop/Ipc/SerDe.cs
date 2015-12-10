// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Text;
using System.IO;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// see PythonRDD.scala with which Work.cs communicates
    /// </summary>
    public enum SpecialLengths : int
    {
        END_OF_DATA_SECTION = -1,
        DOTNET_EXCEPTION_THROWN = -2,
        TIMING_DATA = -3,
        END_OF_STREAM = -4,
        NULL = -5,
    }

/// <summary>
    /// Serialization and Deserialization of data types between JVM & CLR
    /// </summary>
    public class SerDe //TODO - add ToBytes() for other types
    {
        public static long totalReadNum = 0;
        public static long totalWriteNum = 0;

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
            return //Netty byte order is BigEndian
                (int)value[3] |
                (int)value[2] << 8 |
                (int)value[1] << 16 |
                (int)value[0] << 24;
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

        public static int ReadInt(Stream s)
        {
            return ToInt(ReadBytes(s, 4));
        } 
        
        public static long ReadLong(Stream s)
        {
            byte[] buffer = ReadBytes(s, 8);
            return //Netty byte order is BigEndian
                (long)buffer[7] |
                (long)buffer[6] << 8 |
                (long)buffer[5] << 16 |
                (long)buffer[4] << 24 |
                (long)buffer[3] << 32 |
                (long)buffer[2] << 40 |
                (long)buffer[1] << 48 |
                (long)buffer[0] << 56;
        }
        
        public static double ReadDouble(Stream s)
        {
            byte[] buffer = ReadBytes(s, 8);
            Array.Reverse(buffer); //Netty byte order is BigEndian
            return BitConverter.ToDouble(buffer, 0);
        }
        
        public static string ReadString(Stream s)
        {
            return ToString(ReadBytes(s));
        }
        
        public static byte[] ReadBytes(Stream s, int length)
        {
            if (length < 0)
            {
                throw new ArgumentOutOfRangeException("length", length, "length can't be negative.");
            }

            byte[] buffer = new byte[length];
            if (length > 0)
            {
                int bytesRead;
                int totalBytesRead = 0;
                do
                {
                    bytesRead = s.Read(buffer, totalBytesRead, length - totalBytesRead);
                    totalBytesRead += bytesRead;
                }
                while (totalBytesRead < length && bytesRead > 0);

                totalReadNum += totalBytesRead;

                // stream is closed, return null to notify function caller
                if (totalBytesRead == 0)
                    return null;

                if (totalBytesRead < length)
                    throw new ArgumentException(string.Format("Incomplete bytes read: {0}, expected: {1}", totalBytesRead, length));
            }

            return buffer;
        }
        
        public static byte[] ReadBytes(Stream s)
        {
            var lengthBuffer = ReadBytes(s, 4);
            if (lengthBuffer == null)
                return null;

            var length = ToInt(lengthBuffer);
            if (length == (int)SpecialLengths.NULL)
                return null;

            return ReadBytes(s, length);
        }
        
        public static string ReadObjectId(Stream s)
        {
            var type = s.ReadByte();

            if (type == 'n')
                return null;
            
            if (type != 'j')
            {
                Console.WriteLine("Expecting java object identifier type");
                return null;
            }

            return ReadString(s);
        }
        
        public static void Write(Stream s, byte value)
        {
            s.WriteByte(value);
            totalWriteNum += 1;
        }

        public static void Write(Stream s, byte[] value)
        {
            s.Write(value, 0, value.Length);
            totalWriteNum += value.Length;
        }

        public static void Write(Stream s, int value)
        {
            Write(s, new byte[] { 
                (byte)(value >> 24),
                (byte)(value >> 16), 
                (byte)(value >> 8), 
                (byte)value
            });
        }

        public static void Write(Stream s, long value)
        {
            Write(s, new byte[] { 
                (byte)(value >> 56),
                (byte)(value >> 48), 
                (byte)(value >> 40), 
                (byte)(value >> 32), 
                (byte)(value >> 24), 
                (byte)(value >> 16), 
                (byte)(value >> 8), 
                (byte)value,
            });
        }

        public static void Write(Stream s, double value)
        {
            byte[] buffer = BitConverter.GetBytes(value);
            Array.Reverse(buffer);
            Write(s, buffer);
        }

        public static void Write(Stream s, string value)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(value);
            Write(s, buffer.Length);
            Write(s, buffer);
        }
    }
}
