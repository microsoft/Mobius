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
        /// <summary>
        /// Flag to indicate the end of data section
        /// </summary>
        END_OF_DATA_SECTION = -1,

        /// <summary>
        /// Flag to indicate an exception thrown from .NET side
        /// </summary>
        DOTNET_EXCEPTION_THROWN = -2,

        /// <summary>
        /// Flag to indicate a timing data
        /// </summary>
        TIMING_DATA = -3,

        /// <summary>
        /// Flag to indicate the end of stream
        /// </summary>
        END_OF_STREAM = -4,

        /// <summary>
        /// Flag to indicate non-defined type
        /// </summary>
        NULL = -5,
    }

/// <summary>
    /// Serialization and Deserialization of data types between JVM and CLR
    /// </summary>
    public class SerDe //TODO - add ToBytes() for other types
    {
        /// <summary>
        /// The total number of read
        /// </summary>
        public static long totalReadNum = 0;

        /// <summary>
        /// The total number of write
        /// </summary>
        public static long totalWriteNum = 0;

        /// <summary>
        /// Converts a boolean to a byte array
        /// </summary>
        /// <param name="value">The boolean to be converted</param>
        /// <returns>The byte array converted from a boolean</returns>
        public static byte[] ToBytes(bool value)
        {
            return new[] { System.Convert.ToByte(value) };
        }
        
        /// <summary>
        /// Converts a string to a byte array.
        /// </summary>
        /// <param name="value">The string to be converted</param>
        /// <returns>The byte array converted from a string</returns>
        public static byte[] ToBytes(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }

        /// <summary>
        /// Converts an integer to a byte array
        /// </summary>
        /// <param name="value">The intger to be converted</param>
        /// <returns>The byte array converted from an integer</returns>
        public static byte[] ToBytes(int value)
        {
            var byteRepresentationofInputLength = BitConverter.GetBytes(value);
            Array.Reverse(byteRepresentationofInputLength);
            return byteRepresentationofInputLength;
        }

        /// <summary>
        /// Converts a long integer to a byte array
        /// </summary>
        /// <param name="value">The long intger to be converted</param>
        /// <returns>The byte array converted from a long integer</returns>
        public static byte[] ToBytes(long value)
        {
            var byteRepresentationofInputLength = BitConverter.GetBytes(value);
            Array.Reverse(byteRepresentationofInputLength);
            return byteRepresentationofInputLength;
        }

        /// <summary>
        /// Converts a double to a byte array
        /// </summary>
        /// <param name="value">The double to be converted</param>
        /// <returns>The byte array converted from a double</returns>
        public static byte[] ToBytes(double value)
        {
            var byteRepresentationofInputLength = BitConverter.GetBytes(value);
            Array.Reverse(byteRepresentationofInputLength);
            return byteRepresentationofInputLength;
        }

        /// <summary>
        /// Converts a byte to a character
        /// </summary>
        /// <param name="value">The byte to be converted</param>
        /// <returns>The char converted from a byte</returns>
        public static char ToChar(byte value)
        {
            return System.Convert.ToChar(value);
        }

        /// <summary>
        /// Converts a byte array to a string
        /// </summary>
        /// <param name="value">The byte array to be converted</param>
        /// <returns>The string converted from a byte array</returns>
        public static string ToString(byte[] value)
        {
            return Encoding.UTF8.GetString(value);
        }

        /// <summary>
        /// Converts a byte array to an integer
        /// </summary>
        /// <param name="value">The byte array to be converted</param>
        /// <returns>The integer converted from a byte array</returns>
        public static int ToInt(byte[] value)
        {
            return //Netty byte order is BigEndian
                (int)value[3] |
                (int)value[2] << 8 |
                (int)value[1] << 16 |
                (int)value[0] << 24;
        } 

        /// <summary>
        /// Reads an integer from a stream
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The integer read from stream</returns>
        public static int ReadInt(Stream s)
        {
            return ToInt(ReadBytes(s, 4));
        }

        /// <summary>
        /// Reads a long integer from a stream
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The long integer read from stream</returns>
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

        /// <summary>
        /// Reads a double from a stream
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The double read from stream</returns>
        public static double ReadDouble(Stream s)
        {
            byte[] buffer = ReadBytes(s, 8);
            Array.Reverse(buffer); //Netty byte order is BigEndian
            return BitConverter.ToDouble(buffer, 0);
        }
        
        /// <summary>
        /// Reads a string from a stream
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The string read from stream</returns>
        public static string ReadString(Stream s)
        {
            return ToString(ReadBytes(s));
        }

        /// <summary>
        /// Reads a byte array with a given length from a stream
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <param name="length">The length to be read</param>
        /// <returns>The a byte array read from stream</returns>
        /// <exception cref="ArgumentOutOfRangeException">An ArgumentOutOfRangeException thrown if the given length is negative</exception>
        /// <exception cref="ArgumentException">An ArgumentException if the actual read length is less than the given length</exception>
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
        
        /// <summary>
        /// Reads a byte array from a stream. The first 4 bytes indicate the length of a byte array.
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The byte array read from stream</returns>
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
        
        /// <summary>
        /// Read an object Id from a stream.
        /// </summary>
        /// <param name="s">The stream to be read</param>
        /// <returns>The object Id read from stream</returns>
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
        
        /// <summary>
        /// Writes a byte to a stream
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The byte to write</param>
        public static void Write(Stream s, byte value)
        {
            s.WriteByte(value);
            totalWriteNum += 1;
        }

        /// <summary>
        /// Writes a byte array to a stream
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The byte array to write</param>
        public static void Write(Stream s, byte[] value)
        {
            s.Write(value, 0, value.Length);
            totalWriteNum += value.Length;
        }

        /// <summary>
        /// Writes an integer to a stream
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The integer to write</param>
        public static void Write(Stream s, int value)
        {
            Write(s, new byte[] { 
                (byte)(value >> 24),
                (byte)(value >> 16), 
                (byte)(value >> 8), 
                (byte)value
            });
        }

        /// <summary>
        /// Writes a long integer to a stream
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The long integer to write</param>
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

        /// <summary>
        /// Writes a double to a stream
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The double to write</param>
        public static void Write(Stream s, double value)
        {
            byte[] buffer = BitConverter.GetBytes(value);
            Array.Reverse(buffer);
            Write(s, buffer);
        }

        /// <summary>
        /// Writes a string to a stream
        /// </summary>
        /// <param name="s">The stream to write</param>
        /// <param name="value">The string to write</param>
        public static void Write(Stream s, string value)
        {
            byte[] buffer = Encoding.UTF8.GetBytes(value);
            Write(s, buffer.Length);
            Write(s, buffer);
        }
    }
}
