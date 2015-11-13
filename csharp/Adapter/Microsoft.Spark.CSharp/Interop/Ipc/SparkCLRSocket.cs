// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Wraps the socket implementation used by SparkCLR
    /// </summary>
    public class SparkCLRSocket : ISparkCLRSocket
    {
        private Socket socket;
        private SparkCLRSocketStream stream;

        public void Initialize(int portNumber)
        {
            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            var addresses = Dns.GetHostAddresses("localhost");
            socket.Connect(new IPEndPoint(addresses.First(a => a.AddressFamily == AddressFamily.InterNetwork) /*get IPv4 address*/, portNumber));
        }

        public IDisposable InitializeStream()
        {
            stream = new SparkCLRSocketStream(socket);
            return stream;
        }

        public void Write(byte[] value)
        {
            stream.Writer.Write(value);
        }

        public void Write(int value)
        {
            stream.Writer.Write(SerDe.Convert(value));   
        }

        public void Write(long value)
        {
            stream.Writer.Write(SerDe.Convert(value));
        }

        public void Write(string value)
        {
            byte[] buffer = SerDe.ToBytes(value);
            Write(buffer.Length);
            Write(buffer);
        }

        public byte[] ReadBytes(int length)
        {
            return stream.Reader.ReadBytes(length);
        }

        public char ReadChar()
        {
            return SerDe.ToChar(stream.Reader.ReadByte());
        }

        public int ReadInt()
        {
            return SerDe.Convert(stream.Reader.ReadInt32());
        }

        public long ReadLong()
        {
            byte[] buffer = stream.Reader.ReadBytes(8);
            Array.Reverse(buffer);
            return BitConverter.ToInt64(buffer, 0);
        }

        public string ReadString()
        {
            var length = SerDe.Convert(stream.Reader.ReadInt32());
            var stringAsBytes = stream.Reader.ReadBytes(length);
            return SerDe.ToString(stringAsBytes);
        }

        public string ReadString(int length)
        {
            var stringAsBytes = stream.Reader.ReadBytes(length);
            return SerDe.ToString(stringAsBytes);
        }

        public double ReadDouble()
        {
            return SerDe.Convert(stream.Reader.ReadDouble());
        }

        public bool ReadBoolean()
        {
            return stream.Reader.ReadBoolean();
        }

        public void Dispose()
        {
            if (socket != null)
            {
                socket.Dispose();
                socket = null;
            }
        }

        public void Flush()
        {
            stream.Stream.Flush();
        }
        private class SparkCLRSocketStream : IDisposable
        {
            internal readonly BinaryReader Reader;
            internal readonly BinaryWriter Writer;
            internal readonly NetworkStream Stream;
            private ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkCLRSocketStream));

            internal SparkCLRSocketStream(Socket socket)
            {
                Stream = new NetworkStream(socket);
                Reader = new BinaryReader(Stream);
                Writer = new BinaryWriter(Stream);
            }

            public void Dispose()
            {
                try
                {
                    Reader.Dispose();
                }
                catch (Exception e)
                {
                    logger.LogException(e);
                }

                try
                {
                    Writer.Dispose();
                }
                catch (Exception e)
                {
                    logger.LogException(e);
                }

                try
                {
                    Stream.Dispose();
                }
                catch (Exception e)
                {
                    logger.LogException(e);
                }
            }
        }
    }

    
}
