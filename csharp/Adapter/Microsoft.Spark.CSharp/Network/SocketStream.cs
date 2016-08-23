// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Threading;

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// Provides the underlying stream of data for network access.
    /// Just like a NetworkStream.
    /// </summary>
    internal class SocketStream: Stream
    {
        private readonly ByteBufPool bufPool;
        private readonly ISocketWrapper streamSocket;
        private ByteBuf recvDataCache;
        private ByteBuf sendDataCache;

        /// <summary>
        /// Initializes a SocketStream with a SaeaSocketWrapper object.
        /// </summary>
        /// <param name="socket">a SaeaSocketWrapper object</param>
        public SocketStream(SaeaSocketWrapper socket)
        {
            if (socket == null)
            {
                throw new ArgumentNullException("socket");
            }
            streamSocket = socket;
            bufPool = ByteBufPool.Default;
        }

        /// <summary>
        /// Initializes a SocketStream with a RioSocketWrapper object.
        /// </summary>
        /// <param name="socket">a RioSocketWrapper object</param>
        public SocketStream(RioSocketWrapper socket)
        {
            if (socket == null)
            {
                throw new ArgumentNullException("socket");
            }
            streamSocket = socket;
            bufPool = ByteBufPool.UnsafeDefault;
        }

        /// <summary>
        /// Indicates that data can be read from the stream.
        /// This property always returns <see langword='true'/>
        /// </summary>
        public override bool CanRead { get { return true; } }

        /// <summary>
        /// Indicates that the stream can seek a specific location in the stream.
        /// This property always returns <see langword='false'/>
        /// </summary>
        public override bool CanSeek { get { return false; } }

        /// <summary>
        /// Indicates that data can be written to the stream.
        /// This property always returns <see langword='true'/>
        /// </summary>
        public override bool CanWrite { get { return true; } }

        /// <summary>
        /// The length of data available on the stream.
        /// Always throws <see cref='NotSupportedException'/>.
        /// </summary>
        public override long Length { get{ throw new NotSupportedException("This stream does not support seek operations."); } }

        /// <summary>
        /// Gets or sets the position in the stream.
        /// Always throws <see cref='NotSupportedException'/>.
        /// </summary>
        public override long Position
        {
            get
            {
                throw new NotSupportedException("This stream does not support seek operations.");
            }

            set
            {
                throw new NotSupportedException("This stream does not support seek operations.");
            }
        }

        /// <summary>
        /// Flushes data in send cache to the stream.
        /// </summary>
        public override void Flush()
        {
            if (sendDataCache != null && sendDataCache.IsReadable())
            {
                try
                {
                    streamSocket.Send(sendDataCache);
                    sendDataCache = null;
                }
                catch (Exception e)
                {
                    if (e is ThreadAbortException || e is StackOverflowException || e is OutOfMemoryException)
                    {
                        throw;
                    }

                    // some sort of error occurred on the socked call,
                    // set the SocketException as InnerException and throw
                    throw new IOException(string.Format("Unable to write data to the transport connection: {0}.", e.Message), e);
                }
            }
        }

        /// <summary>
        /// Seeks a specific position in the stream. This method is not supported
        /// by the SocketDataStream class.
        /// </summary>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException("This stream does not support seek operations.");
        }

        /// <summary>
        /// Sets the length of the stream. This method is not supported by the SocketDataStream class.
        /// </summary>
        public override void SetLength(long value)
        {
            throw new NotSupportedException("This stream does not support seek operations.");
        }


        /// <summary>
        /// Reads a byte from the stream and advances the position within the stream by one byte, or returns -1 if at the end of the stream.
        /// </summary>
        /// <returns>
        /// The unsigned byte cast to an Int32, or -1 if at the end of the stream.
        /// </returns>
        public override int ReadByte()
        {
            if (recvDataCache == null || !recvDataCache.IsReadable())
            {
                recvDataCache = streamSocket.Receive();
            }

            var v = recvDataCache.ReadByte();
            if (recvDataCache.IsReadable()) return v;

            // Release cache if it is not readable. If we do not reset the cache here,
            // the cache will be used in next Read() that caused 0 bytes return.
            recvDataCache.Release();
            recvDataCache = null;
            return v;
        }

        /// <summary>
        /// Reads data from the stream.
        /// </summary>
        /// <param name="buffer">Buffer to read into.</param>
        /// <param name="offset">Offset into the buffer where we're to read.</param>
        /// <param name="count">Number of bytes to read.</param>
        /// <returns>Number of bytes we read.</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            try
            {
                if (recvDataCache == null)
                {
                    recvDataCache = streamSocket.Receive();
                }

                if (!recvDataCache.IsReadable())
                {
                    recvDataCache.Release();
                    recvDataCache = null;
                    return 0;
                }

                var bytesRemaining = count;
                while (recvDataCache != null && recvDataCache.IsReadable() && bytesRemaining > 0)
                {
                    var bytesToRead = Math.Min(bytesRemaining, recvDataCache.ReadableBytes);
                    bytesRemaining -= recvDataCache.ReadBytes(buffer, offset + count - bytesRemaining, bytesToRead);
                    if (recvDataCache.IsReadable()) continue;

                    recvDataCache.Release();
                    recvDataCache = null;
                    if (streamSocket.HasData)
                    {
                        recvDataCache = streamSocket.Receive();
                    }
                }

                return count - bytesRemaining;
            }
            catch (Exception e)
            {
                if (e is ThreadAbortException || e is StackOverflowException || e is OutOfMemoryException)
                {
                    throw;
                }

                // some sort of error occurred on the socket call,
                // set the SocketException as InnerException and throw
                throw new IOException(string.Format("Unable to read data from the transport connection: {0}.", e.Message), e);
            }
        }

        /// <summary>
        /// Writes data to the stream.
        /// </summary>
        /// <param name="buffer">Buffer to write from.</param>
        /// <param name="offset">Offset into the buffer from where we'll start writing.</param>
        /// <param name="count">Number of bytes to write.</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            try
            {
                var remainingBytes = count;
                var newOffset = offset;
                while (0 < remainingBytes)
                {
                    if (sendDataCache == null)
                    {
                        sendDataCache = bufPool.Allocate();
                    }

                    if (sendDataCache.WritableBytes > remainingBytes)
                    {
                        sendDataCache.WriteBytes(buffer, newOffset, remainingBytes);
                        return;
                    }
                    
                    var sendCount = sendDataCache.WritableBytes;
                    sendDataCache.WriteBytes(buffer, newOffset, sendCount);
                    streamSocket.Send(sendDataCache);
                    sendDataCache = null;

                    newOffset += sendCount;
                    remainingBytes -= sendCount;
                }
            }
            catch (Exception e)
            {
                if (e is ThreadAbortException || e is StackOverflowException || e is OutOfMemoryException)
                {
                    throw;
                }

                // some sort of error occurred on the socked call,
                // set the SocketException as InnerException and throw
                throw new IOException(string.Format("Unable to write data to the transport connection: {0}.", e.Message), e);
            }
        }
    }
}