// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Runtime.InteropServices;
using System.Security;

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// ByteBuf delimits a section of a ByteBufChunk.
    /// It is the smallest unit to be allocated.
    /// </summary>
    internal class ByteBuf
    {
        private int readerIndex;
        private int writerIndex;

        /// <summary>
        /// Indicates the state of that the ByteBuf as socket data transport.
        /// </summary>
        public int Status;

        /// <summary>
        /// We borrow some ideas from Netty's ByteBuf. 
        /// ByteBuf provides two pointer variables to support sequential read and write operations
        ///  - readerIndex for a read operation and writerIndex for a write operation respectively.
        /// The following diagram shows how a buffer is segmented into three areas by the two
        /// pointers:
        /// 
        ///      +-------------------+------------------+------------------+
        ///      | discardable bytes |  readable bytes  |  writable bytes  |
        ///      |                   |     (CONTENT)    |                  |
        ///      +-------------------+------------------+------------------+
        ///      |                   |                  |                  |
        ///      0       ==     readerIndex   ==   writerIndex    ==    capacity
        /// </summary>
        internal ByteBuf(ByteBufChunk chunk, int offset, int capacity)
        {
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset", "Offset is less than zero.");
            if (capacity < 0)
                throw new ArgumentOutOfRangeException("capacity", "Count is less than zero.");
            if (chunk == null)
                throw new ArgumentNullException("chunk");
            if (chunk.Size - offset < capacity)
                throw new ArgumentException(
                    "Offset and length were out of bounds for the array or count is greater than the number of elements from index to the end of the source collection.");

            Capacity = capacity;
            Offset = offset;
            ByteBufChunk = chunk;
            readerIndex = writerIndex = 0;
            Status = 0;
        }

        private ByteBuf(int errorStatus)
        {
            Status = errorStatus;
            Capacity = 0;
            Offset = 0;
            ByteBufChunk = null;
            readerIndex = writerIndex = 0;
        }

        /// <summary>
        /// Gets the underlying array.
        /// </summary>
        public byte[] Array { get { return ByteBufChunk.Array; } }

        /// <summary>
        /// Gets the total number of elements in the range delimited by the ByteBuf.
        /// </summary>
        public int Capacity { get; private set; }

        /// <summary>
        /// Gets the position of the first element in the range delimited
        /// by the ByteBuf, relative to the start of the original array.
        /// </summary>
        public int Offset { get; private set; }

        /// <summary>
        /// Returns the ByteBuf chunk that contains this ByteBuf.
        /// </summary>
        internal ByteBufChunk ByteBufChunk { get; private set; }

        /// <summary>
        /// Returns the number of readable bytes which is equal to (writerIndex - readerIndex).
        /// </summary>
        public int ReadableBytes { get { return WriterIndex - ReaderIndex; } }

        /// <summary>
        /// Returns the number of writable bytes which is equal to (capacity - writerIndex).
        /// </summary>
        public int WritableBytes { get { return Capacity - WriterIndex; } }

        /// <summary>
        /// Gets the underlying unsafe array.
        /// </summary>
        public IntPtr UnsafeArray { get { return ByteBufChunk.UnsafeArray; } }

        /// <summary>
        /// Gets or sets the readerIndex of this ByteBuf
        /// </summary>
        /// <exception cref="IndexOutOfRangeException"></exception>
        public int ReaderIndex
        {
            get { return readerIndex; }
            set
            {
                if (value < 0 || value > WriterIndex)
                {
                    throw new IndexOutOfRangeException(string.Format(
                        "ReaderIndex: {0} (expected: 0 <= readerIndex <= writerIndex({1})", value, writerIndex));
                }
                readerIndex = value;
            }
        }

        /// <summary>
        /// Gets or sets the writerIndex of this ByteBuf
        /// </summary>
        /// <exception cref="IndexOutOfRangeException"></exception>
        public int WriterIndex
        {
            get { return writerIndex; }
            set
            {
                if (value < ReaderIndex || value > Capacity)
                {
                    throw new IndexOutOfRangeException(string.Format(
                        "WriterIndex: {0}  (expected: 0 <= readerIndex({1}) <= writerIndex <= capacity ({2})", value, ReaderIndex, Capacity));
                }
                writerIndex = value;
            }
        }

        /// <summary>
        /// Returns the position of the readerIndex element in the range delimited
        /// by the ByteBuf, relative to the start of the original array.
        /// </summary>
        public int ReaderIndexOffset { get { return Idx(readerIndex); } }

        /// <summary>
        /// Returns the position of the readerIndex element in the range delimited
        /// by the ByteBuf, relative to the start of the original array.
        /// </summary>
        public int WriterIndexOffset { get { return Idx(writerIndex); } }

        /// <summary>
        /// Sets the readerIndex and writerIndex of this buffer to 0.
        /// </summary>
        public void Clear()
        {
            readerIndex = writerIndex = 0;
        }

        /// <summary>
        /// Is this ByteSegment readable if and only if the buffer contains equal or more than
        /// the specified number of elements
        /// </summary>
        /// <param name="size">
        /// The number of elements we would like to read,
        /// The default value is 1 that is to check this ByteBuf has at least 1 byte can be read.
        /// </param>
        /// <returns>true, if it is readable; otherwise, false</returns>
        public bool IsReadable(int size = 1)
        {
            if (ByteBufChunk == null || ByteBufChunk.IsDisposed)
            {
                return false;
            }

            return ReadableBytes >= size;
        }

        /// <summary>
        ///     Returns true if and only if the buffer has enough Capacity to accommodate size
        ///     additional bytes.
        /// </summary>
        /// <param name="size">
        /// The number of additional elements we would like to write
        /// The default value is 1 that is to check this ByteBuf has at least 1 byte can be wroten.
        /// </param>
        /// <returns>true, if this ByteSegment is writable; otherwise, false</returns>
        public bool IsWritable(int size = 1)
        {
            if (ByteBufChunk == null || ByteBufChunk.IsDisposed)
            {
                return false;
            }

            return WritableBytes >= size;
        }

        /// <summary>
        /// Gets a byte at the current readerIndex and increases the readerIndex by 1 in this buffer.
        /// </summary>
        public byte ReadByte()
        {
            CheckReadableBytes(1);
            var b = ByteBufChunk.IsUnsafe
                ? Marshal.ReadByte(ByteBufChunk.UnsafeArray, ReaderIndexOffset)
                : ByteBufChunk.Array[ReaderIndexOffset];
            ReaderIndex += 1;
            return b;
        }

        /// <summary>
        /// Reads a block of bytes from the ByteBuf and writes the data to a buffer.
        /// </summary>
        /// <param name="buffer">
        /// When this method returns, contains the specified byte array with the values
        /// between offset and (offset + count - 1) replaced by the characters read
        ///  from the ByteBuf. 
        /// </param>
        /// <param name="offset">The zero-based byte offset in buffer at which to begin storing data from the ByteBuf.</param>
        /// <param name="count">The maximum number of bytes to read.</param>
        /// <returns></returns>
        public unsafe int ReadBytes(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException("buffer", "Buffer cannot be null.");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset", "Offset is less than zero.");
            if (count < 0)
                throw new ArgumentOutOfRangeException("count", "Count is less than zero.");
            if (buffer.Length - offset < count)
                throw new ArgumentException("Offset and length were out of bounds for the array or count is greater than the number of elements from index to the end of the source collection.");

            EnsureAccessible();
            CheckReadableBytes(count);

            int n = WriterIndex - ReaderIndex;
            if (n > count) n = count;
            if (n <= 0) return 0;

            if (ByteBufChunk.IsUnsafe)
            {
                fixed (byte* pBuf = &buffer[offset])
                {
                    memcpy((IntPtr)pBuf, ByteBufChunk.UnsafeArray + ReaderIndexOffset, (ulong)n);
                }
            }
            else
            {
                Buffer.BlockCopy(ByteBufChunk.Array, ReaderIndexOffset, buffer, offset, n);
            }

            ReaderIndex += n;
            return n;
        }

        /// <summary>
        /// Release the ByteBuf back to the ByteBufPool
        /// </summary>
        public void Release()
        {
            if (ByteBufChunk == null || ByteBufChunk.IsDisposed)
            {
                return;
            }

            var byteBufPool = ByteBufChunk.Pool;
            byteBufPool.Free(this);
            ByteBufChunk = null;
        }

        /// <summary>
        /// Writes a block of bytes to the ByteBuf using data read from a buffer.
        /// </summary>
        /// <param name="buffer">The buffer to write data from. </param>
        /// <param name="offset">The zero-based byte offset in buffer at which to begin copying bytes to the ByteBuf.</param>
        /// <param name="count">The maximum number of bytes to write.</param>
        public unsafe void WriteBytes(byte[] buffer, int offset, int count)
        {
            // Check parameters
            if (buffer == null)
                throw new ArgumentNullException("buffer", "Buffer cannot be null.");
            if (offset < 0)
                throw new ArgumentOutOfRangeException("offset", "Offset is less than zero.");
            if (count < 0)
                throw new ArgumentOutOfRangeException("count", "Count is less than zero.");
            if (buffer.Length - offset < count)
                throw new ArgumentException(
                    "Offset and length were out of bounds for the array or count is greater than the number of elements from index to the end of the source collection.");

            EnsureAccessible();
            EnsureWritable(count);

            if (ByteBufChunk.IsUnsafe)
            {
                fixed (byte* pBuf = &buffer[offset])
                {
                    memcpy(ByteBufChunk.UnsafeArray + WriterIndexOffset, (IntPtr)pBuf, (ulong) count);
                }
            }
            else
            {
                Buffer.BlockCopy(buffer, offset, ByteBufChunk.Array, WriterIndexOffset, count);
            }

            WriterIndex += count;
        }

        /// <summary>
        /// Returns a RioBuf object for input (receive)
        /// </summary>
        /// <returns>A RioBuf object</returns>
        internal RioBuf GetInputRioBuf()
        {
            EnsureAccessible();
            if (!ByteBufChunk.IsUnsafe)
            {
                throw new InvalidOperationException("Managed ByteSegment does not support RioBuf.");
            }

            return new RioBuf(ByteBufChunk.BufId, (uint)WriterIndexOffset, (uint)WritableBytes);
        }

        /// <summary>
        /// Returns a RioBuf object for output (send).
        /// </summary>
        /// <returns>A RioBuf object</returns>
        internal RioBuf GetOutputRioBuf()
        {
            EnsureAccessible();
            if (!ByteBufChunk.IsUnsafe)
            {
                throw new InvalidOperationException("Managed ByteSegment does not support RioBuf.");
            }

            return new RioBuf(ByteBufChunk.BufId, (uint)ReaderIndexOffset, (uint)ReadableBytes);
        }

        /// <summary>
        /// Creates an empty ByteBuf with error status.
        /// </summary>
        internal static ByteBuf NewErrorStatusByteBuf(int errorCode)
        {
            return new ByteBuf(errorCode);
        }

        private void CheckReadableBytes(int minimumReadableBytes)
        {
            EnsureAccessible();
            if (ReaderIndex > WriterIndex - minimumReadableBytes)
            {
                throw new IndexOutOfRangeException(string.Format(
                    "readerIndex({0}) + length({1}) exceeds writerIndex({2})", ReaderIndex, minimumReadableBytes, WriterIndex));
            }
        }

        private void EnsureAccessible()
        {
            if (ByteBufChunk == null || ByteBufChunk.IsDisposed)
            {
                throw new ObjectDisposedException("ByteBufChunk");
            }
        }

        private void EnsureWritable(int minWritableBytes)
        {
            EnsureAccessible();
            if (minWritableBytes <= WritableBytes)
            {
                return;
            }

            if (minWritableBytes > Capacity - WriterIndex)
            {
                throw new IndexOutOfRangeException(string.Format(
                    "writerIndex({0}) + minWritableBytes({1}) exceeds Capacity({2})", WriterIndex, minWritableBytes, Capacity));
            }
        }

        private int Idx(int index)
        {
            return Offset + index;
        }

        [DllImport("msvcrt.dll", EntryPoint = "memcpy", CallingConvention = CallingConvention.Cdecl, SetLastError = false)]
        [SuppressUnmanagedCodeSecurity]
        private static extern IntPtr memcpy(IntPtr dest, IntPtr src, ulong count);
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct RioBuf
    {
        public RioBuf(IntPtr bufferId, uint offset, uint length)
        {
            BufferId = bufferId;
            Offset = offset;
            Length = length;
        }

        public readonly IntPtr BufferId;
        public readonly uint Offset;
        public uint Length;
    }
}