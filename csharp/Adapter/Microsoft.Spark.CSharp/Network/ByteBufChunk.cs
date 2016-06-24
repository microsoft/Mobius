// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// ByteBufChunk represents a memory blocks that can be allocated from 
    /// .Net heap (managed code) or process heap(unsafe code)
    /// </summary>
    internal sealed class ByteBufChunk
    {
        private readonly Queue<Segment> segmentQueue;
        private readonly int segmentSize;
        private bool disposed;
        private byte[] memory;
        private IntPtr unsafeMemory;

        /// <summary>
        /// The ByteBufChunkList that contains this ByteBufChunk
        /// </summary>
        public ByteBufChunkList Parent;

        /// <summary>
        /// The previous ByteBufChunk in linked like list
        /// </summary>
        public ByteBufChunk Prev;

        /// <summary>
        /// The next ByteBufChunk in linked like list
        /// </summary>
        public ByteBufChunk Next;

        private ByteBufChunk(ByteBufPool pool, int segmentSize, int chunkSize)
        {
            Pool = pool;
            FreeBytes = chunkSize;
            Size = chunkSize;
            this.segmentSize = segmentSize;

            segmentQueue = new Queue<Segment>();
            var numSegment = chunkSize / segmentSize;
            for (var i = 0; i < numSegment; i++)
            {
                segmentQueue.Enqueue(new Segment(i * segmentSize, segmentSize));
            }
        }

        private ByteBufChunk(ByteBufPool pool, byte[] memory, int segmentSize, int chunkSize)
            : this(pool, segmentSize, chunkSize)
        {
            if (memory == null || chunkSize == 0)
            {
                throw new ArgumentNullException("memory", "Must be initialized with a valid byte array");
            }

            IsUnsafe = false;
            this.memory = memory;
            unsafeMemory = IntPtr.Zero;
            BufId = IntPtr.Zero;
        }

        private ByteBufChunk(ByteBufPool pool, IntPtr memory, IntPtr bufId, int segmentSize, int chunkSize)
            : this(pool, segmentSize, chunkSize)
        {
            if (memory == IntPtr.Zero || chunkSize == 0)
            {
                throw new ArgumentNullException("memory", "Must be initialized with a valid heap block.");
            }

            IsUnsafe = true;
            unsafeMemory = memory;
            this.memory = null;
            BufId = bufId;
        }

        /// <summary>
        /// Finalizer.
        /// </summary>
        ~ByteBufChunk()
        {
            Dispose(false);
        }

        /// <summary>
        /// Returns the underlying array that is used for managed code.
        /// </summary>
        public byte[] Array { get { return memory; } }

        /// <summary>
        /// Returns the buffer Id that registered as RIO buffer.
        /// Only apply to unsafe ByteBufChunk
        /// </summary>
        public IntPtr BufId { get; private set; }

        /// <summary>
        /// Returns the unused bytes in this chunk.
        /// </summary>
        public int FreeBytes { get; private set; }

        /// <summary>
        /// Indicates whether this ByteBufChunk is disposed.
        /// </summary>
        public bool IsDisposed { get { return disposed; } }

        /// <summary>
        /// Indicates whether the underlying buffer array is a unsafe type array.
        /// The unsafe array is used for PInvoke with native code.
        /// </summary>
        public bool IsUnsafe { get; private set; }

        /// <summary>
        /// Returns the ByteBufPool that this ByteBufChunk belongs to.
        /// </summary>
        public ByteBufPool Pool { get; private set; }

        /// <summary>
        /// Returns the size of the ByteBufChunk
        /// </summary>
        public int Size { get; private set; }

        /// <summary>
        /// Returns the percentage of the current usage of the chunk
        /// </summary>
        public int Usage
        {
            get
            {
                var bytes = FreeBytes;
                if (bytes == 0)
                {
                    return 100;
                }

                var freePercentage = (int)(bytes * 100L / Size);
                if (freePercentage == 0)
                {
                    return 99;
                }
                return 100 - freePercentage;
            }
        }

        /// <summary>
        /// Returns the IntPtr that points to beginning of the cached heap block.
        /// This is used for PInvoke with native code.
        /// </summary>
        public IntPtr UnsafeArray { get { return unsafeMemory;} }

        /// <summary>
        /// Allocates a ByteBuf from this ByteChunk.
        /// </summary>
        /// <param name="byteBuf">The ByteBuf be allocated</param>
        /// <returns>true, if succeed to allocate a ByteBuf; otherwise, false</returns>
        public bool Allocate(out ByteBuf byteBuf)
        {
            if (segmentQueue.Count > 0)
            {
                var segment = segmentQueue.Dequeue();
                FreeBytes -= segmentSize;
                byteBuf = new ByteBuf(this, segment.Offset, segment.Count);
                return true;
            }

            byteBuf = default(ByteBuf);
            return false;
        }

        /// <summary>
        /// Release all resources
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Releases the ByteBuf back to this ByteChunk
        /// </summary>
        /// <param name="byteBuf">The ByteBuf to be released.</param>
        public void Free(ByteBuf byteBuf)
        {
            segmentQueue.Enqueue(new Segment(byteBuf.Offset, byteBuf.Capacity));
            FreeBytes += segmentSize;
        }

        /// <summary>
        /// Returns a readable string for the ByteBufChunk
        /// </summary>
        public override string ToString()
        {
            return new StringBuilder()
                .Append("Chunk(")
                .Append(RuntimeHelpers.GetHashCode(this).ToString("X"))
                .Append(": ")
                .Append(Usage)
                .Append("%, ")
                .Append(Size - FreeBytes)
                .Append('/')
                .Append(Size)
                .Append(')')
                .ToString();
        }

        /// <summary>
        /// Static method to create a new ByteBufChunk with given segment and chunk size.
        /// If isUnsafe is true, it allocates memory from the process's heap.
        /// </summary>
        /// <param name="pool">The ByteBufPool that contains the new ByteChunk</param>
        /// <param name="segmentSize">The segment size</param>
        /// <param name="chunkSize">The chunk size to create</param>
        /// <param name="isUnsafe">Indicates if it is a safe or unsafe</param>
        /// <returns>The new ByteBufChunk object</returns>
        [SuppressMessage("Microsoft.Security", "CA2118:ReviewSuppressUnmanagedCodeSecurityUsage")]
        [SuppressUnmanagedCodeSecurity]
        public static ByteBufChunk NewChunk(ByteBufPool pool, int segmentSize, int chunkSize, bool isUnsafe)
        {
            ByteBufChunk chunk = null;
            if (!isUnsafe)
            {
                chunk = new ByteBufChunk(pool, new byte[chunkSize], segmentSize, chunkSize);
                return chunk;
            }

            // allocate buffers from process heap
            var token = HeapAlloc(GetProcessHeap(), 0, chunkSize);
            if (token == IntPtr.Zero)
            {
                throw new OutOfMemoryException();
            }

            // register this heap buffer to RIO buffer
            var bufferId = RioNative.RegisterRIOBuffer(token, (uint)chunkSize);
            if (bufferId == IntPtr.Zero)
            {
                FreeToProcessHeap(token);
                throw new Exception("Failed to register RIO buffer");
            }

            try
            {
                chunk = new ByteBufChunk(pool, token, bufferId, segmentSize, chunkSize);
                token = IntPtr.Zero;
                bufferId = IntPtr.Zero;
                return chunk;
            }
            finally
            {
                if (chunk == null && token != IntPtr.Zero)
                {
                    if (bufferId != IntPtr.Zero)
                    {
                        RioNative.DeregisterRIOBuffer(bufferId);
                    }
                    FreeToProcessHeap(token);
                }
            }
        }

        /// <summary>
        /// Wraps HeapFree to process heap.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2118:ReviewSuppressUnmanagedCodeSecurityUsage")]
        [SuppressUnmanagedCodeSecurity]
        internal static void FreeToProcessHeap(IntPtr heapBlock)
        {
            Debug.Assert(heapBlock != IntPtr.Zero);
            HeapFree(GetProcessHeap(), 0, heapBlock);
        }

        /// <summary>
        ///     Implementation of the Dispose pattern.
        /// </summary>
        private void Dispose(bool disposing)
        {
            if (disposed)
            {
                return;
            }

            if (!IsUnsafe && memory != null)
            {
                memory = null;
                segmentQueue.Clear();
            }

            if (BufId != IntPtr.Zero)
            {
                RioNative.DeregisterRIOBuffer(BufId);
            }

            // If the unsafedMemory is still valid, free it.
            if (unsafeMemory != IntPtr.Zero)
            {
                var heapBlock = unsafeMemory;
                unsafeMemory = IntPtr.Zero;
                FreeToProcessHeap(heapBlock);
            }

            if (disposing)
            {
                GC.SuppressFinalize(this);
            }

            disposed = true;
        }

        /// <summary>
        /// Segment struct delimits a section of a byte chunk.
        /// </summary>
        private struct Segment
        {
            public Segment(int offset, int count)
            {
                Offset = offset;
                Count = count;
            }

            public readonly int Count;
            public readonly int Offset;
        }
        
        #region PInvoke

        [SuppressUnmanagedCodeSecurity]
        [DllImport("Kernel32.dll", CallingConvention = CallingConvention.Winapi, CharSet = CharSet.Unicode)]
        private static extern IntPtr HeapAlloc(IntPtr heapHandle, int flags, int size);

        [SuppressUnmanagedCodeSecurity]
        [DllImport("Kernel32.dll", CallingConvention = CallingConvention.Winapi, CharSet = CharSet.Unicode)]
        private static extern void HeapFree(IntPtr heapHandle, int flags, IntPtr freePtr);

        [SuppressUnmanagedCodeSecurity]
        [DllImport("Kernel32.dll", CallingConvention = CallingConvention.Winapi, CharSet = CharSet.Unicode)]
        private static extern IntPtr GetProcessHeap();

        #endregion
    }
}