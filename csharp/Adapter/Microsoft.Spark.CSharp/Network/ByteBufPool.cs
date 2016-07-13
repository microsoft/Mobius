// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// ByteBufPool class is used to manage the ByteBuf pool that allocate and free pooled memory buffer.
    /// We borrows some ideas from Netty buffer memory management.
    /// </summary>
    internal sealed class ByteBufPool
    {
        /// <summary>
        /// The chunk size is calculated with given segment size and chunk order.
        /// The MaxChunkSize ensure ByteBuf chunk Size (Int32.MaxValue) does not overflow.
        /// </summary>
        private const int MaxChunkSize = (int)((int.MaxValue + 1L) / 2);

        private static readonly Lazy<ByteBufPool> DefaultPool =
            new Lazy<ByteBufPool>(() => new ByteBufPool(DefaultSegmentSize, DefaultChunkOrder, false));
        private static readonly Lazy<ByteBufPool> DefaultUnsafePool =
            new Lazy<ByteBufPool>(() => new ByteBufPool(DefaultSegmentSize, DefaultChunkOrder, true));

        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(RioSocketWrapper));
        private readonly bool isUnsafe;
        private readonly ByteBufChunkList qInit, q000, q025, q050, q075, q100;

        /// <summary>
        /// The default segment size to delimit a byte array chunk. 
        /// </summary>
        public const int DefaultSegmentSize = 131072; // 65536; // 64K

        /// <summary>
        /// The default chunk order used to calculate chunk size and ensure it is a multiple of a segment size.
        /// </summary>
        public const int DefaultChunkOrder = 7; // 65536 << 8 = 16 MBytes per chunk

        /// <summary>
        /// Initializes a new ByteBufPool instance with a given segment size and chunk order.
        /// If isUnsafe is true, all memory will be allocated from process's heap by using
        /// native HeapAlloc API.
        /// </summary>
        /// <param name="segmentSize">The size of a segment</param>
        /// <param name="chunkOrder">Used to caculate chunk size and ensures it is a multiple of a segment size</param>
        /// <param name="isUnsafe">Indicates whether allocates memory from process's heap</param>
        public ByteBufPool(int segmentSize, int chunkOrder, bool isUnsafe)
        {
            SegmentSize = segmentSize;
            ChunkSize = ValidateAndCalculateChunkSize(segmentSize, chunkOrder);
            this.isUnsafe = isUnsafe;

            q100 = new ByteBufChunkList(null, 100, int.MaxValue); // No maxUsage for this list. All 100% usage of chunks will be in here.
            q075 = new ByteBufChunkList(q100, 75, 100);
            q050 = new ByteBufChunkList(q075, 50, 100);
            q025 = new ByteBufChunkList(q050, 25, 75);
            q000 = new ByteBufChunkList(q025, 1, 50);
            qInit = new ByteBufChunkList(q000, int.MinValue, 25); // No minUsage for this list. All new chunks will be inserted to here.

            q100.PrevList = q075;
            q075.PrevList = q050;
            q050.PrevList = q025;
            q025.PrevList = q000;
            q000.PrevList = null;
            qInit.PrevList = qInit;
        }

        /// <summary>
        /// Gets the default byte buffer pool instance for managed memory.
        /// </summary>
        /// <remarks>
        /// You should only be using this instance to allocate/deallocate the managed memory arena
        /// if you don't want to manage memory arena on your own.
        /// </remarks>
        public static ByteBufPool Default
        {
            get { return DefaultPool.Value; }
        }

        /// <summary>
        /// Gets the default byte arena instance for unmanaged memory.
        /// </summary>
        /// <remarks>
        /// You should only be using this instance to allocate/deallocate the unmanaged memory arena
        /// if you don't want to manage memory arena on your own.
        /// </remarks>
        public static ByteBufPool UnsafeDefault
        {
            get { return DefaultUnsafePool.Value; }
        }

        /// <summary>
        /// Returns the size of a ByteBuf in this ByteBufPool
        /// </summary>
        public int SegmentSize { get; private set; }

        /// <summary>
        /// Returns the size of a ByteChunk in this ByteBufPool
        /// </summary>
        public int ChunkSize { get; private set; }

        /// <summary>
        /// Allocates a ByteBuf from this ByteBufPool to use.
        /// </summary>
        /// <returns>A ByteBuf contained in this ByteBufPool</returns>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public ByteBuf Allocate()
        {
            ByteBuf byteBuf;
            if (q050.Allocate(out byteBuf) || q025.Allocate(out byteBuf) ||
                q000.Allocate(out byteBuf) || qInit.Allocate(out byteBuf) ||
                q075.Allocate(out byteBuf))
            {
                return byteBuf;
            }

            // Add a new chunk and allocate a segment from it.
            var chunk = ByteBufChunk.NewChunk(this, SegmentSize, ChunkSize, isUnsafe);
            if (!chunk.Allocate(out byteBuf))
            {
                logger.LogError("Failed to allocate a ByteBuf from a new ByteBufChunk. {0}", chunk);
                return null;
            }
            qInit.Add(chunk);
            return byteBuf;
        }

        /// <summary>
        /// Deallocates a ByteBuf back to this ByteBufPool.
        /// </summary>
        /// <param name="byteBuf">The ByteBuf to be release.</param>
        public void Free(ByteBuf byteBuf)
        {
            if (byteBuf.ByteBufChunk == null || byteBuf.Capacity == 0 || byteBuf.ByteBufChunk.Size < byteBuf.Offset + byteBuf.Capacity)
            {
                throw new Exception("Attempt to free invalid byteBuf");
            }

            if (byteBuf.Capacity != SegmentSize)
            {
                throw new ArgumentException("Segment was not from the same byte arena", "byteBuf");
            }

            bool mustDestroyChunk;
            var chunk = byteBuf.ByteBufChunk;
            lock (this)
            {
                mustDestroyChunk = !chunk.Parent.Free(chunk, byteBuf);
            }

            if (!mustDestroyChunk) return;

            // Destroy chunk not need to be called while holding the synchronized lock.
            chunk.Parent = null;
            chunk.Dispose();
        }

        /// <summary>
        /// Gets a readable string for this ByteBufPool
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public override string ToString()
        {
            StringBuilder buf = new StringBuilder()
                .Append("Chunk(s) at 0~25%:")
                .Append(Environment.NewLine)
                .Append(qInit)
                .Append(Environment.NewLine)
                .Append("Chunk(s) at 0~50%:")
                .Append(Environment.NewLine)
                .Append(q000)
                .Append(Environment.NewLine)
                .Append("Chunk(s) at 25~75%:")
                .Append(Environment.NewLine)
                .Append(q025)
                .Append(Environment.NewLine)
                .Append("Chunk(s) at 50~100%:")
                .Append(Environment.NewLine)
                .Append(q050)
                .Append(Environment.NewLine)
                .Append("Chunk(s) at 75~100%:")
                .Append(Environment.NewLine)
                .Append(q075)
                .Append(Environment.NewLine)
                .Append("Chunk(s) at 100%:")
                .Append(Environment.NewLine)
                .Append(q100)
                .Append(Environment.NewLine);

            return buf.ToString();
        }

        /// <summary>
        /// Returns the chunk numbers in each queue.
        /// </summary>
        [MethodImpl(MethodImplOptions.Synchronized)]
        public int[] GetUsages()
        {
            var qUsage = new int[6];
            var qIndex = 0;
            for (var q = qInit; q != null; q = q.NextList, qIndex++)
            {
                int count = 0;
                for (var cur = q.head; cur != null; cur = cur.Next, count++) {}
                qUsage[qIndex] = count;
            }
            return qUsage;
        }

        private static int ValidateAndCalculateChunkSize(int segmentSize, int chunkOrder)
        {
            if (chunkOrder > 14)
            {
                throw new ArgumentException(string.Format(
                    "chunkOrder: {0} (expected: 0-14)", chunkOrder));
            }

            // Ensure the resulting chunkSize does not overflow.
            var chunkSize = segmentSize;
            for (var i = chunkOrder; i > 0; i--)
            {
                if (chunkSize > MaxChunkSize >> 1)
                {
                    throw new ArgumentException(string.Format(
                        "segmentSize ({0}) << chunkOrder ({1}) must not exceed {2}", segmentSize, chunkOrder, MaxChunkSize));
                }
                chunkSize <<= 1;
            }

            return chunkSize;
        }
    }
}