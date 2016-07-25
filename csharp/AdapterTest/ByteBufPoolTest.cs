// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Spark.CSharp.Network;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class ByteBufPoolTest
    {
        private ByteBufPool bufPool;
        private ByteBufPool unsafeBufPool;

        [OneTimeSetUp]
        public void CreatePooledBuffer()
        {
            bufPool = new ByteBufPool(1024, ByteBufPool.DefaultChunkOrder, false);
            if (SocketFactory.IsRioSockSupported())
            {
                unsafeBufPool = new ByteBufPool(1024, ByteBufPool.DefaultChunkOrder, true);
            }
        }

        [Test]
        public void TestManagedBufferAllocate()
        {
            var byteBuf = bufPool.Allocate();
            var bufChunk = byteBuf.ByteBufChunk;
            Assert.AreEqual(bufChunk.FreeBytes, bufChunk.Size - byteBuf.Capacity);
            byteBuf.Release();
            Assert.AreEqual(bufChunk.FreeBytes, bufChunk.Size);
        }

        [Test]
        public void TestManagedBufferPoolGrow()
        {
            // Verify no chunks in 100% usage queue at beginning.
            var chunkNumbers = bufPool.GetUsages();
            Assert.AreEqual(0, chunkNumbers[5]); // The number of chunks in 100% usage queue should be 0 at beginning.

            var bufs = new List<ByteBuf>();
            for (var i = 0; i < 257; i++)
            {
                var byteBuf = bufPool.Allocate();
                bufs.Add(byteBuf);
            }

            var firstChunk = bufs[255].ByteBufChunk;
            var secondChunk = bufs[256].ByteBufChunk;

            // Verify the buffer pool got grown.
            Assert.AreNotSame(firstChunk, secondChunk);
            // Verify the usage of the first buffer chunk
            Assert.AreEqual(firstChunk.Usage, 100);
            // Verify the usage of the second buffer chunk
            Assert.AreEqual(secondChunk.Usage, 1);
            // Verify the chunk exhaust and should be in 100% usage queue now.
            chunkNumbers = bufPool.GetUsages();
            Assert.AreNotEqual(0, chunkNumbers[5]); // The number of chunks in 100% usage queue should not be 0 now.
            // Verify the ToString() shows as usage string.
            var usageStr = bufPool.ToString();
            Assert.IsTrue(usageStr.StartsWith("Chunk(s) at 0~25%"));

            // Release buffers back to pool.
            foreach (var byteBuf in bufs)
            {
                byteBuf.Release();
            }

            // Verify the first buffer chunk is disposed.
            Assert.IsTrue(firstChunk.IsDisposed);
            // Verify the usage of the second buffer chunk is 0.
            Assert.AreEqual(secondChunk.Usage, 0);
        }

        [Test]
        public void TestUnsafeBufferAllocate()
        {
            if (!SocketFactory.IsRioSockSupported())
            {
                Assert.Ignore("Omitting due to missing Riosock.dll. It might caused by no VC++ build tool or running on an OS that not supports Windows RIO socket.");
            }

            var byteBuf = unsafeBufPool.Allocate();
            var bufChunk = byteBuf.ByteBufChunk;
            Assert.AreNotEqual(IntPtr.Zero, bufChunk.UnsafeArray);
            Assert.AreNotEqual(bufChunk.BufId, IntPtr.Zero);
            Assert.AreEqual(bufChunk.FreeBytes, bufChunk.Size - byteBuf.Capacity);

            // Verify GetInputRioBuf()
            var inputRioBuf = byteBuf.GetInputRioBuf();
            Assert.AreNotEqual(null, inputRioBuf);
            Assert.AreEqual(byteBuf.WritableBytes, inputRioBuf.Length);

            // Verify GetOutputRioBuf()
            const string writeStr = "Write bytes to ByteBuf.";
            var writeBytes = Encoding.UTF8.GetBytes(writeStr);
            byteBuf.WriteBytes(writeBytes, 0, writeBytes.Length);
            var outputRioBuf = byteBuf.GetOutputRioBuf();
            Assert.AreNotEqual(null, outputRioBuf);
            Assert.AreEqual(byteBuf.ReadableBytes, outputRioBuf.Length);

            byteBuf.Release();
            Assert.AreEqual(bufChunk.FreeBytes, bufChunk.Size);
        }

        [Test]
        public void TestUnsafeBufferPoolGrow()
        {
            if (!SocketFactory.IsRioSockSupported())
            {
                Assert.Ignore("Omitting due to missing Riosock.dll. It might caused by no VC++ build tool or running on an OS that not supports Windows RIO socket.");
            }

            var bufs = new List<ByteBuf>();
            for (var i = 0; i < 257; i++)
            {
                var byteBuf = unsafeBufPool.Allocate();
                bufs.Add(byteBuf);
            }

            var firstChunk = bufs[255].ByteBufChunk;
            var secondChunk = bufs[256].ByteBufChunk;

            // Verify the buffer pool got grown.
            Assert.AreNotSame(firstChunk, secondChunk);
            Assert.AreNotEqual(firstChunk.BufId, secondChunk.BufId);
            // Verify the usage of the first buffer chunk
            Assert.AreEqual(firstChunk.Usage, 100);
            // Verify the usage of the second buffer chunk
            Assert.AreEqual(secondChunk.Usage, 1);

            // Release buffers back to pool.
            foreach (var byteBuf in bufs)
            {
                byteBuf.Release();
            }

            // Verify the first buffer chunk is disposed.
            Assert.IsTrue(firstChunk.IsDisposed);
            // Verify the usage of the second buffer chunk is 0.
            Assert.AreEqual(secondChunk.Usage, 0);
        }

        [Test]
        public void TestInvalidBufPool()
        {
            // Verify to new a ByteBufPool with a bigger chunkOrder which valid value is 0-14.
            Assert.Throws<ArgumentException>(() => new ByteBufPool(1024, 15, false));
            // Verify to new a ByteBugPool with a larger segment size to hit overflow of ByteBufChunk size.
            Assert.Throws<ArgumentException>(() => new ByteBufPool(int.MaxValue, 8, false));
        }
    }
}