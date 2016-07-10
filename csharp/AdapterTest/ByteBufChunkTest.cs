using System;
using Microsoft.Spark.CSharp.Network;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class ByteBufChunkTest
    {
        private ByteBufPool managedBufPool;
        private ByteBufPool unsafeBufPool;

        [OneTimeSetUp]
        public void CreatePooledBuffer()
        {
            managedBufPool = new ByteBufPool(1024, 2, false);
            unsafeBufPool = new ByteBufPool(1024, 2, true);
        }

        [Test]
        public void TestInvalidByteChunk()
        {
            // Input chunk size with 0
            Assert.Throws<ArgumentNullException>(() => ByteBufChunk.NewChunk(managedBufPool, managedBufPool.SegmentSize, 0, false));

            if (!SocketFactory.IsRioSockSupported()) return;
            // Input chunk size with 0
            Assert.Throws<ArgumentNullException>(() => ByteBufChunk.NewChunk(unsafeBufPool, unsafeBufPool.SegmentSize, 0, true));
            // Input chunk size with negative value that caused HeapAlloc failed.
            Assert.Throws<OutOfMemoryException>(() => ByteBufChunk.NewChunk(unsafeBufPool, unsafeBufPool.SegmentSize, -1, true));
        }
        
        private void AllocateFreeBufChunkTest(ByteBufChunk chunk)
        {
            // Verify allocation
            ByteBuf byteBuf1;
            Assert.IsTrue(chunk.Allocate(out byteBuf1));
            Assert.AreEqual(25, chunk.Usage);
            ByteBuf byteBuf2;
            Assert.IsTrue(chunk.Allocate(out byteBuf2));
            Assert.AreEqual(50, chunk.Usage);
            ByteBuf byteBuf3;
            Assert.IsTrue(chunk.Allocate(out byteBuf3));
            Assert.AreEqual(75, chunk.Usage);
            ByteBuf byteBuf4;
            Assert.IsTrue(chunk.Allocate(out byteBuf4));
            Assert.AreEqual(100, chunk.Usage);
            ByteBuf byteBuf5;
            Assert.IsFalse(chunk.Allocate(out byteBuf5)); // Usage is 100%, cannot allocate
            Assert.IsNull(byteBuf5);

            // Verify Free()
            chunk.Free(byteBuf1);
            Assert.AreEqual(75, chunk.Usage);
            chunk.Free(byteBuf2);
            Assert.AreEqual(50, chunk.Usage);
            chunk.Free(byteBuf3);
            Assert.AreEqual(25, chunk.Usage);
            chunk.Free(byteBuf4);
            Assert.AreEqual(0, chunk.Usage);
        }

        [Test]
        public void TestAllocateFreeManagedBufChunk()
        {
            // This chunk can only allocate 4 ByteBufs totally
            var chunk = ByteBufChunk.NewChunk(managedBufPool, managedBufPool.SegmentSize, managedBufPool.ChunkSize, false);
            AllocateFreeBufChunkTest(chunk);
            chunk.Dispose();
        }

        [Test]
        public void TestAllocateFreeUnsafeBufChunk()
        {
            if (!SocketFactory.IsRioSockSupported())
            {
                Assert.Ignore("Omitting due to missing Riosock.dll. It might caused by no VC++ build tool or running on an OS that not supports Windows RIO socket.");
            }
            // This chunk can only allocate 4 ByteBufs totally
            var chunk = ByteBufChunk.NewChunk(unsafeBufPool, unsafeBufPool.SegmentSize, unsafeBufPool.ChunkSize, true);
            Assert.AreNotEqual(IntPtr.Zero, chunk.BufId);
            AllocateFreeBufChunkTest(chunk);
            chunk.Dispose();
        }
    }
}