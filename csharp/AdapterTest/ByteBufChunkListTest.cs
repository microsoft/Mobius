using Microsoft.Spark.CSharp.Network;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class ByteBufChunkListTest
    {
        private ByteBufPool bufPool;

        [OneTimeSetUp]
        public void CreatePooledBuffer()
        {
            bufPool = new ByteBufPool(1024, 2, false);
        }

        [Test]
        public void TestAllocateAndFree()
        {
            var q50 = new ByteBufChunkList(null, 50, 200);
            var q00 = new ByteBufChunkList(q50, 1, 50);
            q00.PrevList = null;
            q50.PrevList = q00;

            // This chunk can only allocate 4 ByteBufs totally
            var chunk1 = ByteBufChunk.NewChunk(bufPool, bufPool.SegmentSize, bufPool.ChunkSize, false);
            var chunk2 = ByteBufChunk.NewChunk(bufPool, bufPool.SegmentSize, bufPool.ChunkSize, false);
            var chunk3 = ByteBufChunk.NewChunk(bufPool, bufPool.SegmentSize, bufPool.ChunkSize, false);

            //
            // Verify Allocate()
            //
            q00.Add(chunk1);
            // Verify the chunk is hosted in q50.
            Assert.AreEqual(chunk1.ToString(), q00.ToString());
            // Verify allocation success
            ByteBuf byteBuf1;
            Assert.IsTrue(q00.Allocate(out byteBuf1));
            // Verify the chunk be moved to next list.
            ByteBuf byteBuf2;
            Assert.IsTrue(q00.Allocate(out byteBuf2));
            Assert.AreEqual(chunk1.ToString(), q50.ToString()); // chunk1 is in q50 now

            // Allocates from q50
            ByteBuf byteBuf3;
            Assert.IsTrue(q50.Allocate(out byteBuf3));
            ByteBuf byteBuf4;
            Assert.IsTrue(q50.Allocate(out byteBuf4));
            // Verify failed allocation due to chunk 100% usage
            ByteBuf byteBuf5;
            Assert.AreEqual(100, chunk1.Usage);
            Assert.IsFalse(q50.Allocate(out byteBuf5));
            // Verify allocation success from chunk2
            q50.Add(chunk2);
            Assert.IsTrue(q50.Allocate(out byteBuf5));
            Assert.AreEqual(chunk2, byteBuf5.ByteBufChunk);

            //
            // Verify Free()
            //

            // Build chunk1's chain for Free()
            chunk1.Next = chunk3;
            chunk3.Prev = chunk1;

            // Verify Free() returns false due to chunk2's usage is 0
            // that means the chunk need to be destroyed.
            Assert.IsFalse(q50.Free(chunk2, byteBuf5));
            q50.Add(chunk2);

            // Free chunk1 from q50 now.
            Assert.IsTrue(q50.Free(chunk1, byteBuf4));
            Assert.IsTrue(q50.Free(chunk1, byteBuf3));
            Assert.IsTrue(q50.Free(chunk1, byteBuf2));
            Assert.AreEqual(chunk1.ToString(), q00.ToString()); // chunk1 is in q00 now
            Assert.AreSame(q00, chunk1.Parent);
            Assert.AreSame(chunk3, chunk2.Next);

            // Free chunk1 from q00 now.
            Assert.IsFalse(q00.Free(chunk1, byteBuf1));
            Assert.AreEqual("none", q00.ToString()); // q00 is empty now
        }
    }
}