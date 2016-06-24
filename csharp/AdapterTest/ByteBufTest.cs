using System;
using System.Text;
using Microsoft.Spark.CSharp.Network;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class ByteBufTest
    {
        private ByteBufPool managedBufPool;
        private ByteBufPool unsafeBufPool;

        [OneTimeSetUp]
        public void CreatePooledBuffer()
        {
            managedBufPool = new ByteBufPool(1024, ByteBufPool.DefaultChunkOrder, false);
            if (SocketFactory.IsRioSockSupported())
            {
                unsafeBufPool = new ByteBufPool(1024, ByteBufPool.DefaultChunkOrder, true);
            }
        }
        
        private void WriteReadByteBufTest(ByteBuf byteBuf)
        {
            var initWriteIndex = byteBuf.WriterIndex;
            var initReadIndex = byteBuf.WriterIndex;
            Assert.AreEqual(initWriteIndex, initReadIndex);
            Assert.AreEqual(byteBuf.Capacity, byteBuf.WritableBytes);
            Assert.AreEqual(0, byteBuf.ReadableBytes);
            Assert.IsFalse(byteBuf.IsReadable());
            Assert.IsTrue(byteBuf.IsWritable());

            // Verify WriteBytes() function
            const string writeStr = "Write bytes to ByteBuf.";
            var writeBytes = Encoding.UTF8.GetBytes(writeStr);
            byteBuf.WriteBytes(writeBytes, 0, writeBytes.Length);

            Assert.AreEqual(initWriteIndex + writeBytes.Length, byteBuf.WriterIndex);
            Assert.AreEqual(byteBuf.Capacity - writeBytes.Length, byteBuf.WritableBytes);
            Assert.AreEqual(writeBytes.Length, byteBuf.ReadableBytes);
            Assert.AreEqual(initReadIndex, byteBuf.ReaderIndex);
            Assert.IsTrue(byteBuf.IsReadable());

            // Verify ReadBytes() function
            var readBytes = new byte[writeBytes.Length];
            var ret = byteBuf.ReadBytes(readBytes, 0, readBytes.Length);
            Assert.AreEqual(writeBytes.Length, ret);
            var readStr = Encoding.UTF8.GetString(readBytes, 0, ret);
            Assert.AreEqual(writeStr, readStr);

            Assert.AreEqual(initWriteIndex + writeBytes.Length, byteBuf.WriterIndex);
            Assert.AreEqual(byteBuf.Capacity - writeBytes.Length, byteBuf.WritableBytes);
            Assert.AreEqual(0, byteBuf.ReadableBytes);
            Assert.AreEqual(initReadIndex + ret, byteBuf.ReaderIndex);

            // Verify ReadByte() function
            byteBuf.WriteBytes(writeBytes, 0, 1);
            var retByte = byteBuf.ReadByte();
            Assert.AreEqual(writeBytes[0], retByte);

            // Verify clear() function
            byteBuf.Clear();
            Assert.AreEqual(0, byteBuf.ReaderIndex);
            Assert.AreEqual(0, byteBuf.WriterIndex);
        }

        [Test]
        public void TestWriteReadManagedBuf()
        {
            var byteBuf = managedBufPool.Allocate();
            Assert.AreEqual(IntPtr.Zero, byteBuf.UnsafeArray); // Verify the pointer of UnsafeArray is IntPtr.Zero on managed buffer.
            WriteReadByteBufTest(byteBuf);
            byteBuf.Release();
        }

        [Test]
        public void TestWriteReadUnsafeBuf()
        {
            if (!SocketFactory.IsRioSockSupported())
            {
                Assert.Ignore("Omitting due to missing Riosock.dll. It might caused by no VC++ build tool or running on an OS that not supports Windows RIO socket.");
            }

            var byteBuf = unsafeBufPool.Allocate();
            Assert.AreNotEqual(IntPtr.Zero, byteBuf.UnsafeArray); // Verify the point of UnsafeArray has value on unsafe buffer.
            WriteReadByteBufTest(byteBuf);
            byteBuf.Release();
        }

        [Test]
        public void TestInvalidByteBuf()
        {
            // Test invalid parameter to new ByteBuf.
            Assert.Throws<ArgumentOutOfRangeException>(() => new ByteBuf(null, -1, 1024));
            Assert.Throws<ArgumentOutOfRangeException>(() => new ByteBuf(null, 0, -1));
            Assert.Throws<ArgumentNullException>(() => new ByteBuf(null, 0, 1024));
            var byteBuf = managedBufPool.Allocate();
            Assert.Throws<ArgumentException>(() => new ByteBuf(byteBuf.ByteBufChunk, byteBuf.ByteBufChunk.Size - 1, 1024));
            byteBuf.Release();
            // Test function on disposed ByteBuf.
            Assert.IsFalse(byteBuf.IsWritable());
            Assert.Throws<ObjectDisposedException>(() => byteBuf.ReadByte());
            // Release double - nothing to do
            byteBuf.Release();
        }

        [Test]
        public void TestInvalidReadBytes()
        {
            var byteBuf = managedBufPool.Allocate();
            var readBytes = new byte[10];
            // Verify ReadBytes with invalid parameters
            Assert.Throws<ArgumentNullException>(() => byteBuf.ReadBytes(null, 0, 0));
            Assert.Throws<ArgumentOutOfRangeException>(() => byteBuf.ReadBytes(readBytes, -1, 1024));
            Assert.Throws<ArgumentOutOfRangeException>(() => byteBuf.ReadBytes(readBytes, 0, -1));
            // Verify ReadBytes with invalid boundary
            Assert.Throws<ArgumentException>(() => byteBuf.ReadBytes(readBytes, readBytes.Length - 1, readBytes.Length));
            Assert.Throws<IndexOutOfRangeException>(() => byteBuf.ReadBytes(readBytes, 0, readBytes.Length));
            byteBuf.Release();
        }

        [Test]
        public void TestInvalidWriteBytes()
        {
            var byteBuf = managedBufPool.Allocate();
            var writeBytes = new byte[2048];
            // Verify WriteBytes with invalid parameters
            Assert.Throws<ArgumentNullException>(() => byteBuf.WriteBytes(null, 0, 0));
            Assert.Throws<ArgumentOutOfRangeException>(() => byteBuf.WriteBytes(writeBytes, -1, 1024));
            Assert.Throws<ArgumentOutOfRangeException>(() => byteBuf.WriteBytes(writeBytes, 0, -1));
            // Verify WriteBytes with invalid boundary
            Assert.Throws<ArgumentException>(() => byteBuf.WriteBytes(writeBytes, writeBytes.Length - 1, writeBytes.Length));
            Assert.Throws<IndexOutOfRangeException>(() => byteBuf.WriteBytes(writeBytes, 0, 2048));
        }

        [Test]
        public void TestInvalidWriterReaderIndex()
        {
            var byteBuf = managedBufPool.Allocate();
            // Verify to set writer/reader index with a negative value.
            Assert.Throws<IndexOutOfRangeException>(() => byteBuf.WriterIndex = -1);
            Assert.Throws<IndexOutOfRangeException>(() => byteBuf.ReaderIndex = -1);

            // Verify to set writer/reader index with an overflow value.
            Assert.Throws<IndexOutOfRangeException>(() => byteBuf.ReaderIndex += 2);
            Assert.Throws<IndexOutOfRangeException>(() => byteBuf.WriterIndex += (managedBufPool.SegmentSize + 1));
            byteBuf.Release();
        }

        [Test]
        public void TestGetRioBufFromManagedBuf()
        {
            var byteBuf = managedBufPool.Allocate();
            // Verify to GetInputRioBuf()/GetOutputRioBuf() from a managed ByteBuf
            Assert.Throws<InvalidOperationException>(() => byteBuf.GetInputRioBuf());
            Assert.Throws<InvalidOperationException>(() => byteBuf.GetOutputRioBuf());
            byteBuf.Release();
        }
    }
}