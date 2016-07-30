using System;
using System.IO;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Network;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class SocketStreamTest
    {
        /// <summary>
        /// Only test invalid case.
        /// The positive case verified on SocketWrapperTest
        /// </summary>
        [Test]
        public void TestInvalidSocketStream()
        {
            Assert.Throws<ArgumentNullException>(() => new SocketStream((SaeaSocketWrapper)null));
            Assert.Throws<ArgumentNullException>(() => new SocketStream((RioSocketWrapper)null));

            Environment.SetEnvironmentVariable(ConfigurationService.CSharpSocketTypeEnvName, "Saea");
            SocketFactory.SocketWrapperType = SocketWrapperType.None;

            using (var socket = SocketFactory.CreateSocket())
            using (var stream = new SocketStream((SaeaSocketWrapper)socket))
            {
                // Verify SocketStream
                Assert.IsTrue(stream.CanRead);
                Assert.IsTrue(stream.CanWrite);
                Assert.IsFalse(stream.CanSeek);
                long lengh = 10;
                Assert.Throws<NotSupportedException>(() => lengh = stream.Length);
                Assert.Throws<NotSupportedException>(() => stream.Position = lengh);
                Assert.Throws<NotSupportedException>(() => lengh = stream.Position);
                Assert.Throws<NotSupportedException>(() => stream.Seek(lengh, SeekOrigin.Begin));
                Assert.Throws<NotSupportedException>(() => stream.SetLength(lengh));
            }

            Environment.SetEnvironmentVariable(ConfigurationService.CSharpSocketTypeEnvName, string.Empty);
            SocketFactory.SocketWrapperType = SocketWrapperType.None;
        }
    }
}