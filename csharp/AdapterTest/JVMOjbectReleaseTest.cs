using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Streaming;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class JVMOjbectReleaseTest
    {
        /// <summary>
        /// In accordance with WeakObjectManager
        /// </summary>
        private static readonly TimeSpan CheckInterval = TimeSpan.FromSeconds(60);

        [Test]
        public void TestJVMObjectRelease()
        {
            var weakObjectManager = WeakObjectManager.GetWeakObjectManager();

            var waitEndTime = DateTime.Now + CheckInterval + TimeSpan.FromSeconds(1);

            var countList = new List<int>();
            for (var k = 0; k < 2; k++)
            {
                StartOneTest();
                countList.Add(weakObjectManager.Count);
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            var remainWaiting = waitEndTime - DateTime.Now;
            if (remainWaiting.TotalMilliseconds > 0)
            {
                Thread.Sleep(remainWaiting);
            }

            var finalCount = weakObjectManager.Count;
            Assert.AreEqual(0, finalCount);
        }


        private void StartOneTest()
        {
            var sc = new SparkContext(null);
            var ssc = new StreamingContext(sc, 1000);
            Assert.IsNotNull((ssc.streamingContextProxy as MockStreamingContextProxy));

            ssc.Start();
            ssc.Remember(1000);
            ssc.Checkpoint(Path.GetTempPath());
            var textFile = ssc.TextFileStream(Path.GetTempPath());
            Assert.IsNotNull(textFile.DStreamProxy);

            var socketStream = ssc.SocketTextStream(IPAddress.Loopback.ToString(), 12345);
            Assert.IsNotNull(socketStream.DStreamProxy);

            var lines = sc.TextFile(Path.GetTempFileName());
            var words = lines.FlatMap(line => line.Split(' ', '\t', '\r', '\n'));
            var wordCount = words.Count();

            ssc.AwaitTerminationOrTimeout(2000);
            ssc.Stop();
            sc.Stop();
        }
    }
}
