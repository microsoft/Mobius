using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters.Binary;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Interop.Ipc;

using NUnit.Framework;
using Moq;
using AdapterTest.Mocks;

namespace AdapterTest
{
    /// <summary>
    /// Validates Accumulator implementation by start accumuator server
    /// simulate interactions between Scala side and accumuator server
    /// </summary>
    [TestFixture]
    public class AccumulatorTest
    {

        /// <summary>
        /// test when no errors, accumuator server receives data as expected and exit with 0
        /// </summary>
        [Test]
        public void TestAccumuatorSuccess()
        {
            var sc = new SparkContext(null);
            Accumulator<int> accumulator = sc.Accumulator<int>(0);

            // get accumulator server port and connect to accumuator server
            int serverPort = (sc.SparkContextProxy as MockSparkContextProxy).AccumulatorServerPort;
            var sock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            sock.Connect(IPAddress.Loopback, serverPort);

            using (var s = new NetworkStream(sock))
            {
                // write numUpdates
                int numUpdates = 1;
                SerDe.Write(s, numUpdates);

                // write update
                int key = 0;
                int value = 100;
                KeyValuePair<int, dynamic> update = new KeyValuePair<int, dynamic>(key, value);
                var ms = new MemoryStream();
                var formatter = new BinaryFormatter();
                formatter.Serialize(ms, update);
                byte[] sendBuffer = ms.ToArray();
                SerDe.Write(s, sendBuffer.Length);
                SerDe.Write(s, sendBuffer);

                s.Flush();
                byte[] receiveBuffer = new byte[1];
                s.Read(receiveBuffer, 0, 1);

                Assert.AreEqual(accumulator.Value, value);

                // try to let service side to close gracefully
                sc.Stop();
                try
                {
                    numUpdates = 0;
                    SerDe.Write(s, numUpdates);
                }
                catch
                {
                    // do nothing here
                }
            }

            sock.Close();
        }
    }
}
