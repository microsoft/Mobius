using System.IO;
using System.Collections.Generic;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;

using NUnit.Framework;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Network;

namespace AdapterTest
{
    /// <summary>
    /// Validates Accumulator implementation by start accumuator server
    /// simulate interactions between Scala side and accumuator server
    /// </summary>
    [TestFixture]
    public class AccumulatorTest
    {
        private SparkContext sc;
        private ISocketWrapper sock;


        [SetUp]
        public void TestInitialize()
        {
            sc = new SparkContext(null);
            sc.StartAccumulatorServer();

            // get accumulator server port and connect to accumuator server
            int serverPort = (sc.SparkContextProxy as MockSparkContextProxy).AccumulatorServerPort;
            sock = SocketFactory.CreateSocket();
            sock.Connect(IPAddress.Loopback, serverPort);
        }

        [TearDown]
        public void TestCleanUp()
        {
            sc.Stop();

            try
            {
                using (var s = sock.GetStream())
                {
                    int numUpdates = 0;
                    SerDe.Write(s, numUpdates);
                }
            }
            catch
            {
                // do nothing here
            }
            finally
            {
                sock.Close();
            }
        }

        /// <summary>
        /// test when no errors, accumulator server receives data as expected and exit with 0
        /// </summary>
        [Test]
        public void TestAccumuatorSuccess()
        {
            Accumulator<int> accumulator = sc.Accumulator<int>(0);

            using (var s = sock.GetStream())
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
            }
        }

        /// <summary>
        /// test when receive update for undefined accumulator
        /// </summary>
        [Test]
        public void TestUndefinedAccumuator()
        {
            using (var s = sock.GetStream())
            {
                // write numUpdates
                int numUpdates = 1;
                SerDe.Write(s, numUpdates);

                // write update
                int key = 1;
                int value = 1000;
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

                Assert.IsTrue(Accumulator.accumulatorRegistry.ContainsKey(update.Key));
                var accumulator = Accumulator.accumulatorRegistry[update.Key] as Accumulator<int>;
                Assert.AreEqual(accumulator.Value, value);
            }
        }
    }
}
