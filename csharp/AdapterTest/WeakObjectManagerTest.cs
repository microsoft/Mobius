using System;
using System.Threading;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class WeakObjectManagerTest
    {
        [Test]
        public void TestJVMObjectRelease()
        {
            //make weak object manager wait for 2 secs for initial validation before start releasing objects
            var weakObjectManager = new WeakObjectManagerImpl(TimeSpan.FromSeconds(2)) { ObjectReleaser = new MockObjectReleaser() };
            //reset WeakObjectManager for validation - this is to avoid side effects *from* other tests
            SparkCLREnvironment.WeakObjectManager = weakObjectManager;

            var waitEndTime = DateTime.Now + TimeSpan.FromSeconds(4);

            //no items added
            Assert.AreEqual(0, weakObjectManager.GetReferencesCount());

            var totalItemCount = 200;
            for (var k = 1; k <= totalItemCount; k++)
            {
                //each object adds itself to WeakObjectManager
                var obj = new JvmObjectReference(k.ToString());
            }

            //all items added should be available
            Assert.AreEqual(totalItemCount, weakObjectManager.GetReferencesCount());

            //reset check interval to start releasing objects
            weakObjectManager.CheckInterval = TimeSpan.FromMilliseconds(200);

            GC.Collect();
            GC.WaitForPendingFinalizers();

            //reset check interval to default
            weakObjectManager.CheckInterval = WeakObjectManagerImpl.DefaultCheckInterval;

            var remainingTimeToWait = waitEndTime - DateTime.Now;
            if (remainingTimeToWait.TotalMilliseconds > 0)
            {
                Thread.Sleep(remainingTimeToWait);
            }

            var countAfterReleasingObjects = weakObjectManager.GetReferencesCount();
            var aliveCount = weakObjectManager.GetAliveCount();
            //validate that some items are released
            Assert.AreEqual(1, countAfterReleasingObjects);
            Assert.IsTrue(countAfterReleasingObjects < totalItemCount);
            //validate that unreleased items are alive items
            Assert.AreEqual(0, countAfterReleasingObjects - aliveCount);


        }
    }

    class MockObjectReleaser : IObjectReleaser
    {
        public void ReleaseObject(string objId)
        {
            //do nothing
        }
    }
}