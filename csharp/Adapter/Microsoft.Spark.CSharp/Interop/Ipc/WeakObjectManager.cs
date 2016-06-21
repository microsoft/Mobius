using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    using WeakReferenceObjectIdPair = KeyValuePair<WeakReference, string>;

    /// <summary>
    /// Release JVMObjectTracker oject reference.
    /// The reason is for the inter-operation from CSharp to Java :
    /// 1.Java-side: https://github.com/Microsoft/Mobius/blob/master/scala/src/main/org/apache/spark/api/csharp/CSharpBackendHandler.scala#L269
    ///     JVMObjectTracker keep a HashMap[String, Object] which is [id, Java-object]
    /// 2.CSharp-side :
    /// 1) JvmObjectReference remember the id : https://github.com/Microsoft/Mobius/blob/master/csharp/Adapter/Microsoft.Spark.CSharp/Interop/Ipc/JvmObjectReference.cs#L20 
    /// 2) So JvmBridge can call java object's method https://github.com/Microsoft/Mobius/blob/master/csharp/Adapter/Microsoft.Spark.CSharp/Interop/Ipc/JvmBridge.cs#L69
    /// 
    /// So potential memory leak can happen in JVMObjectTracker.
    /// To solve this, track the garbage collection in CSharp side, get the id, release JVMObjectTracker's HashMap. 
    /// </summary>
    internal interface IWeakObjectManager : IDisposable
    {
        TimeSpan CheckInterval { get; set; }

        void AddWeakRefereceObject(JvmObjectReference obj);

        /// <summary>
        /// Gets all weak object count including non-alive objects that wait for releasing.
        /// </summary>
        int GetReferencesCount();

        /// <summary>
        /// Gets alive weak object count
        /// </summary>
        /// <returns></returns>
        int GetAliveCount();
    }

    internal class WeakObjectManagerImpl : IWeakObjectManager
    {
        private static readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(WeakObjectManagerImpl));

        internal static TimeSpan DefaultCheckInterval = TimeSpan.FromSeconds(60);
        private TimeSpan checkInterval;

        /// <summary>
        /// Sleep time for checking thread
        /// </summary>
        public TimeSpan CheckInterval
        {
            get
            {
                return checkInterval;
            }
            set
            {
                checkInterval = value;
            }
        }

        /// <summary>
        /// Maximum running duration for checking thread each time
        /// </summary>
        private static readonly TimeSpan MaxReleasingDuration = TimeSpan.FromMilliseconds(100);

        private readonly ConcurrentQueue<WeakReferenceObjectIdPair> weakReferences = new ConcurrentQueue<WeakReferenceObjectIdPair>();

        private bool shouldKeepRunning = true;

        private IObjectReleaser objectReleaser = new JvmObjectReleaser();

        internal IObjectReleaser ObjectReleaser
        {
            set { objectReleaser = value; }
        }

        private Thread releaserThread;

        internal WeakObjectManagerImpl(TimeSpan checkIntervalTimeSpan)
        {
            checkInterval = checkIntervalTimeSpan;
            releaserThread = new Thread(RunReleaseObjectLoop) { IsBackground = true };
            releaserThread.Start();
        }

        internal WeakObjectManagerImpl() : this(DefaultCheckInterval) { }

        public int GetReferencesCount()
        {
            return weakReferences.Count;
        }

        private void RunReleaseObjectLoop()
        {
            logger.LogDebug("Checking objects thread start ...");
            while (shouldKeepRunning)
            {
                ReleseGarbageCollectedObjects();
                Thread.Sleep(CheckInterval);
            }

            logger.LogDebug("Checking objects thread stopped.");
        }

        ~WeakObjectManagerImpl()
        {
            Dispose();
        }

        public void AddWeakRefereceObject(JvmObjectReference obj)
        {
            if (obj == null || string.IsNullOrEmpty(obj.Id))
            {
                logger.LogWarn("Not add null weak object or id : {0}", obj);
                return;
            }

            weakReferences.Enqueue(new WeakReferenceObjectIdPair(new WeakReference(obj), obj.ToString()));
        }

        private void ReleseGarbageCollectedObjects()
        {
            var count = weakReferences.Count;
            if (count == 0)
            {
                logger.LogDebug("check begin : quit as weakReferences.Count = 0");
                return;
            }

            var beginTime = DateTime.Now;
            var endTime = beginTime + MaxReleasingDuration;

            logger.LogDebug("check begin : weakReferences.Count = {0}, will stop checking at the latest: {1}", count, endTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            int garbageCount;
            var aliveList = ReleseGarbageCollectedObjects(endTime, out garbageCount);

            var timeReleaseGarbage = DateTime.Now;
            aliveList.ForEach(item => weakReferences.Enqueue(item));
            var timeStoreAlive = DateTime.Now;

            logger.LogInfo("check end : released {0} garbage, remain {1} alive, used {2} ms : release garbage used {3} ms, store alive used {4} ms",
                    garbageCount, weakReferences.Count, (DateTime.Now - beginTime).TotalMilliseconds,
                    (timeReleaseGarbage - beginTime).TotalMilliseconds,
                    (timeStoreAlive - timeReleaseGarbage).TotalMilliseconds
                );
        }

        private List<WeakReferenceObjectIdPair> ReleseGarbageCollectedObjects(DateTime endTime, out int garbageCount)
        {
            var aliveList = new List<WeakReferenceObjectIdPair>();
            garbageCount = 0;
            WeakReferenceObjectIdPair weakReferenceObjectIdPair;

            while (weakReferences.TryDequeue(out weakReferenceObjectIdPair))
            {
                var weakRef = weakReferenceObjectIdPair.Key;
                if (weakRef.IsAlive)
                {
                    aliveList.Add(weakReferenceObjectIdPair);
                }
                else
                {
                    objectReleaser.ReleaseObject(weakReferenceObjectIdPair.Value);
                    garbageCount++;
                }

                if (DateTime.Now > endTime)
                {
                    logger.LogDebug("Stop releasing as exceeded allowed time : {0}", endTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                    break;
                }
            }

            return aliveList;
        }

        /// <summary>
        /// It can be an expensive operation. ** Do not use ** unless there is a real need for this method
        /// </summary>
        /// <returns></returns>
        public int GetAliveCount()
        {
            //copying to get alive count at the time of this method call
            var copiedList = new Queue<WeakReferenceObjectIdPair>(weakReferences);
            var count = 0;
            foreach (var weakReference in copiedList)
            {
                if (weakReference.Key.IsAlive)
                {
                    count++;
                }
            }

            return count;
        }

        public virtual void Dispose()
        {
            logger.LogInfo("Dispose {0}", this.GetType());
            shouldKeepRunning = false;
        }
    }

    internal interface IObjectReleaser
    {
        void ReleaseObject(string objId);
    }

    internal class JvmObjectReleaser : IObjectReleaser
    {
        private const string ReleaseHandler = "SparkCLRHandler";
        private const string ReleaseMethod = "rm";

        public void ReleaseObject(string objId)
        {
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod(ReleaseHandler, ReleaseMethod, objId);
        }
    }
}