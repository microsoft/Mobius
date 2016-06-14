using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy.Ipc;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    using System.Text.RegularExpressions;
    using Services;

    using ObjectAndId = KeyValuePair<WeakReference, string>;

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
        void AddWeakRefereceObject(JvmObjectReference obj);
    }

    internal class WeakObjectManager : IWeakObjectManager
    {
        private const string ReleaseHandler = "SparkCLRHandler";
        private const string ReleaseMethod = "rm";

        private static readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(WeakObjectManager));

        private ConcurrentQueue<ObjectAndId> weakReferences = new ConcurrentQueue<ObjectAndId>();

        private volatile bool keepRunning = true;

        private const int sleepInterval = 60 * 1000;
        private static readonly TimeSpan MaxReleasingDuration = TimeSpan.FromMilliseconds(100);


        #region Singleton WeakObjectManager

        private static IWeakObjectManager WeakObjManager = new WeakObjectManager();

        public static IWeakObjectManager GetWeakObjectManager()
        {
            return WeakObjManager;
        }

        #endregion

        private WeakObjectManager()
        {
            Init();
        }

        protected void Init()
        {
            var thread = new Thread(() =>
            {
                logger.LogDebug("Checking objects thread start ...");
                while (keepRunning)
                {
                    CheckReleasedObject();
                    Thread.Sleep(sleepInterval);
                }

                logger.LogDebug("Checking objects thread stopped.");
            });

            thread.IsBackground = true;
            thread.Start();
        }

        ~WeakObjectManager()
        {
            Dispose();
        }

        protected void ReleaseObject(string objId)
        {
            SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod(ReleaseHandler, ReleaseMethod, objId);
        }

        public void AddWeakRefereceObject(JvmObjectReference obj)
        {
            if (obj == null || string.IsNullOrEmpty(obj.Id))
            {
                logger.LogWarn("Not add null weak object or id : {0}", obj);
                return;
            }

            weakReferences.Enqueue(new ObjectAndId(new WeakReference(obj), obj.ToString()));
        }

        protected void CheckReleasedObject()
        {
            if (weakReferences.Count == 0)
            {
                logger.LogDebug("CheckReleasedObject begin : weakReferences.Count = {0}", weakReferences.Count);
                return;
            }

            var beginTime = DateTime.Now;
            var endTime = beginTime + MaxReleasingDuration;

            logger.LogDebug("CheckReleasedObject begin : weakReferences.Count = {0}, will stop checking at the latest: {1}", weakReferences.Count, endTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            var aliveList = new List<ObjectAndId>();
            var garbageCount = 0;
            ObjectAndId refId;
            while (weakReferences.TryDequeue(out refId))
            {
                var weakRef = refId.Key;
                if (weakRef.IsAlive)
                {
                    aliveList.Add(refId);
                }
                else
                {
                    ReleaseObject(refId.Value);
                    garbageCount++;
                }

                if (DateTime.Now > endTime)
                {
                    logger.LogDebug("Stop releasing as exceeded allowed time : {0}", endTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                    break;
                }
            }

            var timeReleaseGarbage = DateTime.Now;
            aliveList.ForEach(item => weakReferences.Enqueue(item));
            var timeStoreAlive = DateTime.Now;

            logger.LogInfo("CheckReleasedObject end : released {0} garbage, remain {1} alive, used {2} ms : release garbage used {3} ms, store alive used {4} ms",
                    garbageCount, weakReferences.Count, (DateTime.Now - beginTime).TotalMilliseconds,
                    (timeReleaseGarbage - beginTime).TotalMilliseconds,
                    (timeStoreAlive - timeReleaseGarbage).TotalMilliseconds
                );
        }

        public virtual void Dispose()
        {
            logger.LogInfo("Dispose {0}", this.GetType());
            keepRunning = false;
        }
    }
}
