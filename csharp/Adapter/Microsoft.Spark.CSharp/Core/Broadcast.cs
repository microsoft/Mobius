// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Collections.Concurrent;

using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// A broadcast variable created with SparkContext.Broadcast().
    /// Access its value through Value.
    /// 
    /// var b = sc.Broadcast(new int[] {1, 2, 3, 4, 5})
    /// b.Value
    /// [1, 2, 3, 4, 5]
    /// sc.Parallelize(new in[] {0, 0}).FlatMap(x: b.Value).Collect()
    /// [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
    /// b.Unpersist()
    /// 
    /// See python implementation in broadcast.py, worker.py, PythonRDD.scala
    /// 
    /// </summary>
    [Serializable]
    public class Broadcast
    {
        /// <summary>
        /// A thread-safe static collection that is used to store registered broadcast objects.
        /// </summary>
        [NonSerialized]
        public static ConcurrentDictionary<long, Broadcast> broadcastRegistry = new ConcurrentDictionary<long, Broadcast>();

        /// <summary>
        /// A thread-safe static collection that is used to store the mapping between csharp broadcastId and JVM broadcastId.
        /// </summary>
        [NonSerialized]
        public static ConcurrentDictionary<long, long> csharpToJvmBidMap = new ConcurrentDictionary<long, long>();

        /// <summary>
        /// all broadcast variables created in driver
        /// </summary>
        [NonSerialized]
        public static List<Broadcast> broadcastVars = new List<Broadcast>();

        /// <summary>
        /// C# broadcastId that will be allocated to next broadcast variable.
        /// </summary>
        [NonSerialized]
        public static long nextCSharpBid = 0;

        [NonSerialized]
        internal string path;

        [NonSerialized]
        internal long jvmBid;  // broadcastId allocated by JVM side, this id may change after restart

        internal long csharpBid; // broadcastId allocated by C# side

        internal Broadcast() { }

        /// <summary>
        /// Initializes a new instance of Broadcast class with a specified path.
        /// </summary>
        /// <param name="path">The path that to be set.</param>
        public Broadcast(string path)
        {
            this.path = path;
        }

        /// <summary>
        /// Get value of the broadcast variable.
        /// This method is implemented by the inherited class
        /// </summary>
        internal virtual object GetValue()
        {
            throw new NotImplementedException();
        }

        internal static void DumpBroadcast<T>(T value, string path)
        {
            var formatter = new BinaryFormatter();
            using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Write))
            {
                formatter.Serialize(fs, value);
            }
        }
        internal static T LoadBroadcast<T>(string path)
        {
            var formatter = new BinaryFormatter();
            using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read))
            {
                return (T)formatter.Deserialize(fs);
            }
        }

        internal static void WriteToCheckpointData(CheckpointData checkpointData)
        {
            foreach (var b in broadcastVars)
            {
                checkpointData.broadcastVars[b.csharpBid] = b.GetValue();
            }
        }

        internal static void ReadFromCheckpointData(SparkContext sc, CheckpointData checkpointData)
        {
            foreach (var kvp in checkpointData.broadcastVars)
            {
                sc.Broadcast(kvp.Value, kvp.Key);
            }
        }
    }

    /// <summary>
    /// A generic version of <see cref="Broadcast"/> where the element can be specified.
    /// </summary>
    /// <typeparam name="T">The type of element in Broadcast</typeparam>
    [Serializable]
    public class Broadcast<T> : Broadcast
    {
        [NonSerialized]
        private readonly IBroadcastProxy broadcastProxy;
        [NonSerialized]
        private T value;
        [NonSerialized]
        private bool valueLoaded = false;

        internal Broadcast(SparkContext sparkContext, T value, long designatedCSharpBid = -1)
        {
            this.value = value;
            this.valueLoaded = true;
            path = Path.GetTempFileName();
            DumpBroadcast<T>(value, path);
            broadcastProxy = sparkContext.SparkContextProxy.ReadBroadcastFromFile(path, out jvmBid);
            if (designatedCSharpBid < 0)
            {
                csharpBid = nextCSharpBid;
                nextCSharpBid++;
            }
            else
            {
                csharpBid = designatedCSharpBid;
            }
            broadcastVars.Add(this);
        }

        /// <summary>
        /// Return the broadcasted value
        /// </summary>
        public T Value 
        {
            get
            {
                if (!valueLoaded)
                {
                    if (csharpToJvmBidMap.ContainsKey(csharpBid))
                    {
                        jvmBid = csharpToJvmBidMap[csharpBid];
                        if (broadcastRegistry.ContainsKey(jvmBid))
                        {
                            value = LoadBroadcast<T>(broadcastRegistry[jvmBid].path);
                            valueLoaded = true;
                        }
                        else
                        {
                            throw new ArgumentException(string.Format("Attempted to use jvmBid id {0} after it was destroyed.", jvmBid));
                        }
                    }
                    else
                    {
                        throw new ArgumentException(string.Format("Can't find csharpBid id {0}.", csharpBid));
                    }
                }

                return value;
            } 
        }

        /// <summary>
        /// Get value of the broadcast variable.
        /// </summary>
        internal override object GetValue()
        {
            return Value;
        }

        /// <summary>
        /// Delete cached copies of this broadcast on the executors.
        /// </summary>
        /// <param name="blocking"></param>
        public void Unpersist(bool blocking = false)
        {
            if (broadcastProxy == null)
                throw new ArgumentException("Broadcast can only be unpersisted in driver");
            broadcastProxy.Unpersist(blocking);
            File.Delete(path);
        }
    }
}
