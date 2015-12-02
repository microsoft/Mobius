// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

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
        [NonSerialized]
        public static Dictionary<long, Broadcast> broadcastRegistry = new Dictionary<long, Broadcast>();
        [NonSerialized]
        internal string path;

        internal long broadcastId;
        internal Broadcast() { }
        public Broadcast(string path)
        {
            this.path = path;
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
    }
    [Serializable]
    public class Broadcast<T> : Broadcast
    {
        [NonSerialized]
        private readonly IBroadcastProxy broadcastProxy;
        [NonSerialized]
        private T value;

        internal Broadcast(SparkContext sparkContext, T value)
        {
            this.value = value;
            path = Path.GetTempFileName();
            DumpBroadcast<T>(value, path);
            broadcastProxy = sparkContext.SparkContextProxy.ReadBroadcastFromFile(path, out broadcastId);
        }

        /// <summary>
        /// Return the broadcasted value
        /// </summary>
        public T Value 
        { 
            get 
            {
                if (value == null)
                {
                    if (broadcastRegistry.ContainsKey(broadcastId))
                        value = LoadBroadcast<T>(broadcastRegistry[broadcastId].path);
                    else
                        throw new ArgumentException(string.Format("Attempted to use broadcast id {0} after it was destroyed.", broadcastId));
                }
                return value; 
            } 
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
