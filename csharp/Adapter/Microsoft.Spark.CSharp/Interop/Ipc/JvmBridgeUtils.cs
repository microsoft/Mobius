// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy.Ipc;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Utility methods for C#-JVM interaction
    /// </summary>
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal static class JvmBridgeUtils
    {
        public static JvmObjectReference GetJavaMap<K, V>(IEnumerable<KeyValuePair<K, V>> enumerable)
        {
            var jmap = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.Hashtable", new object[] { });
            if (enumerable != null)
            {
                foreach (var item in enumerable)
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jmap, "put", new object[] { item.Key, item.Value });
            }
            return jmap;
        }

        public static JvmObjectReference GetJavaHashMap<K, V>(IEnumerable<KeyValuePair<K, V>> enumerable)
        {
            var jmap = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.HashMap", new object[] { });
            if (enumerable != null)
            {
                foreach (var item in enumerable)
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jmap, "put", new object[] { item.Key, item.Value });
            }
            return jmap;
        }

        public static JvmObjectReference GetScalaMutableMap<K, V>(Dictionary<K, V> mapValues)
        {
            var hashMapReference = GetJavaHashMap(mapValues.Select(kvp => kvp));
            return new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.JvmBridgeUtils", "toMutableMap", new object[] { hashMapReference }).ToString());
        }

        public static JvmObjectReference GetJavaSet<T>(IEnumerable<T> enumerable)
        {
            var jset = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.HashSet", new object[] { });
            if (enumerable != null)
            {
                foreach (var item in enumerable)
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jset, "add", new object[] { item });
            }
            return jset;
        }

        public static JvmObjectReference GetJavaList<T>(IEnumerable<T> enumerable)
        {
            var jlist = SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.ArrayList", new object[] { });
            if (enumerable != null)
            {
                foreach (var item in enumerable)
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jlist, "add", new object[] { item });
            }
            return jlist;
        }

        public static JvmObjectReference GetJavaSeq<T>(IEnumerable<T> enumerable)
        {
            return new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", GetJavaList<T>(enumerable)));
        }
    }
}
