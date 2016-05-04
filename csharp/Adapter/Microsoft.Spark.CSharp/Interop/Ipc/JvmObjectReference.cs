// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Runtime.CompilerServices;
using Microsoft.Spark.CSharp.Proxy.Ipc;

[assembly: InternalsVisibleTo("Microsoft.Spark.CSharp.Utils")]
namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Reference to object created in JVM
    /// </summary>
    [Serializable]
    internal class JvmObjectReference
    {
        public string Id { get; private set; }
        private DateTime creationTime;

        public JvmObjectReference(string jvmReferenceId)
        {
            Id = jvmReferenceId;
            creationTime = DateTime.UtcNow;
        }

        public override string ToString()
        {
            return Id;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;

            var jvmObjectReference = obj as JvmObjectReference;
            if (jvmObjectReference != null)
            {
                return Id.Equals(jvmObjectReference.Id);
            }

            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public string GetDebugInfo()
        {
            var javaObjectReferenceForClassObject = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(this, "getClass").ToString());
            var className = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(javaObjectReferenceForClassObject, "getName").ToString();
            return string.Format("Java object reference id={0}, type name={1}, creation time (UTC)={2}", Id, className, creationTime.ToString("o"));
        }
    }
}
