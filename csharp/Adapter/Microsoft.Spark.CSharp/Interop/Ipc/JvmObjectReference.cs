// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

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

        public string GetDebugInfo()
        {
            var javaObjectReferenceForClassObject = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(this, "getClass").ToString());
            var className = SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(javaObjectReferenceForClassObject, "getName").ToString();
            return string.Format("Java object reference id={0}, type name={1}, creation time (UTC)={2}", Id, className, creationTime.ToString("o"));
        }
    }
}
