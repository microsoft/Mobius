// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Behavior of the bridge used for the IPC interop between JVM and CLR
    /// </summary>
    public interface IJvmBridge : IDisposable
    {
        void Initialize(int portNo);
        JvmObjectReference CallConstructor(string className, params object[] parameters);
        object CallStaticJavaMethod(string className, string methodName, params object[] parameters);
        object CallStaticJavaMethod(string className, string methodName);
        object CallNonStaticJavaMethod(JvmObjectReference objectId, string methodName, params object[] parameters);
        object CallNonStaticJavaMethod(JvmObjectReference objectId, string methodName);
    }
}
