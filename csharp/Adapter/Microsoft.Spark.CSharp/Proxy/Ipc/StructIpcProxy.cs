// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    internal class StructTypeIpcProxy : IStructTypeProxy
    {
        private readonly JvmObjectReference jvmStructTypeReference;

        internal JvmObjectReference JvmStructTypeReference
        {
            get { return jvmStructTypeReference; }
        }

        internal StructTypeIpcProxy(JvmObjectReference jvmStructTypeReference)
        {
            this.jvmStructTypeReference = jvmStructTypeReference;
        }

        public List<IStructFieldProxy> GetStructTypeFields()
        {
            var fieldsReferenceList = SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStructTypeReference, "fields");
            return (fieldsReferenceList as List<JvmObjectReference>).Select(s => new StructFieldIpcProxy(s)).Cast<IStructFieldProxy>().ToList();
        }
    }

    internal class StructDataTypeIpcProxy : IStructDataTypeProxy
    {
        internal readonly JvmObjectReference jvmStructDataTypeReference;

        internal StructDataTypeIpcProxy(JvmObjectReference jvmStructDataTypeReference)
        {
            this.jvmStructDataTypeReference = jvmStructDataTypeReference;
        }

        public string GetDataTypeString()
        {
            return SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStructDataTypeReference, "toString").ToString();
        }

        public string GetDataTypeSimpleString()
        {
            return SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStructDataTypeReference, "simpleString").ToString();
        }
    }

    internal class StructFieldIpcProxy : IStructFieldProxy
    {
        private readonly JvmObjectReference jvmStructFieldReference;
        internal JvmObjectReference JvmStructFieldReference { get { return jvmStructFieldReference; } }

        internal StructFieldIpcProxy(JvmObjectReference jvmStructFieldReference)
        {
            this.jvmStructFieldReference = jvmStructFieldReference;
        }

        public string GetStructFieldName()
        {
            return SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStructFieldReference, "name").ToString();
        }

        public IStructDataTypeProxy GetStructFieldDataType()
        {
            return new StructDataTypeIpcProxy(new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStructFieldReference, "dataType").ToString()));
        }

        public bool GetStructFieldIsNullable()
        {
            return bool.Parse(SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStructFieldReference, "nullable").ToString());
        }
    }
}
