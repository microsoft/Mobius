// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// Schema of DataFrame
    /// </summary>
    public class StructType
    {
        private IStructTypeProxy structTypeProxy;

        internal IStructTypeProxy StructTypeProxy
        {
            get
            {
                return structTypeProxy;
            }
        }

        public List<StructField> Fields //TODO - avoid calling method everytime
        {
            get
            {
                var structTypeFieldJvmObjectReferenceList =
                    structTypeProxy.GetStructTypeFields();
                var structFieldList = new List<StructField>(structTypeFieldJvmObjectReferenceList.Count);
                structFieldList.AddRange(
                    structTypeFieldJvmObjectReferenceList.Select(
                        structTypeFieldJvmObjectReference => new StructField(structTypeFieldJvmObjectReference)));
                return structFieldList;
            }
        }

        internal StructType(IStructTypeProxy structTypeProxy)
        {
            this.structTypeProxy = structTypeProxy;
        }

        public static StructType CreateStructType(List<StructField> structFields)
        {
            return new StructType(SparkCLREnvironment.SparkCLRProxy.CreateStructType(structFields));
        }
    }

    /// <summary>
    /// Schema for DataFrame column
    /// </summary>
    public class StructField
    {
        private IStructFieldProxy structFieldProxy;

        internal IStructFieldProxy StructFieldProxy
        {
            get
            {
                return structFieldProxy;
            }
        }

        public string Name
        {
            get
            {
                return structFieldProxy.GetStructFieldName();
            }
        }

        public DataType DataType
        {
            get
            {
                return new DataType(structFieldProxy.GetStructFieldDataType());
            }
        }

        public bool IsNullable
        {
            get
            {
                return structFieldProxy.GetStructFieldIsNullable();
            }
        }

        internal StructField(IStructFieldProxy strucFieldProxy)
        {
            structFieldProxy = strucFieldProxy;
        }

        public static StructField CreateStructField(string name, string dataType, bool isNullable)
        {
            return new StructField(SparkCLREnvironment.SparkCLRProxy.CreateStructField(name, dataType, isNullable));
        }
    }

    public class DataType
    {
        private IStructDataTypeProxy structDataTypeProxy;

        internal IStructDataTypeProxy StructDataTypeProxy
        {
            get
            {
                return structDataTypeProxy;
            }
        }

        internal DataType(IStructDataTypeProxy structDataTypeProxy)
        {
            this.structDataTypeProxy = structDataTypeProxy;
        }

        public override string ToString()
        {
            return structDataTypeProxy.GetDataTypeString();
        }

        public string SimpleString()
        {
            return structDataTypeProxy.GetDataTypeSimpleString();
        }
    }
}