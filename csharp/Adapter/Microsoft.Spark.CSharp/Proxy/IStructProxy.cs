// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Proxy
{
    interface IStructTypeProxy
    {
        List<IStructFieldProxy> GetStructTypeFields();
        string ToJson();
    }

    interface IStructDataTypeProxy
    {
        string GetDataTypeString();
        string GetDataTypeSimpleString();
    }

    interface IStructFieldProxy
    {
        string GetStructFieldName();
        IStructDataTypeProxy GetStructFieldDataType();
        bool GetStructFieldIsNullable();
    }
}
