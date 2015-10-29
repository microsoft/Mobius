// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Sql;

namespace Microsoft.Spark.CSharp.Proxy
{
    internal interface ISqlContextProxy
    {
        void CreateSqlContext(ISparkContextProxy sparkContextProxy);
        StructField CreateStructField(string name, string dataType, bool isNullable);
        StructType CreateStructType(List<StructField> fields);
        IDataFrameProxy ReaDataFrame(string path, StructType schema, Dictionary<string, string> options);
        IDataFrameProxy JsonFile(string path);
        IDataFrameProxy TextFile(string path, StructType schema, string delimiter);
        IDataFrameProxy TextFile(string path, string delimiter, bool hasHeader, bool inferSchema);
        IDataFrameProxy Sql(string query);
    }
}