// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Sql;

namespace Microsoft.Spark.CSharp.Proxy
{
    internal interface IDataFrameReaderProxy
    {
        void Format(string source);
        void Schema(StructType schema);
        void Options(Dictionary<string,string> options);
        IDataFrameProxy Load();
        IDataFrameProxy Jdbc(string url, string table, string[] predicates, Dictionary<string,string> connectionProperties);
        IDataFrameProxy Jdbc(string url, string table, Dictionary<String, String> properties);
        IDataFrameProxy Jdbc(string url, string table, string columnName, string lowerBound, string upperBound,
            int numPartitions, Dictionary<String, String> connectionProperties);
        IDataFrameProxy Parquet(string[] paths);
        IDataFrameProxy Table(string tableName);
    }
}
