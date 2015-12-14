// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;

namespace Microsoft.Spark.CSharp.Proxy
{
    internal interface IDataFrameWriterProxy
    {
        void Mode(string saveMode);
        void Format(string source);
        void Options(Dictionary<string, string> options);
        void PartitionBy(params string[] colNames);
        void Save();
        void InsertInto(string tableName);
        void SaveAsTable(string tableName);
        void Jdbc(string url, string table, Dictionary<string, string> properties);
    }
}
