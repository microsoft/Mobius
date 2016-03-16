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
        IDataFrameReaderProxy Read();
        ISqlContextProxy NewSession();
        string GetConf(string key, string defaultValue);
        void SetConf(string key, string value);
        IDataFrameProxy CreateDataFrame(IRDDProxy rddProxy, IStructTypeProxy structTypeProxy);
        void RegisterDataFrameAsTable(IDataFrameProxy dataFrameProxy, string tableName);
        void DropTempTable(string tableName);
        IDataFrameProxy Table(string tableName);
        IDataFrameProxy Tables();
        IDataFrameProxy Tables(string databaseName);
        IEnumerable<string> TableNames();
        IEnumerable<string> TableNames(string databaseName);
        void CacheTable(string tableName);
        void UncacheTable(string tableName);
        void ClearCache();
        bool IsCached(string tableName);
        IDataFrameProxy ReadDataFrame(string path, StructType schema, Dictionary<string, string> options);
        IDataFrameProxy JsonFile(string path);
        IDataFrameProxy TextFile(string path, StructType schema, string delimiter);
        IDataFrameProxy TextFile(string path, string delimiter, bool hasHeader, bool inferSchema);
        IDataFrameProxy Sql(string query);
        void RegisterFunction(string name, byte[] command, string returnType);
    }
}