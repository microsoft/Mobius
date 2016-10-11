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
    internal interface IUdfRegistration { }

    interface ISparkSessionProxy
    {
        ISqlContextProxy SqlContextProxy { get; }
        IUdfRegistration Udf { get; }
        ICatalogProxy GetCatalog();
        IDataFrameReaderProxy Read();
        ISparkSessionProxy NewSession();
        IDataFrameProxy CreateDataFrame(IRDDProxy rddProxy, IStructTypeProxy structTypeProxy);
        IDataFrameProxy Table(string tableName);
        IDataFrameProxy Sql(string query);
        void Stop();
    }
}
