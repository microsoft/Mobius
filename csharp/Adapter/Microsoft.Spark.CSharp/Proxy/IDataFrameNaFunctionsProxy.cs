// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Proxy
{
    interface IDataFrameNaFunctionsProxy
    {
        IDataFrameProxy Drop(int minNonNulls, string[] cols);
        IDataFrameProxy Fill(double value, string[] cols);
        IDataFrameProxy Fill(string value, string[] cols);
        IDataFrameProxy Fill(Dictionary<string, object> valueMap);
        IDataFrameProxy Replace<T>(string col, Dictionary<T, T> replacement);
        IDataFrameProxy Replace<T>(string[] cols, Dictionary<T, T> replacement);
    }
}
