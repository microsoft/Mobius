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
    interface ISparkCLRProxy
    {
        ISparkContextProxy SparkContextProxy { get; }
        ISparkConfProxy CreateSparkConf(bool loadDefaults = true);
        ISparkContextProxy CreateSparkContext(ISparkConfProxy conf);
        IStructFieldProxy CreateStructField(string name, string dataType, bool isNullable);
        IStructTypeProxy CreateStructType(List<StructField> fields);
        IDStreamProxy CreateCSharpDStream(IDStreamProxy jdstream, byte[] func, string deserializer);
        IDStreamProxy CreateCSharpTransformed2DStream(IDStreamProxy jdstream, IDStreamProxy jother, byte[] func, string deserializer, string deserializerOther);
        IDStreamProxy CreateCSharpReducedWindowedDStream(IDStreamProxy jdstream, byte[] func, byte[] invFunc, int windowSeconds, int slideSeconds, string deserializer);
        IDStreamProxy CreateCSharpStateDStream(IDStreamProxy jdstream, byte[] func, string deserializer);
    }
}
