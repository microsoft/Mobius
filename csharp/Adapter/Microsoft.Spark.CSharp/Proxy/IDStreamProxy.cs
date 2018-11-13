// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.Proxy
{
    internal interface IDStreamProxy
    {
        int SlideDuration { get; }
        IDStreamProxy Window(int windowSeconds, int slideSeconds = 0);
        IDStreamProxy AsJavaDStream();
        void CallForeachRDD(byte[] func, string serializedMode);
        void Print(int num = 10);
        void Persist(StorageLevelType storageLevelType);
        void Checkpoint(long intervalMs);
        IRDDProxy[] Slice(long fromUnixTime, long toUnixTime);
    }
}
