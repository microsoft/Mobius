// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Interface for collect operation on RDD
    /// </summary>
    interface IRDDCollector
    {
        IEnumerable<dynamic> Collect(int port, SerializedMode serializedMode, Type type);
    }
}
