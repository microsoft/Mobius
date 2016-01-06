// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.Proxy
{
    interface ISparkCLRProxy
    {
        ISparkContextProxy SparkContextProxy { get; }
        IStreamingContextProxy StreamingContextProxy { get; }
        ISparkConfProxy CreateSparkConf(bool loadDefaults = true);
        ISparkContextProxy CreateSparkContext(ISparkConfProxy conf);
        IStreamingContextProxy CreateStreamingContext(SparkContext sparkContext, long durationMs);
        IStreamingContextProxy CreateStreamingContext(string checkpointPath);
    }
}
