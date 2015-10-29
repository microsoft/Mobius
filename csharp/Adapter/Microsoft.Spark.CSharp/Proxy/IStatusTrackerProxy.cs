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
    internal interface IStatusTrackerProxy
    {
        int[] GetJobIdsForGroup(string jobGroup);
        int[] GetActiveStageIds();
        int[] GetActiveJobsIds();
        SparkJobInfo GetJobInfo(int jobId);
        SparkStageInfo GetStageInfo(int stageId);
    }
}
