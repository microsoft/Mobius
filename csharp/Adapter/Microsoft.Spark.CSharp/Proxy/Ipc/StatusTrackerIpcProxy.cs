// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class StatusTrackerIpcProxy : IStatusTrackerProxy
    {
        private readonly JvmObjectReference jvmStatusTrackerReference;
        public StatusTrackerIpcProxy(JvmObjectReference jStatusTracker)
        {
            jvmStatusTrackerReference = jStatusTracker;
        }

        public int[] GetJobIdsForGroup(string jobGroup)
        {
            return (int[])SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStatusTrackerReference, "getJobIdsForGroup", new object[] { jobGroup });
        }
        public int[] GetActiveStageIds()
        {
            return (int[])SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStatusTrackerReference, "getActiveStageIds");
        }
        public int[] GetActiveJobsIds()
        {
            return (int[])SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStatusTrackerReference, "getActiveJobsIds");
        }

        public SparkJobInfo GetJobInfo(int jobId)
        {
            var jobInfoId = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStatusTrackerReference, "getJobInfo", new object[] { jobId });
            if (jobInfoId == null)
                return null;

            JvmObjectReference jJobInfo = new JvmObjectReference((string)jobInfoId);
            int[] stageIds = (int[])SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jJobInfo, "stageIds");
            string status = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jJobInfo, "status").ToString();

            return new SparkJobInfo(jobId, stageIds, status);
        }
        public SparkStageInfo GetStageInfo(int stageId)
        {
            var stageInfoId = SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmStatusTrackerReference, "getStageInfo", new object[] { stageId });
            if (stageInfoId == null)
                return null;

            JvmObjectReference jStageInfo = new JvmObjectReference((string)stageInfoId);
            int currentAttemptId = (int)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "currentAttemptId");
            int submissionTime = (int)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "submissionTime");
            string name = (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "name");
            int numTasks = (int)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "numTasks");
            int numActiveTasks = (int)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "numActiveTasks");
            int numCompletedTasks = (int)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "numCompletedTasks");
            int numFailedTasks = (int)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "numFailedTasks");

            return new SparkStageInfo(stageId, currentAttemptId, (long)submissionTime, name, numTasks, numActiveTasks, numCompletedTasks, numFailedTasks);
        }
    }
}
