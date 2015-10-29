// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    internal class StatusTrackerIpcProxy : IStatusTrackerProxy
    {
        private JvmObjectReference jvmStatusTrackerReference;
        public StatusTrackerIpcProxy(JvmObjectReference jStatusTracker)
        {
            this.jvmStatusTrackerReference = jStatusTracker;
        }

        public int[] GetJobIdsForGroup(string jobGroup)
        {
            return (int[])SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStatusTrackerReference, "getJobIdsForGroup", new object[] { jobGroup });
        }
        public int[] GetActiveStageIds()
        {
            return (int[])SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStatusTrackerReference, "getActiveStageIds");
        }
        public int[] GetActiveJobsIds()
        {
            return (int[])SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStatusTrackerReference, "getActiveJobsIds");
        }

        public SparkJobInfo GetJobInfo(int jobId)
        {
            var jobInfoId = SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStatusTrackerReference, "getJobInfo", new object[] { jobId });
            if (jobInfoId == null)
                return null;

            JvmObjectReference jJobInfo = new JvmObjectReference((string)jobInfoId);
            int[] stageIds = (int[])SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jJobInfo, "stageIds");
            string status = SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jJobInfo, "status").ToString();

            return new SparkJobInfo(jobId, stageIds, status);
        }
        public SparkStageInfo GetStageInfo(int stageId)
        {
            var stageInfoId = SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmStatusTrackerReference, "getStageInfo", new object[] { stageId });
            if (stageInfoId == null)
                return null;

            JvmObjectReference jStageInfo = new JvmObjectReference((string)stageInfoId);
            int currentAttemptId = (int)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "currentAttemptId");
            int submissionTime = (int)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "submissionTime");
            string name = (string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "name");
            int numTasks = (int)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "numTasks");
            int numActiveTasks = (int)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "numActiveTasks");
            int numCompletedTasks = (int)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "numCompletedTasks");
            int numFailedTasks = (int)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jStageInfo, "numFailedTasks");

            return new SparkStageInfo(stageId, currentAttemptId, (long)submissionTime, name, numTasks, numActiveTasks, numCompletedTasks, numFailedTasks);
        }
    }
}
