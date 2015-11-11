// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Core
{
    public class StatusTracker
    {
        private readonly IStatusTrackerProxy statusTrackerProxy;
        internal StatusTracker(IStatusTrackerProxy statusTrackerProxy)
        {
            this.statusTrackerProxy = statusTrackerProxy;
        }

        /// <summary>
        /// Return a list of all known jobs in a particular job group.  If
        /// `jobGroup` is None, then returns all known jobs that are not
        /// associated with a job group.
        ///
        /// The returned list may contain running, failed, and completed jobs,
        /// and may vary across invocations of this method. This method does
        /// not guarantee the order of the elements in its result.
        /// </summary>
        /// <param name="jobGroup"></param>
        /// <returns></returns>
        public int[] GetJobIdsForGroup(string jobGroup)
        {
            return statusTrackerProxy.GetJobIdsForGroup(jobGroup);
        }

        /// <summary>
        /// Returns an array containing the ids of all active stages.
        /// </summary>
        /// <returns></returns>
        public int[] GetActiveStageIds()
        {
            return statusTrackerProxy.GetActiveStageIds();
        }

        /// <summary>
        /// Returns an array containing the ids of all active jobs.
        /// </summary>
        /// <returns></returns>
        public int[] GetActiveJobsIds()
        {
            return statusTrackerProxy.GetActiveJobsIds();
        }

        /// <summary>
        /// Returns a :class:`SparkJobInfo` object, or None if the job info
        /// could not be found or was garbage collected.
        /// </summary>
        /// <param name="jobId"></param>
        /// <returns></returns>
        public SparkJobInfo GetJobInfo(int jobId)
        {
            return statusTrackerProxy.GetJobInfo(jobId);
        }

        /// <summary>
        /// Returns a :class:`SparkStageInfo` object, or None if the stage
        /// info could not be found or was garbage collected.
        /// </summary>
        /// <param name="stageId"></param>
        /// <returns></returns>
        public SparkStageInfo GetStageInfo(int stageId)
        {
            return statusTrackerProxy.GetStageInfo(stageId);
        }
    }

    public class SparkJobInfo
    {
        readonly int jobId;
        readonly int[] stageIds;
        readonly string status;
        public SparkJobInfo(int jobId, int[] stageIds, string status)
        {
            this.jobId = jobId;
            this.stageIds = stageIds;
            this.status = status;
        }

        public int JobId { get { return jobId; } }
        public int[] StageIds { get { return stageIds; } }
        public string Status { get { return status; } }

    }

    public class SparkStageInfo
    {
        readonly int stageId;
        readonly int currentAttemptId;
        readonly long submissionTime;
        readonly string name;
        readonly int numTasks;
        readonly int numActiveTasks;
        readonly int numCompletedTasks;
        readonly int numFailedTasks;
        public SparkStageInfo(int stageId, int currentAttemptId, long submissionTime, string name, int numTasks, int numActiveTasks, int numCompletedTasks, int numFailedTasks)
        {
            this.stageId = stageId;
            this.currentAttemptId = currentAttemptId;
            this.submissionTime = submissionTime;
            this.name = name;
            this.numTasks = numTasks;
            this.numActiveTasks = numActiveTasks;
            this.numCompletedTasks = numCompletedTasks;
            this.numFailedTasks = numFailedTasks;
        }

        public int StageId { get { return stageId; } }
        public int CurrentAttemptId { get { return currentAttemptId; } }
        public long SubmissionTime { get { return submissionTime; } }
        public string Name { get { return name; } }
        public int NumTasks { get { return numTasks; } }
        public int NumActiveTasks { get { return numActiveTasks; } }
        public int NumCompletedTasks { get { return numCompletedTasks; } }
        public int NumFailedTasks { get { return numFailedTasks; } }
    }
}
