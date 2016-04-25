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
    /// <summary>
    /// Low-level status reporting APIs for monitoring job and stage progress.
    /// </summary>
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

    /// <summary>
    /// SparkJobInfo represents a job information of Spark
    /// </summary>
    public class SparkJobInfo
    {
        readonly int jobId;
        readonly int[] stageIds;
        readonly string status;

        /// <summary>
        /// Initializes a SparkJobInfo instance with a given job Id, stage Ids, and status
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="stageIds"></param>
        /// <param name="status"></param>
        public SparkJobInfo(int jobId, int[] stageIds, string status)
        {
            this.jobId = jobId;
            this.stageIds = stageIds;
            this.status = status;
        }

        /// <summary>
        /// Gets the Id of this Spark job
        /// </summary>
        public int JobId { get { return jobId; } }

        /// <summary>
        /// Gets the stage Ids of this Spark job
        /// </summary>
        public int[] StageIds { get { return stageIds; } }

        /// <summary>
        /// Gets the status of this Spark job
        /// </summary>
        public string Status { get { return status; } }

    }

    /// <summary>
    /// SparkJobInfo represents a stage information of Spark
    /// </summary>
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

        /// <summary>
        /// Initializes a SparkStageInfo instance with given values
        /// </summary>
        /// <param name="stageId">The stage Id</param>
        /// <param name="currentAttemptId">The current attempt Id</param>
        /// <param name="submissionTime">The submission time</param>
        /// <param name="name">The name of this stage</param>
        /// <param name="numTasks">The number of tasks</param>
        /// <param name="numActiveTasks">The number of active tasks</param>
        /// <param name="numCompletedTasks">The number of completed tasks</param>
        /// <param name="numFailedTasks">The number of failed tasks</param>
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

        /// <summary>
        /// Gets the stage Id of this SparkStageInfo
        /// </summary>
        public int StageId { get { return stageId; } }

        /// <summary>
        /// Gets the current attempt Id of this SparkStageInfo
        /// </summary>
        public int CurrentAttemptId { get { return currentAttemptId; } }

        /// <summary>
        /// Gets the submission time of this SparkStageInfo
        /// </summary>
        public long SubmissionTime { get { return submissionTime; } }

        /// <summary>
        /// Gets the name of this SparkStageInfo
        /// </summary>
        public string Name { get { return name; } }

        /// <summary>
        /// Gets the number of tasks of this SparkStageInfo
        /// </summary>
        public int NumTasks { get { return numTasks; } }

        /// <summary>
        /// Gets the number of active tasks of this SparkStageInfo
        /// </summary>
        public int NumActiveTasks { get { return numActiveTasks; } }

        /// <summary>
        /// Gets the number of completed tasks of this SparkStageInfo
        /// </summary>
        public int NumCompletedTasks { get { return numCompletedTasks; } }

        /// <summary>
        /// Gets the number of failed tasks of this SparkStageInfro
        /// </summary>
        public int NumFailedTasks { get { return numFailedTasks; } }
    }
}
