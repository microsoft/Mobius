// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Proxy;
using NUnit.Framework;
using Moq;

namespace AdapterTest
{
    /// <summary>
    /// Validates interaction between StatusTracker and its proxies
    /// </summary>
    [TestFixture]
    public class StatusTrackerTest
    {
        /// <summary>
        /// Test 'GetJobIdsForGroup', 'GetActiveJobsIds' and 'GetActiveStageIds' methods.
        /// </summary>
        [Test]
        public void TestStatusTrackerMethods()
        {
            // arrange
            Mock<IStatusTrackerProxy> statusTrackerProxy = new Mock<IStatusTrackerProxy>();

            // GetJobIdsForGroup
            const string jobGroup = "group-0";
            int[] expectedJobIds = new[] { 1001, 1002, 1003 };
            statusTrackerProxy.Setup(m => m.GetJobIdsForGroup(It.IsAny<string>())).Returns(expectedJobIds);

            // GetActiveJobsIds
            int[] expectedActiveJobIds = new[] { 1001, 1003 };
            statusTrackerProxy.Setup(m => m.GetActiveJobsIds()).Returns(expectedActiveJobIds);

            // GetActiveStageIds
            int[] expectedActiveStageIds = new[] { 666, 667, 668 };
            statusTrackerProxy.Setup(m => m.GetActiveStageIds()).Returns(expectedActiveStageIds);

            var tracker = new StatusTracker(statusTrackerProxy.Object);

            // act
            var jobIds = tracker.GetJobIdsForGroup(jobGroup);
            var activeJobIds = tracker.GetActiveJobsIds();
            var activeStageIds = tracker.GetActiveStageIds();

            // assert
            // GetJobIdsForGroup
            Assert.IsNotNull(jobIds);
            Assert.AreEqual(expectedJobIds, jobIds);

            // GetActiveJobsIds
            Assert.IsNotNull(activeJobIds);
            Assert.AreEqual(expectedActiveJobIds, activeJobIds);

            // GetActiveStageIds
            Assert.IsNotNull(activeStageIds);
            Assert.AreEqual(expectedActiveStageIds, activeStageIds);
        }

        /// <summary>
        /// Test StatusTracker.GetJobInfo() and methods of class SparkJobInfo.
        /// </summary>
        [Test]
        public void TestSparkJobInfo(
            [Values(JobExecutionStatus.Failed, JobExecutionStatus.Running, JobExecutionStatus.Succeeded, JobExecutionStatus.Unknown)] JobExecutionStatus status
            )
        {
            const int jobId = 65536;
            int[] stageIds = new[] { 100, 102, 104 };

            // arrange
            Mock<IStatusTrackerProxy> statusTrackerProxy = new Mock<IStatusTrackerProxy>();
            var expectedJobInfo = new SparkJobInfo(jobId, stageIds, status);
            statusTrackerProxy.Setup(m => m.GetJobInfo(It.IsAny<int>())).Returns(expectedJobInfo);
            var tracker = new StatusTracker(statusTrackerProxy.Object);

            // act
            SparkJobInfo jobInfo = tracker.GetJobInfo(jobId);

            // assert
            Assert.IsNotNull(jobInfo);
            Assert.AreEqual(jobId, jobInfo.JobId);
            Assert.AreEqual(stageIds, jobInfo.StageIds);
            Assert.AreEqual(status, jobInfo.Status);
        }

        /// <summary>
        /// Test StatusTracker.GetStageInfo() and methods of class SparkStageInfo.
        /// </summary>
        [Test]
        public void TestSparkStageInfo()
        {
            const int stageId = 65536;
            const int currentAttemptId = 1;
            long submissionTime = DateTime.Now.Millisecond;
            const string name = "name-1";
            const int numTasks = 20;
            const int numActiveTasks = 10;
            const int numCompletedTasks = 5;
            const int numFailedTasks = 0;

            // arrange
            Mock<IStatusTrackerProxy> statusTrackerProxy = new Mock<IStatusTrackerProxy>();
            var expectedStageInfo = new SparkStageInfo(stageId, currentAttemptId, submissionTime, name, numTasks, numActiveTasks, numCompletedTasks, numFailedTasks);
            statusTrackerProxy.Setup(m => m.GetStageInfo(It.IsAny<int>())).Returns(expectedStageInfo);
            var tracker = new StatusTracker(statusTrackerProxy.Object);

            // act
            SparkStageInfo stageInfo = tracker.GetStageInfo(stageId);

            // assert
            Assert.IsNotNull(stageInfo);
            Assert.AreEqual(stageId, stageInfo.StageId);
            Assert.AreEqual(currentAttemptId, stageInfo.CurrentAttemptId);
            Assert.AreEqual(submissionTime, stageInfo.SubmissionTime);
            Assert.AreEqual(name, stageInfo.Name);
            Assert.AreEqual(numTasks, stageInfo.NumTasks);
            Assert.AreEqual(numActiveTasks, stageInfo.NumActiveTasks);
            Assert.AreEqual(numCompletedTasks, stageInfo.NumCompletedTasks);
            Assert.AreEqual(numFailedTasks, stageInfo.NumFailedTasks);
        }
    }
}
