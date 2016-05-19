/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package org.apache.spark.streaming.api.csharp

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{StreamingContext, Duration, Time}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.csharp.SparkCLRFunSuite

import scala.reflect.ClassTag

private class MockSparkContext(config: SparkConf) extends SparkContext(config) {
  var numParallelJobs = 0
  override def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    numParallelJobs = numParallelJobs + 1
    Array.empty
  }
}

private class MockCSharpStateDStream(
    parent: DStream[Array[Byte]],
    reduceFunc: Array[Byte],
    serializationMode: String,
    serializationMode2: String)
  extends CSharpStateDStream(parent, reduceFunc, serializationMode, serializationMode2) {

  override def dependencies: List[DStream[_]] = List.empty
  override def slideDuration: Duration = new Duration(1000)
}

class CSharpDStreamSuite extends SparkCLRFunSuite {

  test("runParallelJob in UpdateStateByKey") {

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new MockSparkContext(conf)
    val ssc = new StreamingContext(sc, new Duration(1000))
    val parent = ssc.binaryRecordsStream("test", 0)

    try {
      var actual = sc.getConf.getInt("spark.mobius.streaming.parallelJobs", 0)
      assert(actual == 0, s", configured parallel jobs: 0; actual: $actual")

      var ds = new MockCSharpStateDStream(parent, null, null, null)
      ds.zeroTime = new Time(0)

      ds.runParallelJob(new Time(2000), Some(sc.emptyRDD[Array[Byte]]))
      actual = sc.numParallelJobs
      assert(sc.numParallelJobs == 0, s", parallelJobs disabled by default, expected parallel jobs: 0; actual: $actual")

      ssc.conf.set("spark.mobius.streaming.parallelJobs", "2")
      actual = sc.getConf.getInt("spark.mobius.streaming.parallelJobs", 0)
      assert(sc.getConf.getInt("spark.mobius.streaming.parallelJobs", 0) == 2, s", configured parallel jobs: 2; actual: $actual")

      ds = new MockCSharpStateDStream(parent, null, null, null)
      ds.zeroTime = new Time(0)

      // parallel job not applicable to first batch
      ds.runParallelJob(new Time(1000), Some(sc.emptyRDD[Array[Byte]]))
      actual = sc.numParallelJobs
      assert(sc.numParallelJobs == 0, s", first batch expected parallel jobs: 0; actual: $actual")

      // unblock current batch by running a parallel job if previous batch not completed yet
      sc.numParallelJobs = 0
      ds.runParallelJob(new Time(2000), Some(sc.emptyRDD[Array[Byte]]))
      actual = sc.numParallelJobs
      assert(sc.numParallelJobs == 1, s", previous batch not completed, expected parallel jobs: 1; actual: $actual")
      assert(ds.jobExecutor != null, s", job executor")
      actual = ds.jobExecutor.getCorePoolSize()
      // verify thread pool size, actual threading is JVM's responsibility
      assert(actual  == 2, s", job executor pool size expected: 2; actual: $actual")

      val batchCompleted = StreamingListenerBatchCompleted(BatchInfo(
        batchTime = Time(1000L),
        streamIdToInputInfo = Map(),
        submissionTime = 0L,
        null,
        null,
        outputOperationInfos = Map()
      ))
      // mark previous batch as completed
      ssc.progressListener.onBatchCompleted(batchCompleted)
      sc.numParallelJobs = 0
      ds.runParallelJob(new Time(2000), Some(sc.emptyRDD[Array[Byte]]))
      actual = sc.numParallelJobs
      assert(sc.numParallelJobs == 0, s", previous batch completed, expected parallel jobs: 0; actual: $actual")

    } finally {
      sc.stop()
    }
  }
}
