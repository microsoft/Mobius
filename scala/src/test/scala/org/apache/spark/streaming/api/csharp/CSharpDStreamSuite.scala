/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package org.apache.spark.streaming.api.csharp

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.csharp.SparkCLRFunSuite
import org.apache.spark.rdd.{LocalRDDCheckpointData, RDD}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{BatchInfo, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.mutable.{ArrayBuffer, Queue}
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

class CSharpDStreamSuite extends SparkCLRFunSuite with BeforeAndAfterAll with BeforeAndAfter {
  private var conf: SparkConf = null
  private var sc: SparkContext = null

  before {
    StreamingContext.getActive().foreach { _.stop(stopSparkContext = false) }
    if (sc != null && !sc.stopped.get) {
      sc.stop()
    }
  }

  after {
    StreamingContext.getActive().foreach { _.stop(stopSparkContext = false) }
    if (sc != null && !sc.stopped.get) {
      sc.stop()
    }
  }

  test("CSharpStateDStream - local checkpoint enabled") {
    val replicas = 2
    conf = new SparkConf().setMaster("local[*]").setAppName("CSharpStateDStream")
    conf.set("spark.mobius.streaming.localCheckpoint.enabled", "true")
    conf.set("spark.mobius.streaming.localCheckpoint.replicas", replicas.toString)

    // set reserved memory to 50M so the min system memory only needs 50M * 1.5 = 70.5M
    conf.set("spark.testing.reservedMemory", "" + 50 * 1024 * 1024)

    sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    // Set up CSharpDStream
    val expectedRDD = sc.makeRDD(ArrayBuffer.fill(10)("a".getBytes))
    CSharpDStream.debugMode = true
    CSharpDStream.debugRDD = Some(expectedRDD)

    // Construct CSharpStateDStream
    val parentDStream = ssc.queueStream[Array[Byte]](Queue(sc.makeRDD(ArrayBuffer.fill(10)("a".getBytes))), true)
    val stateDStream = new CSharpStateDStream(parentDStream, new Array[Byte](0), "byte", "byte")
    stateDStream.register()

    val rddCount = new AtomicInteger(0) // counter to indicate how many batches have finished

    stateDStream.foreachRDD { stateRDD =>
      rddCount.incrementAndGet()
      println(stateRDD.count())
      // asserts
      assert(stateRDD.checkpointData.get.isInstanceOf[LocalRDDCheckpointData[Array[Byte]]])
      assert(stateRDD.getStorageLevel.replication == replicas)
    }
    ssc.start()

    // check whether first batch finished
    val startWaitingTimeInMills = System.currentTimeMillis()
    val maxWaitingTimeInSecs = 20
    while(rddCount.get() < 1) {
      Thread.sleep(100)
      if((System.currentTimeMillis() - startWaitingTimeInMills) > maxWaitingTimeInSecs * 1000) {
        // if waiting time exceeds `maxWaitingTimeInSecs` secs, will fail current test.
        fail(s"Total running time exceeds $maxWaitingTimeInSecs, fail current test.")
      }
    }

    ssc.stop(false)
    sc.stop
    assert(rddCount.get() >= 1)
  }

  test("runParallelJob in UpdateStateByKey") {

    val conf = new SparkConf().setAppName("test").setMaster("local").set("spark.testing", "true")
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
