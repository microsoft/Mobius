/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.streaming.api.csharp

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.csharp.SparkCLRFunSuite
import org.apache.spark.rdd.LocalRDDCheckpointData
import org.apache.spark.streaming._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.mutable._

class CSharpDStreamSuite extends SparkCLRFunSuite with BeforeAndAfterAll with BeforeAndAfter {
  private var conf: SparkConf = null
  private var sc: SparkContext = null

  before {
    StreamingContext.getActive().foreach { _.stop(stopSparkContext = false) }
  }

  after {
    StreamingContext.getActive().foreach { _.stop(stopSparkContext = false) }
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  test("CSharpStateDStream - local checkpoint enabled") {
    val replicas = 2
    conf = new SparkConf().setMaster("local[*]").setAppName("CSharpStateDStream")
    conf.set("spark.mobius.streaming.localCheckpoint.enabled", "true")
    conf.set("spark.mobius.streaming.localCheckpoint.replicas", replicas.toString)

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
    assert(rddCount.get() >= 1)
  }
}
