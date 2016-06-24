/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package org.apache.spark.streaming.api.csharp

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.csharp.SparkCLRFunSuite

class RddPreComputeProcessorSuite extends SparkCLRFunSuite {

  test("RddPreComputeProcessor") {
  val conf = new SparkConf().setAppName("test").setMaster("local").set("spark.testing", "true")
  val sc = new SparkContext(conf)
  val preComputeProcessor = new RddPreComputeProcessor[Long](
      sc, "RddPreComputeProcessor-test", 1, 1, 1, StorageLevel.MEMORY_ONLY)

    try {
      val rdd1 = sc.range(1L, 10L, 1L)
      preComputeProcessor.put(rdd1)
      var stop = false
      while (!stop) {
        var preComputedResult1 = preComputeProcessor.get()
        if (preComputedResult1.isEmpty) {
          Thread.sleep(100)
        } else {
          stop = true
          assert(preComputedResult1.size == 1)
        }
      }

      // test bypass scenario because ackRdd() is not called
      val rdd2 = sc.range(1L, 5L, 1L)
      preComputeProcessor.put(rdd2)
      var preComputedResult2 = preComputeProcessor.get()
      assert(preComputedResult2.size == 1)
    } finally {
      preComputeProcessor.stop()
      sc.stop()
    }
  }
}
