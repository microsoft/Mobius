/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package org.apache.spark.streaming.kafka

import java.io.File
import java.nio.file.Files

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.spark.csharp.SparkCLRFunSuite
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.streaming.api.csharp.CSharpDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.scheduler.InputInfoTracker
import org.apache.spark.streaming.{Duration, StreamingContext, Time}
import org.apache.spark.{Partition, SparkConf, SparkContext}

class DynamicPartitionKafkaInputDStreamSuite extends SparkCLRFunSuite {

  test("checkpoint-restore") {

    val port = 5000
    val host0 = "host0"
    val host1 = "host1"
    val topic = "testTopic"
    val tp0 = TopicAndPartition(topic, 0)
    val tp1 = TopicAndPartition(topic, 1)
    val offsetRange0 = OffsetRange(tp0, 100, 1000)
    val offsetRange1 = OffsetRange(tp1, 200, 300)
    val offsetRanges = Array(offsetRange0, offsetRange1)
    val leaders = Map(tp0 -> (host0, port), tp1 -> (host1, port))
    val kafkaParams = Map("metadata.broker.list" -> "", "auto.offset.reset" -> "largest")

    val conf = new SparkConf().setAppName("test").setMaster("local").set("spark.testing", "true")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, new Duration(1000))
    ssc.scheduler.inputInfoTracker = new InputInfoTracker(ssc)

    try {
      val messageHandler =
        (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => (mmd.key, mmd.message)
      val ds = new DynamicPartitionKafkaInputDStream
        [Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder, (Array[Byte], Array[Byte])](
          ssc, kafkaParams, Set(), Map(), sc.clean(messageHandler), 1)
      ds.zeroTime = new Time(0)

      ds.offsetsRangeForNextBatch = Some((Map(tp0 -> 100L, tp1 -> 200L), Map(tp0 -> LeaderOffset(host0, port, 1000), tp1 -> LeaderOffset(host1, port, 300))))
      // parent ds's kafka rdd will be generated first and put in generatedRDDs
      new CSharpDStream(ds, null, null).compute(new Time(1000))

      // checkpoint and clear generatedRDDs
      ds.checkpointData.update(new Time(1000))
      ds.generatedRDDs.clear()

      var count = ds.currentOffsets.count(_ => true)
      assert(count == 0, s", currentOffsets should have 0 items before restore")

      count = ds.topicAndPartitions.count(_ => true)
      assert(count == 0, s", topicAndPartitions should have 0 items before restore")

      count = ds.generatedRDDs.count(_ => true)
      assert(count == 0, s", generatedRDDs should have 0 items before restore")

      ds.checkpointData.restore()

      count = ds.currentOffsets.count(_ => true)
      assert(count == 2, s", currentOffsets should have 2 items after restore")

      val currentOffset0 = ds.currentOffsets(tp0)
      assert(currentOffset0 == 1000, s", currentOffset0 should be 1000")

      val currentOffset1 = ds.currentOffsets(tp1)
      assert(currentOffset1 == 300, s", currentOffset1 should be 300")

      count = ds.topicAndPartitions.count(_ => true)
      assert(count == 2, s", topicAndPartitions should have 2 items after restore")

      count = ds.generatedRDDs.count(_ => true)
      assert(count == 1, s", generatedRDDs should have 1 item after restore")

      val rdd = ds.generatedRDDs(new Time(1000))
      val name = rdd.getClass.getSimpleName
      assert(name == "DynamicPartitionKafkaRDD", s", generatedRDDs type should be DynamicPartitionKafkaRDD")

    } finally {
      sc.stop()
    }
  }

  test("Kafka offset checkpoint and replay") {

    val port = 5000
    val host0 = "host0"
    val host1 = "host1"
    val topic = "testTopic"
    val dc = "testDC"
    val tp0 = TopicAndPartition(topic, 0)
    val tp1 = TopicAndPartition(topic, 1)
    val offsetRange = Some((Map(tp0 -> 100L, tp1 -> 200L), Map(tp0 -> LeaderOffset(host0, port, 1000), tp1 -> LeaderOffset(host1, port, 300))))
    val leaders = Map(tp0 -> (host0, port), tp1 -> (host1, port))
    val kafkaParams = Map("metadata.broker.list" -> "broker1",
      "metadata.leader.list" -> leaders.map { case (tp, hostAndPort) =>
          s"${tp.topic},${tp.partition},${hostAndPort._1},${hostAndPort._2}"
      }.mkString("|"),
      "auto.offset.reset" -> "largest",
      "cluster.id" -> dc
    )

    val conf = new SparkConf()
      .setAppName("test")
      .setMaster("local")
      .set("spark.testing", "true")
      .set("spark.mobius.streaming.kafka.enableOffsetCheckpoint", "true")
      .set("spark.mobius.streaming.kafka.replayBatches", "1")
      .set("spark.mobius.streaming.kafka.maxRetainedOffsetCheckpoints", "1")
    val sc = new SparkContext(conf)

    val checkpointDirPath = new Path(FileUtils.getTempDirectoryPath, "test-checkpoint")
    val fs = checkpointDirPath.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(checkpointDirPath)) fs.delete(checkpointDirPath, true)

    sc.setCheckpointDir(checkpointDirPath.toString)
    val ssc = new StreamingContext(sc, new Duration(1000))
    ssc.scheduler.inputInfoTracker = new InputInfoTracker(ssc)

    var ds: DynamicPartitionKafkaInputDStream[_, _, _, _, _] = null
    try {
      val messageHandler =
        (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => (mmd.key, mmd.message)
      ds = new DynamicPartitionKafkaInputDStream
        [Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder, (Array[Byte], Array[Byte])](
        ssc, kafkaParams, Set(topic), Map(), sc.clean(messageHandler), 1)

      ds.zeroTime = new Time(0)
      ds.offsetsRangeForNextBatch = offsetRange

      ds.compute(new Time(1000)) // fresh start
      ds.clearMetadata(new Time(1000)) // this will
      assert(!ds.needToReplayOffsetRanges)

      ds.start()
      assert(ds.offsetsRangeForNextBatch.isDefined)
      assert(ds.needToReplayOffsetRanges)
      assertResult(offsetRange)(ds.offsetsRangeForNextBatch)

    } finally {
      if(ds != null) ds.stop
      sc.stop()
      fs.delete(checkpointDirPath, true)
      fs.close()
    }
  }
}
