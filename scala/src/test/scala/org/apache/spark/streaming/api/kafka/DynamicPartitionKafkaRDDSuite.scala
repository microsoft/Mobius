/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package org.apache.spark.streaming.kafka

import java.nio.ByteBuffer

import kafka.common.TopicAndPartition
import org.apache.spark.{Partition, SparkContext, SparkConf}
import org.apache.spark.csharp.SparkCLRFunSuite

class DynamicPartitionKafkaRDDSuite extends SparkCLRFunSuite {

  private def testEqualityOfKafkaPartitions(
      a: Array[KafkaRDDPartition], b: Array[Partition]): Boolean = {
    if (a.length != b.length) {
      false
    } else {
      a.indices.forall(
        pos => {
          val p1 = a(pos)
          val p2 = b(pos).asInstanceOf[KafkaRDDPartition]
          p1.index == p2.index &&
          p1.topic == p2.topic &&
          p1.partition == p2.partition &&
          p1.fromOffset == p2.fromOffset &&
          p1.host == p2.host &&
          p1.port == p2.port
        }
      )
    }
  }

  test("getPartitions") {

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

    val conf = new SparkConf().setAppName("test").setMaster("local").set("spark.testing", "true")
    val sc = new SparkContext(conf)

    try {
      // test numPartitions = 0
      var rdd = new DynamicPartitionKafkaRDD(sc, Map(), offsetRanges, leaders, null, 0)
      var partitions = Array(
        new KafkaRDDPartition(0, topic, 0, 100, 600, host0, port),
        new KafkaRDDPartition(1, topic, 0, 600, 1000, host0, port),
        new KafkaRDDPartition(2, topic, 1, 200, 300, host1, port)
      )
      assert(testEqualityOfKafkaPartitions(partitions, rdd.getPartitions))

      // test numPartitions > 0
      rdd = new DynamicPartitionKafkaRDD(sc, Map(), offsetRanges, leaders, null, 4)
      partitions = Array(
        new KafkaRDDPartition(0, topic, 0, 100, 350, host0, port),
        new KafkaRDDPartition(1, topic, 0, 350, 600, host0, port),
        new KafkaRDDPartition(2, topic, 0, 600, 850, host0, port),
        new KafkaRDDPartition(3, topic, 0, 850, 1000, host0, port),
        new KafkaRDDPartition(4, topic, 1, 200, 300, host1, port)
      )
      assert(testEqualityOfKafkaPartitions(partitions, rdd.getPartitions))

      // test numPartitions < 0, without spark.streaming.kafka.maxRatePerTask.* conf
      rdd = new DynamicPartitionKafkaRDD(sc, Map(), offsetRanges, leaders, null, -1)
      partitions = Array(
        new KafkaRDDPartition(0, topic, 0, 100, 1000, host0, port),
        new KafkaRDDPartition(1, topic, 1, 200, 300, host1, port)
      )
      assert(testEqualityOfKafkaPartitions(partitions, rdd.getPartitions))

      // test numPartitions < 0, with spark.mobius.streaming.kafka.maxMessagesPerTask.* conf
      sc.conf.set("spark.mobius.streaming.kafka.maxMessagesPerTask." + topic, "500")
      rdd = new DynamicPartitionKafkaRDD(sc, Map(), offsetRanges, leaders, null, -1)
      partitions = Array(
        new KafkaRDDPartition(0, topic, 0, 100, 600, host0, port),
        new KafkaRDDPartition(1, topic, 0, 600, 1000, host0, port),
        new KafkaRDDPartition(2, topic, 1, 200, 300, host1, port)
      )
      assert(testEqualityOfKafkaPartitions(partitions, rdd.getPartitions))
    } finally {
      sc.stop()
    }
  }

  test("compute") {

    val conf = new SparkConf().setAppName("test").setMaster("local").set("spark.testing", "true").set("spark.mobius.streaming.kafka.CSharpReader.enabled", "true")
    val sc = new SparkContext(conf)

    try {
      assert(sc.getConf.getBoolean("spark.mobius.streaming.kafka.CSharpReader.enabled", false), s", CSharpReader.enabled expects true")

      val rdd = new DynamicPartitionKafkaRDD(sc, Map("cluster.id" -> "testId"), Array(), Map(), null, 0)
      val thePart = new KafkaRDDPartition(0, "testTopic", 1, 100, 600, null, 0)
      val metadata = rdd.compute(thePart, null).toArray

      val topicAndPartition = metadata(0).asInstanceOf[(Array[Byte], Array[Byte])]
      val topicAndClusterId = new String(topicAndPartition._1, "UTF-8")
      assert(topicAndClusterId == "testTopic,testId", s"expected topic and cluster id: testTopic,testId actual: $topicAndClusterId")
      val partition = ByteBuffer.wrap(topicAndPartition._2).getInt
      assert(partition == 1, s"expected partition: 1 actual: $partition")

      val offsetRange = metadata(1).asInstanceOf[(Array[Byte], Array[Byte])]
      val from = ByteBuffer.wrap(offsetRange._1).getLong
      assert(from == 100, s"expected from offset: 100 actual: $from")
      val until = ByteBuffer.wrap(offsetRange._2).getLong
      assert(until == 600, s"expected until offset: 100 actual: $until")
    } finally {
      sc.stop()
    }
  }
}
