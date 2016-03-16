/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.streaming.kafka

import java.io.OutputStream
import java.lang.{Integer => JInt, Long => JLong}
import java.util.{List => JList, Map => JMap, Set => JSet}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.google.common.base.Charsets.UTF_8
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, DefaultDecoder, StringDecoder}
import net.razorvine.pickle.{IObjectPickler, Opcodes, Pickler}

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.util.WriteAheadLogUtils

/**
 * This is a helper class that wraps DynamicPartitionKafkaInputDStream
 */
private[kafka] class KafkaUtilsCSharpHelper extends KafkaUtilsPythonHelper{

  def createDirectStreamWithoutMessageHandler(
      jssc: JavaStreamingContext,
      kafkaParams: JMap[String, String],
      topics: JSet[String],
      fromOffsets: JMap[TopicAndPartition, JLong],
      numPartitions: Int): JavaDStream[(Array[Byte], Array[Byte])] = {
    val messageHandler =
      (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => (mmd.key, mmd.message)
    new JavaDStream(createDirectStream(jssc, kafkaParams, topics, fromOffsets, messageHandler, numPartitions))
  }

  def createDirectStream[V: ClassTag](
      jssc: JavaStreamingContext,
      kafkaParams: JMap[String, String],
      topics: JSet[String],
      fromOffsets: JMap[TopicAndPartition, JLong],
      messageHandler: MessageAndMetadata[Array[Byte], Array[Byte]] => V,
      numPartitions: Int): DStream[V] = {

    val currentFromOffsets: Map[TopicAndPartition, Long] = if (!fromOffsets.isEmpty) {
      val topicsFromOffsets = fromOffsets.keySet().asScala.map(_.topic)
      if (topicsFromOffsets != topics.asScala.toSet) {
        throw new IllegalStateException(
          s"The specified topics: ${topics.asScala.toSet.mkString(" ")} " +
            s"do not equal to the topic from offsets: ${topicsFromOffsets.mkString(" ")}")
      }
      Map(fromOffsets.asScala.mapValues { _.longValue() }.toSeq: _*)
    } else {
      /*
      val kc = new KafkaCluster(Map(kafkaParams.asScala.toSeq: _*))
      KafkaUtils.getFromOffsets(
        kc, Map(kafkaParams.asScala.toSeq: _*), Set(topics.asScala.toSeq: _*))
      */
      Map()
    }

    new DynamicPartitionKafkaInputDStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder, V](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Set(topics.asScala.toSeq: _*),
      Map(currentFromOffsets.toSeq: _*),
      jssc.ssc.sc.clean(messageHandler),
      numPartitions)
  }
}
