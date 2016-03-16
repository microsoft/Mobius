/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.streaming.kafka

import java.util.Properties

import kafka.api._
import kafka.common.{ErrorMapping, OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import org.apache.spark.SparkException

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

/**
 * Convenience methods for interacting with a Kafka cluster.
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>.
 *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
 *   NOT zookeeper servers, specified in host1:port1,host2:port2 form
 */
private[spark]
class DynamicPartitionKafkaCluster(override val kafkaParams: Map[String, String]) extends KafkaCluster(kafkaParams) {
  import KafkaCluster.{Err, LeaderOffset, SimpleConsumerConfig}

  def findLeaders2(
                   topicAndPartitions: Set[TopicAndPartition]
                   ): (Option[Err], Map[TopicAndPartition, (String, Int)]) = {
    val topics = topicAndPartitions.map(_.topic)
    val (getPartionMetaError, topicMetaSet) = getPartitionMetadata2(topics)
    val answer = {
      val leaderMap = topicMetaSet.flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.flatMap { pm: PartitionMetadata =>
          val tp = TopicAndPartition(tm.topic, pm.partitionId)
          if (topicAndPartitions(tp)) {
            pm.leader.map { l =>
              tp -> (l.host -> l.port)
            }
          } else {
            None
          }
        }
      }.toMap

      if (leaderMap.keys.size == topicAndPartitions.size) {
        (getPartionMetaError, leaderMap)
      } else {
        val missing = topicAndPartitions.diff(leaderMap.keySet)
        val err = new Err
        err.append(new SparkException(s"Couldn't find leaders for ${missing}"))
        (Some(err), leaderMap)
      }
    }
    answer
  }

  def getPartitions2(topics: Set[String]): (Option[Err], Set[TopicAndPartition]) = {
    val (errs, topicMetaSet) = getPartitionMetadata2(topics)
    val topicAndPartitions = topicMetaSet.flatMap { tm: TopicMetadata =>
      tm.partitionsMetadata.map { pm: PartitionMetadata =>
        TopicAndPartition(tm.topic, pm.partitionId)
      }
    }
    (errs, topicAndPartitions)
  }

  def getPartitionMetadata2(topics: Set[String]): (Option[Err], Set[TopicMetadata]) = {
    val req = TopicMetadataRequest(
      TopicMetadataRequest.CurrentVersion, 0, config.clientId, topics.toSeq)
    var topicMetaSet: Set[TopicMetadata] = Set()
    val errs = new Err
    withBrokers(Random.shuffle(config.seedBrokers), errs) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      val respErrs = resp.topicsMetadata.filter(m => m.errorCode != ErrorMapping.NoError)
      if (respErrs.isEmpty) {
        return (None, resp.topicsMetadata.toSet)
      } else {
        respErrs.foreach { m =>
          val cause = ErrorMapping.exceptionFor(m.errorCode)
          val msg = s"Error getting partition metadata for '${m.topic}'. Does the topic exist?"
          errs.append(new SparkException(msg, cause))
        }

        val tms = resp.topicsMetadata.filter(m => m.errorCode == ErrorMapping.NoError).toSet
        if (tms.size > topicMetaSet.size) {
          topicMetaSet = tms
        }
      }
    }
    (Some(errs), topicMetaSet)
  }

  def getLatestLeaderOffsets2(
                               topicAndPartitions: Set[TopicAndPartition]
                               ): (Option[Err], Map[TopicAndPartition, LeaderOffset]) =
    getLeaderOffsets2(topicAndPartitions, OffsetRequest.LatestTime)

  def getEarliestLeaderOffsets2(
                                 topicAndPartitions: Set[TopicAndPartition]
                                 ): (Option[Err], Map[TopicAndPartition, LeaderOffset]) =
    getLeaderOffsets2(topicAndPartitions, OffsetRequest.EarliestTime)

  def getLeaderOffsets2(
      topicAndPartitions: Set[TopicAndPartition],
      before: Long
    ): (Option[Err], Map[TopicAndPartition, LeaderOffset]) = {
    val (err, leaderOffsets) = getLeaderOffsets2(topicAndPartitions, before, 1)
    (err,
     leaderOffsets.map { kv =>
       // mapValues isnt serializable, see SI-7005
       kv._1 -> kv._2.head
     })
  }

  def getLeaderOffsets2(
      topicAndPartitions: Set[TopicAndPartition],
      before: Long,
      maxNumOffsets: Int
      ): (Option[Err], Map[TopicAndPartition, Seq[LeaderOffset]]) = {
    val (leaderErr, tpToLeaderMap) = findLeaders2(topicAndPartitions)
    val leaderToTp: Map[(String, Int), Seq[TopicAndPartition]] = flip(tpToLeaderMap)
    val leaders = leaderToTp.keys
    var result = Map[TopicAndPartition, Seq[LeaderOffset]]()
    val errs = new Err
    withBrokers(leaders, errs) { consumer =>
      val partitionsToGetOffsets: Seq[TopicAndPartition] =
        leaderToTp((consumer.host, consumer.port))
      val reqMap = partitionsToGetOffsets.map { tp: TopicAndPartition =>
        tp -> PartitionOffsetRequestInfo(before, maxNumOffsets)
      }.toMap
      val req = OffsetRequest(reqMap)
      val resp = consumer.getOffsetsBefore(req)
      val respMap = resp.partitionErrorAndOffsets
      partitionsToGetOffsets.foreach { tp: TopicAndPartition =>
        respMap.get(tp).foreach { por: PartitionOffsetsResponse =>
          if (por.error == ErrorMapping.NoError) {
            if (por.offsets.nonEmpty) {
              result += tp -> por.offsets.map { off =>
                LeaderOffset(consumer.host, consumer.port, off)
              }
            } else {
              errs.append(new SparkException(
                s"Empty offsets for ${tp}, is ${before} before log beginning?"))
            }
          } else {
            errs.append(ErrorMapping.exceptionFor(por.error))
          }
        }
      }
      if (result.keys.size == topicAndPartitions.size) {
        return (None, result)
      }
    }
    val missing = topicAndPartitions.diff(result.keySet)
    errs.append(new SparkException(s"Couldn't find leader offsets for ${missing}"))
    (Some(errs), result)
  }

  private def flip[K, V](m: Map[K, V]): Map[V, Seq[K]] =
    m.groupBy(_._2).map { kv =>
      kv._1 -> kv._2.keys.toSeq
    }

  // Try a call against potentially multiple brokers, accumulating errors
  private def withBrokers(brokers: Iterable[(String, Int)], errs: Err)
                         (fn: SimpleConsumer => Any): Unit = {
    brokers.foreach { hp =>
      var consumer: SimpleConsumer = null
      try {
        consumer = connect(hp._1, hp._2)
        fn(consumer)
      } catch {
        case NonFatal(e) =>
          errs.append(e)
      } finally {
        if (consumer != null) {
          consumer.close()
        }
      }
    }
  }
}
