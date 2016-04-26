/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.streaming.kafka

import java.nio.ByteBuffer

import kafka.api.{FetchRequestBuilder, FetchResponse, FetchRequest}
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.message.{MessageAndOffset, MessageAndMetadata}
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.spark.util.NextIterator
import org.apache.spark._

import scala.reflect._

/**
 * A batch-oriented interface for consuming from Kafka with
 * dynamic partitioning and kafka read redirecting
 *  @getPartitions split larger partitions based on average partition size calculated by numPartitions
 *  @compute option to pass metadata instead of actual data, user process responsible for reading kafka
 * Starting and ending offsets are specified in advance,
 * so that you can control exactly-once semantics.
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers" to be set
 * with Kafka broker(s) specified in host1:port1,host2:port2 form.
 * @param offsetRanges offset ranges that define the Kafka data belonging to this RDD
 * @param messageHandler function for translating each message into the desired type
 * @param numPartitions user hint on how many RDD partitions to create instead of aligning with Kafka partitions
 */
private[kafka]
class DynamicPartitionKafkaRDD[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag,
  R: ClassTag] private[spark] (
    @transient sc: SparkContext,
    kafkaParams: Map[String, String],
    override val offsetRanges: Array[OffsetRange],
    leaders: Map[TopicAndPartition, (String, Int)],
    messageHandler: MessageAndMetadata[K, V] => R,
    val numPartitions: Int
  ) extends KafkaRDD[K, V, U, T, R](sc, kafkaParams, offsetRanges, leaders, messageHandler) {
  override def getPartitions: Array[Partition] = {
    val steps = offsetRanges.groupBy(o => o.topic).map { case (topic, offRanges) =>
      if (numPartitions < 0) {
        // If numPartitions = -1, either repartition based on spark.streaming.kafka.maxRatePerTask.*
        // or do nothing if config not defined
        (topic, sparkContext.getConf.getInt("spark.mobius.streaming.kafka.maxMessagesPerTask." + topic,
          Int.MaxValue).asInstanceOf[Long])
      } else {
        val partitions = {
          // If numPartitions = 0, repartition using original kafka partition count
          // If numPartitions > 0, repartition using this parameter
          if (numPartitions == 0) {
            Math.max(offRanges.length, 1)  // use Math.max(..,1) to ensure numPartitions >= 1
          } else numPartitions
        }
        // step can't be zero, use Math.max(..., 1) to ensure that step >= 1
        (topic, Math.max(offRanges.map(o => o.untilOffset - o.fromOffset).sum / partitions, 1))
      }
    }
    offsetRanges.flatMap { case o =>
      (o.fromOffset until o.untilOffset by steps(o.topic)).map(s =>
        (o.topic, o.partition, s, math.min(o.untilOffset, s + steps(o.topic)))
      )
    }.zipWithIndex.map { case ((topic, partition, start, until), i) =>
      val (host, port) = leaders(TopicAndPartition(topic, partition))
      new KafkaRDDPartition(i, topic, partition, start, until, host, port)
    }.toArray
  }

  private def errBeginAfterEnd(part: KafkaRDDPartition): String =
    s"Beginning offset ${part.fromOffset} is after the ending offset ${part.untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition}. " +
      "You either provided an invalid fromOffset, or the Kafka topic has been damaged"

  private def errRanOutBeforeEnd(part: KafkaRDDPartition): String =
    s"Ran out of messages before reaching ending offset ${part.untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
      " This should not happen, and indicates that messages may have been lost"

  private def errOvershotEnd(itemOffset: Long, part: KafkaRDDPartition): String =
    s"Got ${itemOffset} > ending offset ${part.untilOffset} " +
      s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
      " This should not happen, and indicates a message may have been skipped"

  private val CSharpReaderEnabled: Boolean = sc.getConf.getBoolean("spark.mobius.streaming.kafka.CSharpReader.enabled", false)

  override def compute(thePart: Partition, context: TaskContext): Iterator[R] = {
    val part = thePart.asInstanceOf[KafkaRDDPartition]
    assert(part.fromOffset <= part.untilOffset, errBeginAfterEnd(part))
    if (part.fromOffset == part.untilOffset) {
      log.info(s"Beginning offset ${part.fromOffset} is the same as ending offset " +
        s"skipping ${part.topic} ${part.partition}")
      Iterator.empty
    } else if (CSharpReaderEnabled) {
        Iterator((part.topic.getBytes(), ByteBuffer.allocate(4).putInt(part.partition).array()).asInstanceOf[R], (ByteBuffer.allocate(8).putLong(part.fromOffset).array(), ByteBuffer.allocate(8).putLong(part.untilOffset).array()).asInstanceOf[R])
    } else {
      new DynamicPartitionKafkaRDDIterator(part, context)
    }
  }

  private class DynamicPartitionKafkaRDDIterator(
                                  part: KafkaRDDPartition,
                                  context: TaskContext) extends NextIterator[R] {

    context.addTaskCompletionListener{ context => closeIfNeeded() }

    log.info(s"Task index ${part.index} " +
      s"Computing topic ${part.topic}, partition ${part.partition} " +
      s"offsets ${part.fromOffset} -> ${part.untilOffset} count ${part.untilOffset - part.fromOffset} " +
      s"from ${part.host}:${part.port}")

    val kc = new KafkaCluster(kafkaParams)
    val keyDecoder = classTag[U].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(kc.config.props)
      .asInstanceOf[Decoder[K]]
    val valueDecoder = classTag[T].runtimeClass.getConstructor(classOf[VerifiableProperties])
      .newInstance(kc.config.props)
      .asInstanceOf[Decoder[V]]
    val consumer = connectLeader
    var requestOffset = part.fromOffset
    var iter: Iterator[MessageAndOffset] = null

    // The idea is to use the provided preferred host, except on task retry attempts,
    // to minimize number of kafka metadata requests
    private def connectLeader: SimpleConsumer = {
      if (context.attemptNumber > 0) {
        kc.connectLeader(part.topic, part.partition).fold(
          errs => throw new SparkException(
            s"Couldn't connect to leader for topic ${part.topic} ${part.partition}: " +
              errs.mkString("\n")),
          consumer => consumer
        )
      } else {
        kc.connect(part.host, part.port)
      }
    }

    private def handleFetchErr(resp: FetchResponse) {
      if (resp.hasError) {
        val err = resp.errorCode(part.topic, part.partition)
        if (err == ErrorMapping.LeaderNotAvailableCode ||
          err == ErrorMapping.NotLeaderForPartitionCode) {
          log.error(s"Lost leader for topic ${part.topic} partition ${part.partition}, " +
            s" sleeping for ${kc.config.refreshLeaderBackoffMs}ms")
          Thread.sleep(kc.config.refreshLeaderBackoffMs)
        }
        // Let normal rdd retry sort out reconnect attempts
        throw ErrorMapping.exceptionFor(err)
      }
    }

    private def fetchBatch: Iterator[MessageAndOffset] = {
      val req = new FetchRequestBuilder()
        .addFetch(part.topic, part.partition, requestOffset, kc.config.fetchMessageMaxBytes)
        .build()
      val resp = consumer.fetch(req)
      handleFetchErr(resp)
      // kafka may return a batch that starts before the requested offset
      resp.messageSet(part.topic, part.partition)
        .iterator
        .dropWhile(_.offset < requestOffset)
    }

    override def close(): Unit = {
      if (consumer != null) {
        consumer.close()
      }
    }

    override def getNext(): R = {
      if (iter == null || !iter.hasNext) {
        iter = fetchBatch
      }
      if (!iter.hasNext) {
        //assert(requestOffset == part.untilOffset, errRanOutBeforeEnd(part))
        if (requestOffset < part.untilOffset)
          log.error(errRanOutBeforeEnd(part))
        finished = true
        null.asInstanceOf[R]
      } else {
        val item = iter.next()
        if (item.offset >= part.untilOffset) {
          //assert(item.offset == part.untilOffset, errOvershotEnd(item.offset, part))
          if (item.offset > part.untilOffset)
            log.error(errOvershotEnd(item.offset, part))
          finished = true
          null.asInstanceOf[R]
        } else {
          requestOffset = item.nextOffset
          messageHandler(new MessageAndMetadata(
            part.topic, part.partition, item.message, item.offset, keyDecoder, valueDecoder))
        }
      }
    }
  }
}

private[kafka]
object DynamicPartitionKafkaRDD {
  import KafkaCluster.LeaderOffset

  /**
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   * configuration parameters</a>.
   *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
   *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
   * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
   *  starting point of the batch
   * @param untilOffsets per-topic/partition Kafka offsets defining the (exclusive)
   *  ending point of the batch
   * @param messageHandler function for translating each message into the desired type
   */
  def apply[
    K: ClassTag,
    V: ClassTag,
    U <: Decoder[_]: ClassTag,
    T <: Decoder[_]: ClassTag,
    R: ClassTag](
      sc: SparkContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicAndPartition, Long],
      untilOffsets: Map[TopicAndPartition, LeaderOffset],
      messageHandler: MessageAndMetadata[K, V] => R,
      numPartitions: Int
    ): DynamicPartitionKafkaRDD[K, V, U, T, R] = {
    val leaders = untilOffsets.map { case (tp, lo) =>
        tp -> (lo.host, lo.port)
    }.toMap

    val offsetRanges = fromOffsets.map { case (tp, fo) =>
        val uo = untilOffsets(tp)
        OffsetRange(tp.topic, tp.partition, fo, uo.offset)
    }.toArray

    new DynamicPartitionKafkaRDD[K, V, U, T, R](sc, kafkaParams, offsetRanges, leaders, messageHandler, numPartitions)
  }
}
