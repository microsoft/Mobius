/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.streaming.kafka

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.api.csharp.CSharpDStream
import org.apache.spark.streaming.scheduler.rate.RateEstimator

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag

import java.util.concurrent.{TimeUnit, ScheduledExecutorService}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder

import org.apache.spark.Logging
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.scheduler.{RateController, StreamInputInfo}
import org.apache.spark.util.ThreadUtils

/**
 * A stream of the new DynamicPartitionKafkaRDD to support where
 * each given Kafka topic/partition can correspond to multiple RDD partitions
 * calculated by numPartitions instead of a single RDD partition.
 * also, new methods added to achieve better fault tolerance when reading kafka metadata
 * Starting offsets are specified in advance,
 * and this DStream is not responsible for committing offsets,
 * so that you can control exactly-once semantics.
 * For an easy interface to Kafka-managed offsets,
 *  see {@link org.apache.spark.streaming.kafka.KafkaCluster}
 * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
 * configuration parameters</a>.
 *   Requires "metadata.broker.list" or "bootstrap.servers" to be set with Kafka broker(s),
 *   NOT zookeeper servers, specified in host1:port1,host2:port2 form.
 * @param fromOffsets per-topic/partition Kafka offsets defining the (inclusive)
 *  starting point of the stream
 * @param messageHandler function for translating each message into the desired type
 * @param numPartitions user hint on how many RDD partitions to create instead of aligning with Kafka partitions
 */
private[streaming]
class DynamicPartitionKafkaInputDStream[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[K]: ClassTag,
  T <: Decoder[V]: ClassTag,
  R: ClassTag](
    ssc_ : StreamingContext,
    val kafkaParams: Map[String, String],
    topics: Set[String],
    val fromOffsets: Map[TopicAndPartition, Long],
    messageHandler: MessageAndMetadata[K, V] => R,
    numPartitions: Int,
    cSharpFunc: Array[Byte] = null,
    serializationMode: String = null
  ) extends InputDStream[R](ssc_) with Logging {
  val maxRetries = context.sparkContext.getConf.getInt(
    "spark.streaming.kafka.maxRetries", 1)

  // Keep this consistent with how other streams are named (e.g. "Flume polling stream [2]")
  private[streaming] override def name: String = s"Dynamic partition Kafka direct stream [$id]"

  protected[streaming] override val checkpointData =
    new DynamicPartitionKafkaInputDStreamCheckpointData

  protected val kc = new DynamicPartitionKafkaCluster(kafkaParams)

  @transient protected[streaming] var currentOffsets = fromOffsets

  private val autoOffsetReset: Option[String] =
    kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
  private var hasFetchedAllPartitions: Boolean = false
  private var hasFetchedAllFromOffsets: Boolean = false
  @transient private[streaming] var topicAndPartitions: Set[TopicAndPartition] = Set()

  @transient private var refreshOffsetsScheduler: ScheduledExecutorService = null

  // reading metadata of mutilple topics from across multiple data centers takes long time to complete,
  // which impacts DStream performance and causes UI steaming tab not responsive due to mutex held by DStream
  // so a separate thread is introduced to refresh metadata (current offsets) asynchronously at below interval
  // this unblocks DStream in above described situation but not quite in sync with batch timestamp,
  // which is OK since batches are still generated at the same interval
  // the interval is set to half of the batch interval to make sure they're not in sync to block each other
  // TODO: configurable as performance tuning option
  private val refreshOffsetsInterval = Math.max(slideDuration.milliseconds / 2, 50)

  // fromOffsets and untilOffsets for next batch
  @transient @volatile var offsetsRangeForNextBatch:
  Option[(Map[TopicAndPartition, Long], Map[TopicAndPartition, LeaderOffset])] = None


  private def refreshPartitions(): Unit = {
    if (!hasFetchedAllPartitions) {
      kc.getPartitions2(topics) match {
        case (None, tps) =>
          hasFetchedAllPartitions = true
          topicAndPartitions = tps
          logInfo(s"successfully get all partitions: $tps")
          currentOffsets = currentOffsets.filterKeys(tp => topicAndPartitions.contains(tp)).map(identity)

        case (Some(errs), tps) =>
          topicAndPartitions = topicAndPartitions ++ tps
          logInfo(s"get partial partitions: $tps, error info: $errs")
      }
    }
  }

  /*
    some partitions might fail to get fromOffsets at last batch
    which will be bypassed instead of blocking and be retried at this batch
   */
  private def refreshFromOffsets(): Unit = {
    if (!hasFetchedAllFromOffsets) {
      var missing = topicAndPartitions.diff(currentOffsets.keySet)
      if (missing.nonEmpty) {
        currentOffsets = currentOffsets ++ getFromOffsets(missing)
      }

      if (hasFetchedAllPartitions) {
        missing = topicAndPartitions.diff(currentOffsets.keySet)
        if (missing.isEmpty) {
          logInfo("successfully refreshFromOffsets for all partitions")
          hasFetchedAllFromOffsets = true
        }
      }
    }
  }

  @tailrec
  private final def getEarliestLeaderOffsets(
      tps: Set[TopicAndPartition],
      retries: Int
    ): Map[TopicAndPartition, LeaderOffset] = {
    var offsets: Map[TopicAndPartition, LeaderOffset] = Map()
    if (retries <= 0) {
      return offsets
    }

    kc.getEarliestLeaderOffsets2(tps) match {
      case (None, ofs) =>
        ofs
      case (Some(err), ofs) =>
        log.warn(s"getEarliestLeaderOffsets err: $err")
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
        offsets = offsets ++ ofs
        getEarliestLeaderOffsets(tps, retries - 1)
    }
  }

  @tailrec
  private final def getLatestLeaderOffsets(
      tps: Set[TopicAndPartition],
      retries: Int
    ): Map[TopicAndPartition, LeaderOffset] = {
    var offsets: Map[TopicAndPartition, LeaderOffset] = Map()
    if (retries <= 0) {
      return offsets
    }

    kc.getLatestLeaderOffsets2(tps) match {
      case (None, ofs) =>
        ofs
      case (Some(err), ofs) =>
        log.warn(s"getLatestLeaderOffsets err: $err")
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
        offsets = offsets ++ ofs
        getLatestLeaderOffsets(tps, retries - 1)
    }
  }

  private def getFromOffsets(tps: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    val leaderOffsets = {
      autoOffsetReset match {
        case Some("smallest") =>
          getEarliestLeaderOffsets(tps, maxRetries)
        case _ =>
          getLatestLeaderOffsets(tps, maxRetries)
      }
    }

    leaderOffsets.map { case (tp, lo) =>
      (tp, lo.offset)
    }
  }

  /*
    new leader might fail to update with latest replica causing its offset lagging behind
    not much can be done here for the lost offsets, just reset fromOffset to its most recent value
   */
  private def resetFromOffsets(
      leaderOffsets: Map[TopicAndPartition, LeaderOffset]
    ): Map[TopicAndPartition, Long] = {
    val fromOffsets: collection.mutable.Map[TopicAndPartition, Long] = collection.mutable.Map()
    for ((tp, lo) <- leaderOffsets) {
      if (currentOffsets.contains(tp)) {
        val currentOffset = currentOffsets.get(tp).get
        if (currentOffset > lo.offset) {
          logWarning(s"currentOffset $currentOffset is larger than untilOffset ${lo.offset} " +
            s"for partition $tp, will reset it")
          currentOffsets = currentOffsets + (tp -> lo.offset)
          fromOffsets += (tp -> lo.offset)
        } else {
          fromOffsets += (tp -> currentOffset)
        }
      }
    }
    Map(fromOffsets.toList:_*)
  }

  private def setOffsetsRangeForNextBatch(): Unit = {
    if (!hasFetchedAllPartitions) refreshPartitions()
    if (!hasFetchedAllFromOffsets) refreshFromOffsets()
    // do nothing if there exists offset range waiting to be processed
    synchronized {
      if (offsetsRangeForNextBatch.isDefined) {
        return
      }
    }
    val leaderOffsets = getLatestLeaderOffsets(topicAndPartitions, maxRetries)
    val leaderOffsetCount = leaderOffsets.count(_ => true)
    logInfo(s"leaderOffsets count for stream $id: $leaderOffsetCount")
    val fromOffsets = resetFromOffsets(leaderOffsets)
    val untilOffsets = leaderOffsets
    synchronized {
      offsetsRangeForNextBatch = Some((fromOffsets, untilOffsets))
    }
    currentOffsets = currentOffsets ++ untilOffsets.map(kv => kv._1 -> kv._2.offset)
  }

  def instantiateAndStartRefreshOffsetsScheduler(): Unit = {
    // start a new thread to refresh offsets range asynchronously
    if (refreshOffsetsScheduler == null) {
      refreshOffsetsScheduler =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"refresh-offsets-$id")
      logInfo(s"Instantiated refreshOffsetsScheduler successfully for stream $id.")
    }
    refreshOffsetsScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        setOffsetsRangeForNextBatch()
      }
    }, 0, refreshOffsetsInterval, TimeUnit.MILLISECONDS)
  }

  if (fromOffsets.nonEmpty) {
    // use partitions and offsets passed from constructor
    hasFetchedAllPartitions = true
    hasFetchedAllFromOffsets = true
    topicAndPartitions = fromOffsets.keySet
    currentOffsets = fromOffsets
  } else {
    if (autoOffsetReset == None) {
      throw new IllegalArgumentException("Must set auto.offset.reset if fromOffsets is empty")
    }
  }

  private def callCSharpTransform(rdd: DynamicPartitionKafkaRDD[K, V, U, T, R], validTime: Time): Option[RDD[R]] = {
    if (cSharpFunc == null)
      Some(rdd)
    else
      CSharpDStream.callCSharpTransform(List(Some(rdd)), validTime, cSharpFunc, List(serializationMode)).asInstanceOf[Option[RDD[R]]]
  }

  override def compute(validTime: Time): Option[RDD[R]] = {
    var offsetsRange:
    Option[(Map[TopicAndPartition, Long], Map[TopicAndPartition, LeaderOffset])] = None
    synchronized {
      offsetsRange = offsetsRangeForNextBatch
      offsetsRangeForNextBatch = None
    }
    val (fromOffsets, untilOffsets) = {
      offsetsRange match {
        case None => // return empty offsets
          (Map[TopicAndPartition, Long](), Map[TopicAndPartition, LeaderOffset]())
        case Some((from, until)) =>
          (from, until)
      }
    }

    val rdd = DynamicPartitionKafkaRDD[K, V, U, T, R](
      context.sparkContext, kafkaParams, fromOffsets, untilOffsets, messageHandler, numPartitions)

    val csharpRdd = callCSharpTransform(rdd, validTime)

    // Report the record number and metadata of this batch interval to InputInfoTracker.
    val offsetRanges = fromOffsets.map { case (tp, fo) =>
      val uo = untilOffsets(tp)
      OffsetRange(tp.topic, tp.partition, fo, uo.offset)
    }
    val description = offsetRanges.filter { offsetRange =>
      // Don't display empty ranges.
      offsetRange.fromOffset != offsetRange.untilOffset
    }.map { offsetRange =>
      s"topic: ${offsetRange.topic}\tpartition: ${offsetRange.partition}\t" +
        s"offsets: ${offsetRange.fromOffset} to ${offsetRange.untilOffset}"
    }.mkString("\n")
    // Copy offsetRanges to immutable.List to prevent from being modified by the user
    val metadata = Map(
      "offsets" -> offsetRanges.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    val inputInfo = StreamInputInfo(id, rdd.count, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    csharpRdd
  }

  override def start(): Unit = {
    instantiateAndStartRefreshOffsetsScheduler
  }

  override def stop(): Unit = {
    if (refreshOffsetsScheduler != null) {
      refreshOffsetsScheduler.shutdown()
    }
  }

  private[streaming]
  class DynamicPartitionKafkaInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    def batchForTime: mutable.HashMap[Time, Array[(String, Int, Long, Long)]] = {
      data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
    }

    override def update(time: Time) {
      batchForTime.clear()
      generatedRDDs.foreach { kv =>
        val a = (if (cSharpFunc == null) kv._2 else kv._2.firstParent[R]).asInstanceOf[DynamicPartitionKafkaRDD[K, V, U, T, R]].offsetRanges.map(_.toTuple).toArray
        batchForTime += kv._1 -> a
      }
    }

    override def cleanup(time: Time) { }

    override def restore() {
      currentOffsets = batchForTime(batchForTime.keys.max).map(o => (TopicAndPartition(o._1, o._2), o._4)).toMap
      // this is assuming that the topics don't change during execution, which is true currently
      topicAndPartitions = currentOffsets.keySet
      offsetsRangeForNextBatch = None
      instantiateAndStartRefreshOffsetsScheduler()
      // for unit test purpose only, it will not get here in prod if broker list is empty
      val leaders = if (kafkaParams("metadata.broker.list").isEmpty)
        Map[TopicAndPartition, (String, Int)]()
      else
        KafkaCluster.checkErrors(kc.findLeaders(topicAndPartitions))
      val leaderCount = leaders.count(_ => true)
      logInfo(s"Restoring DynamicPartitionKafkaRDD for leaders: $leaderCount $leaders")

      batchForTime.toSeq.sortBy(_._1)(Time.ordering).foreach { case (t, b) =>
        logInfo(s"Restoring DynamicPartitionKafkaRDD for id $id time $t ${b.mkString("[", ", ", "]")}")
        generatedRDDs += t -> callCSharpTransform(new DynamicPartitionKafkaRDD[K, V, U, T, R](
          context.sparkContext, kafkaParams, b.map(OffsetRange(_)), leaders, messageHandler, numPartitions), t).get
      }
    }
  }
}
