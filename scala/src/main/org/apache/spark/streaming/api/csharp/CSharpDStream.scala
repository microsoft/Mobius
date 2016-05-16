/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.streaming.api.csharp

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.api.csharp._
import org.apache.spark.api.csharp.SerDe._
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.util.{ArrayList => JArrayList, UUID}
import java.util.concurrent.{LinkedBlockingQueue, ExecutorService, ConcurrentHashMap}

import org.apache.spark.util.ThreadUtils

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import org.apache.spark.api.java._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Interval, StreamingContext, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._

import scala.language.existentials
import scala.reflect.ClassTag

object CSharpDStream {

  /**
   * helper function for DStream.foreachRDD().
   */
  def callForeachRDD(jdstream: JavaDStream[Array[Byte]], cSharpFunc: Array[Byte],
                     serializationMode: String) {
    val func = (rdd: RDD[_], time: Time) => {
      val res = callCSharpTransform(List(Some(rdd)), time, cSharpFunc, List(serializationMode))
    }
    jdstream.dstream.foreachRDD(func)
  }

  def callCSharpTransform(rdds: List[Option[RDD[_]]], time: Time, cSharpfunc: Array[Byte],
                          serializationModeList: List[String]): Option[RDD[Array[Byte]]] = {
    var socket: Socket = null
    try {
      socket = CSharpBackend.callbackSockets.poll()
      if (socket == null) {
        socket = new Socket("localhost", CSharpBackend.callbackPort)
      }

      val dos = new DataOutputStream(socket.getOutputStream())
      val dis = new DataInputStream(socket.getInputStream())

      writeString(dos, "callback")
      writeInt(dos, rdds.size)
      rdds.foreach(x => writeObject(dos,
        x.map(JavaRDD.fromRDD(_).asInstanceOf[AnyRef]).orNull))
      writeDouble(dos, time.milliseconds.toDouble)
      writeBytes(dos, cSharpfunc)
      serializationModeList.foreach(x => writeString(dos, x))
      dos.flush()
      val result = Option(readObject(dis).asInstanceOf[JavaRDD[Array[Byte]]]).map(_.rdd)
      CSharpBackend.callbackSockets.offer(socket)
      result
    } catch {
      case e: Exception =>
        // log exception only when callback socket is not shutdown explicitly
        if (!CSharpBackend.callbackSocketShutdown) {
          // TODO: change println to log
          System.err.println("CSharp transform callback failed with " + e) // scalastyle:off println
          e.printStackTrace()
        }

        // close this socket if error happen
        if (socket != null) {
          try {
            socket.close()
          }
        }

        None
    }
  }

  /**
   * convert list of RDD into queue of RDDs, for ssc.queueStream()
   */
  def toRDDQueue(rdds: JArrayList[JavaRDD[Array[Byte]]]): java.util.Queue[JavaRDD[Array[Byte]]] = {
    val queue = new java.util.LinkedList[JavaRDD[Array[Byte]]]
    rdds.forall(queue.add(_))
    queue
  }

  /**
   * Create a AsyncUnionDStream from multiple DStreams of the same type and same slide duration.
   */
  def unionAsync[T](ssc: StreamingContext, javaDStreams: JArrayList[JavaDStream[T]]):
      JavaDStream[T] = {
    val first = javaDStreams(0)
    implicit val cm: ClassTag[T] = first.classTag
    val dstreams: Seq[DStream[T]] = javaDStreams.asScala.map(_.dstream)
    val dstream = ssc.withScope {
      new AsyncUnionDStream[T](dstreams.toArray)
    }
    new JavaDStream[T](dstream)
  }
}

class CSharpDStream(
                     parent: DStream[_],
                     cSharpFunc: Array[Byte],
                     serializationMode: String)
  extends DStream[Array[Byte]] (parent.ssc) {

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val rdd = parent.compute(validTime)
    if (rdd.isDefined) {
      CSharpDStream.callCSharpTransform(List(rdd), validTime, cSharpFunc, List(serializationMode))
    } else {
      None
    }
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * Transformed from two DStreams in CSharp
 */
class CSharpTransformed2DStream(
                                 parent: DStream[_],
                                 parent2: DStream[_],
                                 cSharpFunc: Array[Byte],
                                 serializationMode: String,
                                 serializatioinMode2: String)
  extends DStream[Array[Byte]] (parent.ssc) {

  override def dependencies: List[DStream[_]] = List(parent, parent2)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val empty: RDD[_] = ssc.sparkContext.emptyRDD
    val rdd1 = Some(parent.getOrCompute(validTime).getOrElse(empty))
    val rdd2 = Some(parent2.getOrCompute(validTime).getOrElse(empty))
    CSharpDStream.callCSharpTransform(List(rdd1, rdd2), validTime, cSharpFunc,
      List(serializationMode, serializatioinMode2))
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * similar to ReducedWindowedDStream
 */
class CSharpReducedWindowedDStream(
                                    parent: DStream[Array[Byte]],
                                    csharpReduceFunc: Array[Byte],
                                    csharpInvReduceFunc: Array[Byte],
                                    _windowDuration: Duration,
                                    _slideDuration: Duration,
                                    serializationMode: String)
  extends DStream[Array[Byte]] (parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY)

  override def dependencies: List[DStream[_]] = List(parent)

  override val mustCheckpoint: Boolean = true

  def windowDuration: Duration = _windowDuration

  override def slideDuration: Duration = _slideDuration

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val currentTime = validTime
    val current = new Interval(currentTime - windowDuration, currentTime)
    val previous = current - slideDuration

    //  _____________________________
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    //                     |_____________________________|
    //
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs
    //
    val previousRDD = getOrCompute(previous.endTime)

    // for small window, reduce once will be better than twice
    if (csharpInvReduceFunc != null && previousRDD.isDefined
      && windowDuration >= slideDuration * 5) {

      // subtract the values from old RDDs
      val oldRDDs = parent.slice(previous.beginTime + parent.slideDuration, current.beginTime)
      val subtracted = if (oldRDDs.size > 0) {
        CSharpDStream.callCSharpTransform(List(previousRDD, Some(ssc.sc.union(oldRDDs))),
          validTime, csharpInvReduceFunc, List(serializationMode, serializationMode))
      } else {
        previousRDD
      }

      // add the RDDs of the reduced values in "new time steps"
      val newRDDs = parent.slice(previous.endTime + parent.slideDuration, current.endTime)
      if (newRDDs.size > 0) {
        CSharpDStream.callCSharpTransform(List(subtracted, Some(ssc.sc.union(newRDDs))),
          validTime, csharpReduceFunc, List(serializationMode, serializationMode))
      } else {
        subtracted
      }
    } else {
      // Get the RDDs of the reduced values in current window
      val currentRDDs = parent.slice(current.beginTime + parent.slideDuration, current.endTime)
      if (currentRDDs.size > 0) {
        CSharpDStream.callCSharpTransform(List(None, Some(ssc.sc.union(currentRDDs))),
          validTime, csharpReduceFunc, List(serializationMode, serializationMode))
      } else {
        None
      }
    }
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * similar to StateDStream
 */
class CSharpStateDStream(
                          parent: DStream[Array[Byte]],
                          reduceFunc: Array[Byte],
                          serializationMode: String,
                          serializationMode2: String)
  extends DStream[Array[Byte]](parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY)

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val lastState = getOrCompute(validTime - slideDuration)
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isDefined) {
      CSharpDStream.callCSharpTransform(List(lastState, rdd), validTime, reduceFunc,
        List(serializationMode, serializationMode2))
    } else {
      lastState
    }
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * MapWithStateDStream API support follows 'internal' DStream idea from
 * scala\org\apache\spark\streaming\dstream\MapWithStateDStream.scala
 */

class CSharpMapWithStateDStream(
                           parent: DStream[Array[Byte]],
                           mappingFunc: Array[Byte],
                           deserializer: String,
                           deserializer2: String)
  extends DStream[Array[Byte]](parent.ssc) {

  private val internalStream =
    new InternalMapWithStateDStream(parent, mappingFunc, deserializer, deserializer2)

  override def dependencies: List[DStream[_]] = List(internalStream)

  override def slideDuration: Duration = internalStream.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    internalStream.getOrCompute(validTime).map { _.mapPartitions(x => x.drop(1), true) }
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

class InternalMapWithStateDStream(
                                   parent: DStream[Array[Byte]],
                                   mappingFunc: Array[Byte],
                                   deserializer: String,
                                   deserializer2: String)
  extends DStream[Array[Byte]](parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY)

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val lastState = getOrCompute(validTime - slideDuration).map { _.mapPartitions(x => x.take(1), true) }
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isDefined) {
      CSharpDStream.callCSharpTransform(List(lastState, rdd), validTime, mappingFunc,
        List(deserializer, deserializer2))
    } else {
      lastState
    }
  }
}

/**
  * An input stream that always returns the same RDD on each timestep. Useful for testing.
  */
class CSharpConstantInputDStream(ssc_ : StreamingContext, rdd: RDD[Array[Byte]])
  extends ConstantInputDStream[Array[Byte]](ssc_, rdd) {

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/*
 * asynchronously pre-compute input RDDs, and then output the union of them
 */
class AsyncRddCombiner[T](sc: SparkContext, id: String) extends Logging {
  // Thread pool is used to pre-compute RDDs, so seqNum is assigned to RDDs at input, and we can
  // reassemble them in output
  @volatile private var inputRddSeqNum: Long = 0L
  @volatile private var outputRddSeqNum: Long = 0L

  // ackedSeqNum is used for back-pressure
  @volatile private var ackedSeqNum: Long = -1L

  // the element is Tuple3(rddSeqNum, userInfo, rdd)
  private val inputRddQueue: LinkedBlockingQueue[(Long, Long, RDD[T])] =
    new LinkedBlockingQueue[(Long, Long, RDD[T])]()

  // key is rddSeqNum, value is Tuple2(userInfo, rdd)
  private val precomputedRddMap: ConcurrentHashMap[Long, (Long, RDD[T])] = new ConcurrentHashMap()

  // If RDD is not pre-computed because of back pressure, it is put in this queue for future process
  // the element is Tuple3(rddSeqNum, userInfo, rdd)
  private val bypassRddQueue: LinkedBlockingQueue[(Long, Long, RDD[T])] =
    new LinkedBlockingQueue[(Long, Long, RDD[T])]()

  // If rdd is not pre-computed because of back pressure, it is put in this map
  // key is rddSeqNum, value is Tuple2(userInfo, rdd)
  private val bypassRddMap: ConcurrentHashMap[Long, (Long, RDD[T])] = new ConcurrentHashMap()

  private val maxPendingNum = sc.getConf.getInt(
    "spark.mobius.streaming.asyncUnion.maxPendingNum", 2)
  private val nThreads = sc.getConf.getInt(
    "spark.mobius.streaming.asyncUnion.numThreads", maxPendingNum)
  private var jobExecutor = ThreadUtils.newDaemonFixedThreadPool(nThreads, s"job-executor-$id")
  private val inputQueueScheduler = ThreadUtils.newDaemonSingleThreadExecutor(
    s"input-scheduler-$id")
  private val bypassQueueScheduler = ThreadUtils.newDaemonSingleThreadExecutor(
    s"bypass-scheduler-$id")

  private def isPending(rddSeqNum: Long): Boolean = {
    rddSeqNum > ackedSeqNum + maxPendingNum
  }

  private def processInputQueue(): Unit = {
    while (true) {
      val (rddSeqNum, userInfo, rdd) = inputRddQueue.take()
      logInfo(s"processInputQueue[$id], rddSeqNum: $rddSeqNum, ackedSeqNum: $ackedSeqNum, " +
        s"outputRddSeqNum: $outputRddSeqNum")
      if (isPending(rddSeqNum)) {
        bypassRddMap.put(rddSeqNum, (userInfo, rdd))
        bypassRddQueue.put(rddSeqNum, userInfo, rdd)
      } else {
        logInfo(s"processInputQueue submitting job, rddSeqNum: $rddSeqNum, " +
          s"ackedSeqNum: $ackedSeqNum, outputRddSeqNum: $outputRddSeqNum")
        jobExecutor.execute(new Runnable {
          override def run(): Unit = {
            rdd.cache()
            sc.setCallSite(s"runJob for [$id]")
            sc.runJob(rdd, (iterator: Iterator[T]) => {})
            precomputedRddMap.put(rddSeqNum, (userInfo, rdd))
          }
        })
      }
    }
  }

  private def processBypassQueue(): Unit = {
    while (true) {
      val (rddSeqNum, userInfo, rdd) = bypassRddQueue.take()
      logInfo(s"processBypassQueue[$id], rddSeqNum: $rddSeqNum, ackedSeqNum: $ackedSeqNum," +
        s" outputRddSeqNum: $outputRddSeqNum")
      while (isPending(rddSeqNum)) {
        Thread.sleep(500)
      }
      logInfo(s"processBypassQueue submitting job, rddSeqNum: $rddSeqNum, " +
        s"ackedSeqNum: $ackedSeqNum, outputRddSeqNum: $outputRddSeqNum")
      jobExecutor.execute(new Runnable {
        override def run(): Unit = {
          rdd.cache()
          sc.setCallSite(s"runJob for [$id]")
          sc.runJob(rdd, (iterator: Iterator[T]) => {})
        }
      })
    }
  }

  inputQueueScheduler.execute(new Runnable {
    override def run(): Unit = {
      processInputQueue()
    }
  })

  bypassQueueScheduler.execute(new Runnable {
    override def run(): Unit = {
      processBypassQueue()
    }
  })

  // feed input RDD
  def input(userInfo: Long, rdd: RDD[T]): Unit = {
    logInfo(s"add input, inputRddSeqNum: $inputRddSeqNum")
    val rddSeqNum = inputRddSeqNum
    inputRddSeqNum += 1
    inputRddQueue.put(rddSeqNum, userInfo, rdd)
  }

  // get pre-computed output RDDs
  def output(): List[(Long, Long, RDD[T])] = {
    val result = new ArrayBuffer[(Long, Long, RDD[T])]()
    var stop = false
    while (!stop) {
      val rddSeqNum = outputRddSeqNum
      if (precomputedRddMap.containsKey(rddSeqNum)) {
        val (userInfo, rdd) = precomputedRddMap.remove(rddSeqNum)
        result += Tuple3(rddSeqNum, userInfo, rdd)
        outputRddSeqNum += 1
      } else {
        stop = true
      }
    }
    if (result.isEmpty) {
      val rddSeqNum = outputRddSeqNum
      if (bypassRddMap.containsKey(rddSeqNum)) {
        val (userInfo, rdd) = bypassRddMap.remove(rddSeqNum)
        result += Tuple3(rddSeqNum, userInfo, rdd)
        outputRddSeqNum += 1
      }
    }
    result.toList
  }

  def ackSeqNum(seqNum: Long): Unit = {
    if (ackedSeqNum < seqNum) {
      ackedSeqNum = seqNum
    }
  }
}

/**
 * Similar to UnionDStream, but RDDs of parents DStream will be precomputed and cached
 * asynchronously.
 */
class AsyncUnionDStream[T: ClassTag](parents: Array[DStream[T]])
  extends DStream[T](parents.head.ssc) {

  require(parents.length > 0, "List of DStreams to AyncUnion is empty")
  require(parents.map(_.ssc).distinct.size == 1, "Some of the DStreams have different contexts")
  require(parents.map(_.slideDuration).distinct.size == 1,
    "Some of the DStreams have different slide durations")

  override def dependencies: List[DStream[_]] = parents.toList

  override def slideDuration: Duration = parents.head.slideDuration

  private val parentsNum = parents.length

  @transient private var initialized: Boolean = false
  @transient private var rddCombiners: Array[AsyncRddCombiner[T]] = null

  // for the HashMap, key is time, value is List[Tuple2(rddSeqNum, time)]
  @transient private var mappingOfRddInfos: Array[ConcurrentHashMap[Time, List[(Long, Time)]]]
    = null

  private def init(): Unit = {
    if (!initialized) {
      rddCombiners = new Array[AsyncRddCombiner[T]](parentsNum)
      mappingOfRddInfos = new Array[ConcurrentHashMap[Time, List[(Long, Time)]]](parentsNum)
      for (i <- 0 until parentsNum) {
        rddCombiners(i) = new AsyncRddCombiner[T](ssc.sc, s"AsyncUnionDStream-$i")
        mappingOfRddInfos(i) = new ConcurrentHashMap()
      }
      initialized = true
    }
  }

  override def compute(validTime: Time): Option[RDD[T]] = {
    if (!initialized) init()
    for (i <- 0 until parentsNum) {
      val rddOption = parents(i).getOrCompute(validTime)
      if (rddOption.isDefined) {
        val rdd = rddOption.get
        rddCombiners(i).input(validTime.milliseconds, rdd)
      }
    }

    // get pre-computed RDDs
    val rdds = new ArrayBuffer[RDD[T]]()
    for (i <- 0 until parentsNum) {
      val rddInfos = new ArrayBuffer[(Long, Time)]()
      for ((rddSeqNum, userInfo, rdd) <- rddCombiners(i).output()) {
        rdds += rdd
        rddInfos += Tuple2(rddSeqNum, Time(userInfo))
      }
      mappingOfRddInfos(i).put(validTime, rddInfos.toList)
    }
    if (rdds.nonEmpty) {
      Some(new UnionRDD(ssc.sc, rdds))
    } else {
      Some(ssc.sc.emptyRDD[T])
    }
  }

  // override this method to prevent prematurely unpersist parent RDDs that are still cached
  override private[streaming] def clearMetadata(time: Time): Unit = {
    val unpersistData = ssc.conf.getBoolean("spark.streaming.unpersist", true)
    val oldRDDs = generatedRDDs.filter(_._1 <= (time - rememberDuration))
    logDebug("Clearing references to old RDDs: [" +
      oldRDDs.map(x => s"${x._1} -> ${x._2.id}").mkString(", ") + "]")
    generatedRDDs --= oldRDDs.keys
    if (unpersistData) {
      logDebug("Unpersisting old RDDs: " + oldRDDs.values.map(_.id).mkString(", "))
      oldRDDs.values.foreach { rdd =>
        rdd.unpersist(false)
        // Explicitly remove blocks of BlockRDD
        rdd match {
          case b: BlockRDD[_] =>
            logInfo("Removing blocks of RDD " + b + " of time " + time)
            b.removeBlocks()
          case _ =>
        }
      }
    }
    logDebug("Cleared " + oldRDDs.size + " RDDs that were older than " +
      (time - rememberDuration) + ": " + oldRDDs.keys.mkString(", "))

    for (i <- 0 until parentsNum) {
      if (mappingOfRddInfos(i).containsKey(time)) {
        for ((rddSeqNum, rddTime) <- mappingOfRddInfos(i).remove(time)) {
          dependencies(i).clearMetadata(rddTime)
          rddCombiners(i).ackSeqNum(rddSeqNum)
        }
      }
    }
  }
}
