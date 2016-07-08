/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.streaming.api.csharp

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.language.existentials

import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.util.{ArrayList => JArrayList}
import java.util.concurrent.{LinkedBlockingQueue, ConcurrentHashMap, ThreadPoolExecutor}

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.api.csharp._
import org.apache.spark.api.csharp.SerDe._
import org.apache.spark.api.java._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ThreadUtils
import org.apache.spark.streaming.{Duration, Interval, StreamingContext, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._

import scala.language.existentials

object CSharpDStream {

  // Variables for debugging
  var debugMode = false
  var debugRDD: Option[RDD[_]] = None

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
    //debug mode enabled
    if (debugMode) {
      return debugRDD.asInstanceOf[Option[RDD[Array[Byte]]]]
    }

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
}

class CSharpDStream(
                     parent: DStream[_],
                     cSharpFunc: Array[Byte],
                     serializationMode: String)
  extends DStream[Array[Byte]] (parent.ssc) {

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isDefined && cSharpFunc != null && !cSharpFunc.isEmpty) {
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

  val localCheckpointEnabled = context.sc.conf.getBoolean("spark.mobius.streaming.localCheckpoint.enabled", false)
  logInfo("Local checkpoint is enabled: " + localCheckpointEnabled)

  if (localCheckpointEnabled) {
    val replicas = context.sc.conf.getInt("spark.mobius.streaming.localCheckpoint.replicas", 3)
    logInfo("spark.mobius.localCheckpoint.replicas is set to " + replicas)
    super.persist(StorageLevel(true, true, false, false, replicas))
  } else {
    super.persist(StorageLevel.MEMORY_ONLY)
  }

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override val mustCheckpoint = !localCheckpointEnabled

  private val numParallelJobs = parent.ssc.sc.getConf.getInt("spark.mobius.streaming.parallelJobs", 0)
  @transient private[streaming] var jobExecutor : ThreadPoolExecutor = null

  private[streaming] def runParallelJob(validTime: Time, rdd: Option[RDD[Array[Byte]]]): Unit = {
    if (numParallelJobs > 0) {
      val lastCompletedBatch = parent.ssc.progressListener.lastCompletedBatch
      val lastBatchCompleted = validTime - slideDuration == zeroTime ||
        lastCompletedBatch.isDefined && lastCompletedBatch.get.batchTime >= validTime - slideDuration
      logInfo(s"Last batch completed: $lastBatchCompleted")
      // if last batch already completed, no need to submit a parallel job
      if (!lastBatchCompleted) {
        if (jobExecutor == null) {
          jobExecutor = ThreadUtils.newDaemonFixedThreadPool(numParallelJobs, "mobius-parallel-job-executor")
        }
        rdd.get.cache()
        val queue = new java.util.concurrent.LinkedBlockingQueue[Int]
        val rddid = rdd.get.id
        val runnable = new Runnable {
          override def run(): Unit = {
            logInfo(s"Starting rdd: $rddid $validTime")
            parent.ssc.sc.runJob(rdd.get, (iterator: Iterator[Array[Byte]]) => {})
            logInfo(s"Finished rdd: $rddid $validTime")
            queue.put(rddid)
          }
        }
        jobExecutor.execute(runnable)
        logInfo(s"Waiting rdd: $rddid $validTime")
        queue.take()
        logInfo(s"Taken rdd: $rddid $validTime")
      }
    }
  }

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val lastState = getOrCompute(validTime - slideDuration)
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isDefined) {
      runParallelJob(validTime, rdd)
      val state = CSharpDStream.callCSharpTransform(List(lastState, rdd), validTime, reduceFunc,
        List(serializationMode, serializationMode2))
      if(localCheckpointEnabled && state.isDefined) {
        state.get.localCheckpoint()
      }
      state
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

case class RddPreComputeRecord[T] (
    rddSeqNum: Long,
    rdd: RDD[T])

/**
 * Used to pre-compute and materialize the input RDDs.
 */
class RddPreComputeProcessor[T](
     sc: SparkContext,
     val id: String,
     val maxPendingJobs: Int,
     val numThreads: Int,
     val outputLimit: Int,       // maximum number of records for each output
     val storageLevel: StorageLevel) extends Logging {

  // Thread pool is used to pre-compute RDDs. To preserve order, SeqNum is assigned to RDDs at
  // input, so we can reassemble them at output
  @volatile private var inputRddSeqNum: Long = 0L
  @volatile private var outputRddSeqNum: Long = 0L

  // ackedSeqNum is used for backpressure
  @volatile private var ackedSeqNum: Long = -1L

  private val precomputedRddMap = new ConcurrentHashMap[Long, RddPreComputeRecord[T]]()

  // If RDD is not pre-computed because of backpressure, it is put in this map
  private val bypassRddMap = new ConcurrentHashMap[Long, RddPreComputeRecord[T]]()

  // If RDD is not pre-computed because of backpressure, it is also put in this queue for future process
  private val pendingRddQueue = new LinkedBlockingQueue[RddPreComputeRecord[T]]()

  private var jobExecutor = ThreadUtils.newDaemonFixedThreadPool(numThreads, s"mobius-precompute-job-executor-$id")
  private val pendingQueueScheduler = ThreadUtils.newDaemonSingleThreadExecutor(s"mobius-precompute-pending-scheduler-$id")

  private def isPending(rddSeqNum: Long): Boolean = {
    rddSeqNum > ackedSeqNum + maxPendingJobs
  }

  private def logStatus(): Unit = {
    logInfo(s"log status for [$id], inputRddSeqNum: $inputRddSeqNum, ackedSeqNum: $ackedSeqNum, outputRddSeqNum: $outputRddSeqNum")
  }

  private def processPendingQueue(): Unit = {
    while (true) {
      val record = pendingRddQueue.take()
      logInfo(s"process pending queue [$id], rddSeqNum: ${record.rddSeqNum}")
      logStatus()
      while (isPending(record.rddSeqNum)) {
        Thread.sleep(100)
      }
      logStatus()
      logInfo(s"submit job from pending queue [$id], rddSeqNum: ${record.rddSeqNum}")
      jobExecutor.execute(new Runnable {
        override def run(): Unit = {
          record.rdd.persist(storageLevel)
          sc.setCallSite(s"materialize pending RDD for [$id]")
          sc.runJob(record.rdd, (iterator: Iterator[T]) => {})
        }
      })
    }
  }

  // put input RDD to processor
  def put(rdd: RDD[T]): Unit = {
    val rddSeqNum = inputRddSeqNum
    inputRddSeqNum += 1
    val record = RddPreComputeRecord[T](rddSeqNum, rdd)
    logInfo(s"put input record [$id], rddSeqNum: ${record.rddSeqNum}")
    logStatus()
    if (isPending(record.rddSeqNum)) {
      bypassRddMap.put(record.rddSeqNum, record)
      pendingRddQueue.put(record)
    } else {
      logInfo(s"submit job [$id], rddSeqNum: ${record.rddSeqNum}")
      jobExecutor.execute(new Runnable {
        override def run(): Unit = {
          record.rdd.persist(storageLevel)
          sc.setCallSite(s"materialize RDD for [$id]")
          sc.runJob(record.rdd, (iterator: Iterator[T]) => {})
          precomputedRddMap.put(record.rddSeqNum, record)
        }
      })
    }
  }

  // get output RDDs from processor
  def get(): List[RddPreComputeRecord[T]] = {
    val result = new ListBuffer[RddPreComputeRecord[T]]()
    var stop = false
    var outputNum = 0
    while (!stop) {
      val rddSeqNum = outputRddSeqNum
      if (precomputedRddMap.containsKey(rddSeqNum)) {
        result += precomputedRddMap.remove(rddSeqNum)
        outputRddSeqNum += 1
        outputNum += 1
        if (outputNum > outputLimit) {
          stop = true
        }
      } else {
        stop = true
      }
    }
    if (result.isEmpty) {
      stop = false
      outputNum = 0
      while (!stop) {
        val rddSeqNum = outputRddSeqNum
        if (bypassRddMap.containsKey(rddSeqNum)) {
          result += bypassRddMap.remove(rddSeqNum)
          outputRddSeqNum += 1
          outputNum += 1
          if (outputNum > outputLimit) {
            stop = true
          }
        } else {
          stop = true
        }
      }
    }
    result.toList
  }

  // ack consumed RDDs
  def ackRdd(rddSeqNum: Long): Unit = {
    if (ackedSeqNum < rddSeqNum) {
      ackedSeqNum = rddSeqNum
    }
  }

  def start(): Unit = {
    pendingQueueScheduler.execute(new Runnable {
      override def run(): Unit = {
        processPendingQueue()
      }
    })
  }

  def stop(): Unit = {
    jobExecutor.shutdown()
    pendingQueueScheduler.shutdown()
  }
}
