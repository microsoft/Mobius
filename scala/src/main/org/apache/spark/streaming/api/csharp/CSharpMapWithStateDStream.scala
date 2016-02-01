package org.apache.spark.streaming.api.csharp

import org.apache.spark.api.csharp.SerDe
import org.apache.spark.api.python.{PythonBroadcast, PythonRunner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.util.{StateMap, EmptyStateMap}
import org.apache.spark.util.Utils
import org.apache.spark._

import java.io._
import java.util.{List => JList, Base64}
import org.apache.spark.streaming.rdd.MapWithStateRDDRecord

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

import scala.language.existentials

object CSharpMapWithStateDStream {

  def ToJavaDStream(stream: DStream[Array[Byte]]): JavaDStream[Array[Byte]] = {
    JavaDStream.fromDStream(stream)
  }
}

private[streaming] class CSharpMapWithStateDStream(
                                                    parent: DStream[Array[Byte]],
                                                    func: Array[Byte],
                                                    timeoutIntervalInMillis: Long,
                                                    cSharpWorkerExec: String,
                                                    broadcastVars: JList[Broadcast[PythonBroadcast]],
                                                    accumulator: Accumulator[JList[Array[Byte]]])
  extends DStream[Array[Byte]](parent.context) {

  def this(parent: DStream[Array[Byte]],
           func: Array[Byte],
           timeoutIntervalInMillis: Long,
           cSharpWorkerExec: String,
           broadcastVars: JList[Broadcast[PythonBroadcast]]) =
    this(parent, func, timeoutIntervalInMillis, cSharpWorkerExec, broadcastVars, null)

  private val internalStream =
    new InternalCSharpMapWithStateDStream(parent, func, timeoutIntervalInMillis, cSharpWorkerExec,
      broadcastVars, accumulator)

  def slideDuration: Duration = internalStream.slideDuration

  def dependencies: List[DStream[_]] = List(internalStream)

  /**
   * Forward the checkpoint interval to the internal DStream that computes the state maps. This
   * to make sure that this DStream does not get checkpointed, only the internal stream.
   */
  override def checkpoint(checkpointInterval: Duration): DStream[Array[Byte]] = {
    internalStream.checkpoint(checkpointInterval)
    this
  }

  /** Return a pair DStream where each RDD is the snapshot of the state of all the keys. */
  def stateSnapshots(): DStream[Array[Byte]] = {
    internalStream.flatMap {
      _.stateMap.getAll().map { case (k, s, _) => {

        val bos = new ByteArrayOutputStream() // no need to close
        val dos = new DataOutputStream(bos)
        SerDe.writeBytes(dos, Base64.getDecoder.decode(k))
        SerDe.writeBytes(dos, s)
        dos.close()
        bos.toByteArray

      } }.toTraversable }
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    internalStream.getOrCompute(validTime).map {
      _.flatMap[Array[Byte]] {
        _.mappedData
      }
    }
  }
}

private[streaming] object InternalCSharpMapWithStateDStream {
  private val DEFAULT_CHECKPOINT_DURATION_MULTIPLIER = 10
}

private[streaming] class InternalCSharpMapWithStateDStream(
                                                            parent: DStream[Array[Byte]],
                                                            func: Array[Byte],
                                                            timeoutIntervalInMillis: Long,
                                                            cSharpWorkerExec: String,
                                                            broadcastVars: JList[Broadcast[PythonBroadcast]],
                                                            accumulator: Accumulator[JList[Array[Byte]]])
  extends DStream[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](parent.context) {

  persist(StorageLevel.MEMORY_ONLY)

  private val partitioner = new HashPartitioner(ssc.sc.defaultParallelism)

  override def dependencies: List[DStream[_]] = List(parent)

  /** Enable automatic checkpointing */
  override val mustCheckpoint = true

  override def initialize(time: Time): Unit = {
    if (checkpointDuration == null) {
      checkpointDuration = slideDuration * InternalCSharpMapWithStateDStream.DEFAULT_CHECKPOINT_DURATION_MULTIPLIER
    }
    super.initialize(time)
  }

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]]] = {
    val prevStateRDD = getOrCompute(validTime - slideDuration) match {
      case Some(rdd) => if (rdd.partitioner != Some(partitioner)) {
        CSharpMapWithStateRDD.createFromRDD(
          rdd.flatMap { _.stateMap.getAll() },
          partitioner, validTime, cSharpWorkerExec, broadcastVars, accumulator)
      } else {
        rdd
      }
      case None => {
        // TODO use spec.getInitialStateRDD instead of EmptyRDD
        val rdd = new EmptyRDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](ssc.sparkContext)
        CSharpMapWithStateRDD.createFromRDD(
          rdd.flatMap { _.stateMap.getAll() },
          partitioner, validTime, cSharpWorkerExec, broadcastVars, accumulator)
      }
    }

    val dataRDD = parent.getOrCompute(validTime).getOrElse {
      context.sparkContext.emptyRDD[Array[Byte]]
    }.map(e => {
      if(e == null || e.length == 0){
        ("", e)
      }else {
        val dis = new DataInputStream(new ByteArrayInputStream(e))
        val mappedKeyBytes = Base64.getEncoder.encodeToString(SerDe.readBytes(dis))
        dis.close()
        (mappedKeyBytes, e)
      }
    }).partitionBy(partitioner)

    val timeoutThresholdTime = Some(validTime.milliseconds - timeoutIntervalInMillis)

    Some(new CSharpMapWithStateRDD(
      prevStateRDD,
      dataRDD,
      func,
      validTime,
      timeoutThresholdTime,
      cSharpWorkerExec,
      broadcastVars,
      accumulator))
  }
}

private[spark] class CSharpMapWithStateRDD (
             var prevStateRDD: RDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]],
             var partitionedDataRDD: RDD[(String, Array[Byte])],
             command: Array[Byte],
             batchTime: Time,
             timeoutThresholdTime: Option[Long],
             cSharpWorkerExec: String,
             broadcastVars: JList[Broadcast[PythonBroadcast]],
             accumulator: Accumulator[JList[Array[Byte]]])
  extends RDD[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](
            partitionedDataRDD.sparkContext,
            List(
              new OneToOneDependency[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]](prevStateRDD),
              new OneToOneDependency(partitionedDataRDD))) {

  val bufferSize = conf.getInt("spark.buffer.size", 65536)
  val reuse_worker = conf.getBoolean("spark.python.worker.reuse", true)

  @volatile private var doFullScan = false

  require(prevStateRDD.partitioner.nonEmpty)
  require(partitionedDataRDD.partitioner == prevStateRDD.partitioner)

  override val partitioner = prevStateRDD.partitioner

  override def checkpoint(): Unit = {
    super.checkpoint()
    doFullScan = true
  }

  override def toString(): String = {
    "CSharpMapWithStateRDD, " + super.toString() + ", partitioner:" + partitioner
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prevStateRDD = null
    partitionedDataRDD = null
  }

  override def compute(
                        partition: Partition,
                        context: TaskContext):
  Iterator[MapWithStateRDDRecord[String, Array[Byte], Array[Byte]]] = {
    val stateRDDPartition = partition.asInstanceOf[CSharpMapWithStateRDDPartition]
    val prevStateRDDIterator = prevStateRDD.iterator(
      stateRDDPartition.previousSessionRDDPartition, context)
    val dataIterator = partitionedDataRDD.iterator(
      stateRDDPartition.partitionedDataRDDPartition, context)

    val prevRecord = if (prevStateRDDIterator.hasNext) Some(prevStateRDDIterator.next()) else None

    val newStateMap = prevRecord.map { _.stateMap.copy() }. getOrElse { new EmptyStateMap[String, Array[Byte]]() }
    val mappedData = new ArrayBuffer[Array[Byte]]

    if(command.length > 0) {
      val runner = new PythonRunner(
        command,
        new java.util.HashMap[String, String](),
        new java.util.ArrayList[String](),
        cSharpWorkerExec,
        "",
        broadcastVars,
        accumulator,
        bufferSize,
        reuse_worker)

      runner.compute(new MapWithStateDataIterator(partition.index, dataIterator, newStateMap, batchTime),
        partition.index, context)
        .foreach( bytes => {
          val dis = new DataInputStream(new ByteArrayInputStream(bytes))
          val key = Base64.getEncoder().encodeToString(SerDe.readBytes(dis))
          mappedData ++= Some(SerDe.readBytes(dis))

          // read state status, 1 - updated, 2 - defined, 3 - removed
          dis.readInt() match {
            case 3 => newStateMap.remove(key)
            case _ => {
              val state = SerDe.readBytes(dis)
              newStateMap.put(key, state, batchTime.milliseconds)
            }
          }
      })

      // Get the timed out state records, call the mapping function on each and collect the
      // data returned. Remove timedout data only when full scan is enabled.
      if (doFullScan && timeoutThresholdTime.isDefined) {
        runner.compute(new MapWithStateTimedoutDataIterator(newStateMap.getByTime(timeoutThresholdTime.get), batchTime),
          partition.index, context)
          .foreach(bytes => {
            val dis = new DataInputStream(new ByteArrayInputStream(bytes))
            val key = Base64.getEncoder().encodeToString(SerDe.readBytes(dis))
            mappedData ++= Some(SerDe.readBytes(dis))
            // read state status, no matter what status returned, always remove mapped state from new newStateMap
            dis.readInt()
            newStateMap.remove(key)
          })
      }
    }

    Iterator(MapWithStateRDDRecord(newStateMap, mappedData))
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate(prevStateRDD.partitions.length) { i =>
      new CSharpMapWithStateRDDPartition(i, prevStateRDD, partitionedDataRDD)}
  }
}

private [streaming] object CSharpMapWithStateRDD {

  def createFromRDD(
                     rdd: RDD[(String, Array[Byte], Long)],
                     partitioner: Partitioner,
                     updateTime: Time,
                     cSharpWorkerExec: String,
                     broadcastVars: JList[Broadcast[PythonBroadcast]],
                     accumulator: Accumulator[JList[Array[Byte]]]): CSharpMapWithStateRDD = {

    val pairRDD = rdd.map { x => (x._1, (x._2, x._3)) }
    val stateRDD = pairRDD.partitionBy(partitioner).mapPartitions({ iterator =>
      val stateMap = StateMap.create[String, Array[Byte]](SparkEnv.get.conf)
      iterator.foreach { case (key, (state, updateTime)) =>
        stateMap.put(key, state, updateTime)
      }
      Iterator(MapWithStateRDDRecord(stateMap, Seq.empty[Array[Byte]]))
    }, preservesPartitioning = true)

    val emptyDataRDD = pairRDD.sparkContext.emptyRDD[(String, Array[Byte])].partitionBy(partitioner)

    new CSharpMapWithStateRDD(
      stateRDD,
      emptyDataRDD,
      new Array[Byte](0),
      updateTime,
      None,
      cSharpWorkerExec,
      broadcastVars,
      accumulator)
  }
}

private[streaming] class MapWithStateDataIterator(
                                                 partitionIndex: Int,
                                                 dataIterator: Iterator[(String, Array[Byte])],
                                                 stateMap: StateMap[String, Array[Byte]],
                                                 batchTime: Time)
  extends Iterator[(Array[Byte])] {

  def hasNext = dataIterator.hasNext

  def next = {
    val (key, pairBytes) = dataIterator.next()
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos) // since it's backed by ByteArrayOutputStream, no need to close it explicitly.

    stateMap.get(key) match {
      case Some(stateBytes) => SerDe.writeBytes(dos, stateBytes)
      case None => SerDe.writeBytes(dos, new Array[Byte](0))
    }

    SerDe.writeLong(dos, batchTime.milliseconds)
    pairBytes ++ bos.toByteArray
  }
}

private[streaming] class MapWithStateTimedoutDataIterator(
                                                   dataIterator: Iterator[(String, Array[Byte], Long)],
                                                   batchTime: Time)
  extends Iterator[(Array[Byte])] {

  def hasNext = dataIterator.hasNext

  def next = {
    // [key bytes length] + [key bytes] + [0000](null value) + [state bytes length]
    // + [state bytes] + [batch time length] + [batch time bytes]
    val (key, stateBytes, _) = dataIterator.next()
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)
    SerDe.writeBytes(dos, Base64.getDecoder.decode(key))
    SerDe.writeInt(dos, 0)
    SerDe.writeBytes(dos, stateBytes)
    SerDe.writeLong(dos, batchTime.milliseconds)
    dos.flush()
    bos.toByteArray
  }
}

private[streaming] class CSharpMapWithStateRDDPartition(
                                                 idx: Int,
                                                 @transient var prevStateRDD: RDD[_],
                                                 @transient var partitionedDataRDD: RDD[_])
  extends Partition {

  private[streaming] var previousSessionRDDPartition: Partition = null
  private[streaming] var partitionedDataRDDPartition: Partition = null

  override def hashCode(): Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    previousSessionRDDPartition = prevStateRDD.partitions(index)
    partitionedDataRDDPartition = partitionedDataRDD.partitions(index)
    oos.defaultWriteObject()
  }

  override def index: Int = idx
}
