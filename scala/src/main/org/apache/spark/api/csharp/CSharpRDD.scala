/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.api.csharp

import java.nio.ByteBuffer
import java.nio.channels.{OverlappingFileLockException, FileChannel, FileLock}
import java.util.{List => JList, Map => JMap}
import java.io._
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.api.python.{PythonBroadcast, PythonRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.util.csharp.{Utils => CSharpUtils}

/**
 * RDD used for forking an external C# process and pipe in & out the data
 * between JVM and CLR. Since PythonRDD already has the required implementation
 * it just extends from it without overriding any behavior for now
 */
class CSharpRDD(
    @transient parent: RDD[_],
    command: Array[Byte],
    envVars: JMap[String, String],
    cSharpIncludes: JList[String],
    preservePartitioning: Boolean,
    cSharpWorkerExecutable: String,
    unUsedVersionIdentifier: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: Accumulator[JList[Array[Byte]]])
  extends PythonRDD (
    parent,
    command,
    envVars,
    cSharpIncludes,
    preservePartitioning,
    cSharpWorkerExecutable,
    unUsedVersionIdentifier,
    broadcastVars,
    accumulator) {

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    unzip(new File(cSharpWorkerExecutable).getAbsoluteFile.getParentFile)
    logInfo(s"compute CSharpRDD[${this.id}], stageId: ${context.stageId()}" +
      s", partitionId: ${context.partitionId()}, split_index: ${split.index}")

    // fill rddId, stageId and partitionId info
    val bb = ByteBuffer.allocate(12)
    bb.putInt(this.id)
    bb.putInt(context.stageId())
    bb.putInt(context.partitionId())
    val bytes = bb.array()
    for (i <- 0 until bytes.size) {
      command(i) = bytes(i)
    }

    if (CSharpRDD.maxCSharpWorkerProcessCount >= 0) {
      var workerFactoryId = CSharpRDD.getWorkerFactoryId(context.stageId())
      // change envVars to use different PythonWorkerFactory
      envVars.put("WORKER_FACTORY_ID", workerFactoryId.toString)
      logInfo(s"workerFactoryId: $workerFactoryId")
    }

    super.compute(split, context)
  }

  /**
   * Uncompress all zip files under directory cSharpWorkerWorkingDir.
   * As .zip file is supported to be submitted by sparkclr-submit.cmd, and there might be
   * some runtime dependencies of cSharpWorker.exe in the zip files,
   * so before start to execute cSharpWorker.exe, uncompress all zip files first.
   *
   * One executor might process multiple splits, if zip files have already been unzipped
   * in the previous split, there is no need to unzip them again.
   * Once uncompression is done, a flag file "doneFlag" will be created.
   * @param cSharpWorkerWorkingDir directory where cSharpWorker.exe is located
   */
  private def unzip(cSharpWorkerWorkingDir: File): Unit = {

    val files = cSharpWorkerWorkingDir.list.filter(_.toLowerCase.endsWith(".zip"))

    val lockName = "_unzip_lock"
    val unzippingFlagName = "_unzipping"
    val doneFlagName = "_unzip_done"

    if (files.length == 0) {
      logWarning("Found no zip files.")
      return
    } else {
      logInfo("Found zip files: " + files.mkString(","))
    }

    val doneFlag = new File(cSharpWorkerWorkingDir, doneFlagName)

    // check whether all zip files have already uncompressed
    if (doneFlag.exists()) {
      logInfo("Already unzipped all zip files, skip.")
      return
    }

    val unzippingFlag = new File(cSharpWorkerWorkingDir, unzippingFlagName)

    // if another thread is uncompressing files,
    // current thread just needs to wait the operation done and return
    if (unzippingFlag.exists()) {
      waitUnzipOperationDone(doneFlag)
      return
    }

    val lockFile = new File(cSharpWorkerWorkingDir, lockName)
    var file: RandomAccessFile = null
    var lock: FileLock = null
    var channel: FileChannel = null

    try {
      file = new RandomAccessFile(lockFile, "rw")
      channel = file.getChannel()
      lock = channel.tryLock()

      if (lock == null) {
        logWarning("Failed to obtain lock for file " + lockFile.getPath)
        waitUnzipOperationDone(doneFlag)
        return
      }

      // check again whether un-compression operation already done
      if (new File(cSharpWorkerWorkingDir, doneFlagName).exists()) {
        return
      }

      // unzippingFlag file will be deleted before release the lock
      // so if obtain the lock successfully, there is no chance that the unzippingFlag still exists
      unzippingFlag.createNewFile()

      // unzip file
      for (zipFile <- files) {
        CSharpUtils.unzip(new File(cSharpWorkerWorkingDir, zipFile), cSharpWorkerWorkingDir)
        logInfo("Unzip file: " + zipFile)
      }

      doneFlag.createNewFile()
      unzippingFlag.delete()
      logInfo("Unzip done.")

    } catch {
      case e: OverlappingFileLockException => {
        logInfo("Already obtained the lock.")
        waitUnzipOperationDone(doneFlag)
      }
      case e: Exception => e.printStackTrace()
    }
    finally {
      if (lock != null && lock.isValid()) lock.release()
      if (channel != null && channel.isOpen) channel.close()
      if (file != null) file.close()
    }
  }

  /**
   * Wait until doneFlag file is created, or total waiting time exceeds threshold
   * @param doneFlag
   */
  private def waitUnzipOperationDone(doneFlag: File): Unit = {
    val maxSleepTimeInSeconds = 30 // max wait time
    var sleepTimeInSeconds = 0
    val interval = 5

    while (true) {

      if (!doneFlag.exists()) {
        if (sleepTimeInSeconds > maxSleepTimeInSeconds) {
          return
        }

        sleepTimeInSeconds += interval
        Thread.sleep(5 * 1000) // sleep 5 seconds
      } else {
        return
      }
    }
  }
}

object CSharpRDD {
  var currentStageId: Int = 0
  var nextSeqNum: Int = 0

  // long running multi-process CSharpWorker mode is enabled only when configurated explicitly
  var maxCSharpWorkerProcessCount: Int = SparkEnv.get.conf.getInt("spark.mobius.CSharpWorker.maxProcessCount", -1)

  def createRDDFromArray(
      sc: SparkContext,
      arr: Array[Array[Byte]],
      numSlices: Int): JavaRDD[Array[Byte]] = {
    JavaRDD.fromRDD(sc.parallelize(arr, numSlices))
  }
  
  //this method is called when saveAsTextFile is called on RDD<string>
  //calling saveAsTextFile() on CSharpRDDs result in bytes written to text file - this method converts bytes to string before writing to file
  def saveStringRddAsTextFile(javaRdd: JavaRDD[Array[Byte]], path: String) = {
    var stringRdd = JavaRDD.toRDD(javaRdd).map(s => new String(s, "UTF-8"))
    stringRdd.saveAsTextFile(path)
  }

  //this method is called when saveAsTextFile is called on RDD<string>
  //calling saveAsTextFile() on CSharpRDDs result in bytes written to text file - this method converts bytes to string before writing to file
  def saveStringRddAsTextFile(javaRdd: JavaRDD[Array[Byte]], path: String, codec: Class[_ <: CompressionCodec]) = {
    var stringRdd = JavaRDD.toRDD(javaRdd).map(s => new String(s, "UTF-8"))
    stringRdd.saveAsTextFile(path, codec)
  }

  //In long runing multi-process CSharpWorker mode, multiple WorkerFactory is created, with each
  //WorkerFactory launching one long running CSharpWorker process.
  private def getWorkerFactoryId(stageId: Int): Int = {
    synchronized {
      if (stageId != currentStageId) {
        // new stage is started
        currentStageId = stageId
        nextSeqNum = 0
      }
      val workerFactoryId = {
        if (maxCSharpWorkerProcessCount != 0) {
          nextSeqNum % maxCSharpWorkerProcessCount
        } else {
          nextSeqNum
        }
      }
      nextSeqNum += 1
      workerFactoryId
    }
  }
}
