/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.api.csharp

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, FileLock, OverlappingFileLockException}
import java.nio.file.{Files, Paths}
import java.nio.file.attribute.PosixFilePermission._
import java.util.{List => JList, Map => JMap}

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python.{PythonBroadcast, PythonRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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
    // hacky check
    // TODO: fix this
    {
      var path = Paths.get(cSharpWorkerExecutable)
      if (!path.isAbsolute && path.getNameCount == 1) {
        path = Paths.get(".", path.toString)
      }
      path.toString
    },
    unUsedVersionIdentifier,
    broadcastVars,
    accumulator) {

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val cSharpWorker = new File(cSharpWorkerExecutable).getAbsoluteFile
    unzip(cSharpWorker.getParentFile)

    // hacky check
    // TODO: fix this
    val cSharpWorkerPath = cSharpWorker.toPath
    if (CSharpUtils.supportPosix) {
      val permissions = Files.getPosixFilePermissions(cSharpWorkerPath)
      permissions.add(OWNER_EXECUTE)
      permissions.add(GROUP_EXECUTE)
      permissions.add(OTHERS_EXECUTE)
      Files.setPosixFilePermissions(cSharpWorkerPath, permissions)
    }

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
      channel = file.getChannel
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
      case e: OverlappingFileLockException =>
        logInfo("Already obtained the lock.")
        waitUnzipOperationDone(doneFlag)
      case e: Exception => e.printStackTrace()
    }
    finally {
      if (lock != null && lock.isValid) lock.release()
      if (channel != null && channel.isOpen) channel.close()
      if (file != null) file.close()
    }
  }

  /**
   * Wait until doneFlag file is created, or total waiting time exceeds threshold
   * @param doneFlag doneFlag file
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
  def createRDDFromArray(
      sc: SparkContext,
      arr: Array[Array[Byte]],
      numSlices: Int): JavaRDD[Array[Byte]] = {
    JavaRDD.fromRDD(sc.parallelize(arr, numSlices))
  }
}
