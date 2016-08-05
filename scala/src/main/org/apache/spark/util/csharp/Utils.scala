/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package org.apache.spark.util.csharp

import java.io._
import java.nio.file._
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermission._
import java.util.{Timer, TimerTask}

import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipArchiveOutputStream, ZipFile}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.collection.Set

/**
 * Utility methods used by SparkCLR.
 */
object Utils extends Logging {
  private val posixFilePermissions = Array(
    OWNER_READ, OWNER_WRITE, OWNER_EXECUTE,
    GROUP_READ, GROUP_WRITE, GROUP_EXECUTE,
    OTHERS_READ, OTHERS_WRITE, OTHERS_EXECUTE
  )

  val supportPosix = FileSystems.getDefault.supportedFileAttributeViews().contains("posix")

  /**
   * Compress all files under given directory into one zip file and drop it to the target directory
   *
   * @param sourceDir source directory to zip
   * @param targetZipFile target zip file
   */
  def zip(sourceDir: File, targetZipFile: File): Unit = {
    var fos: FileOutputStream = null
    var zos: ZipArchiveOutputStream = null
    try {
      fos = new FileOutputStream(targetZipFile)
      zos = new ZipArchiveOutputStream(fos)

      val sourcePath = sourceDir.toPath
      FileUtils.listFiles(sourceDir, null, true).asScala.foreach { file =>
        var in: FileInputStream = null
        try {
          val path = file.toPath
          val entry = new ZipArchiveEntry(sourcePath.relativize(path).toString)
          if (supportPosix) {
            entry.setUnixMode(permissionsToMode(Files.getPosixFilePermissions(path).asScala)
              | (if (entry.getName.endsWith(".exe")) 0x1ED else 0x1A4))
          } else if (entry.getName.endsWith(".exe")) {
            entry.setUnixMode(0x1ED) // 755
          } else {
            entry.setUnixMode(0x1A4) // 644
          }
          zos.putArchiveEntry(entry)

          in = new FileInputStream(file)
          IOUtils.copy(in, zos)
          zos.closeArchiveEntry()
        } finally {
          IOUtils.closeQuietly(in)
        }
      }
    } finally {
      IOUtils.closeQuietly(zos)
      IOUtils.closeQuietly(fos)
    }
  }

  /**
   * Unzip a file to the given directory
   *
   * @param file file to be unzipped
   * @param targetDir target directory
   */
  def unzip(file: File, targetDir: File): Unit = {
    var zipFile: ZipFile = null
    try {
      targetDir.mkdirs()
      zipFile = new ZipFile(file)
      zipFile.getEntries.asScala.foreach { entry =>
        val targetFile = new File(targetDir, entry.getName)

        if (targetFile.exists()) {
          logWarning(s"Target file/directory $targetFile already exists. Skip it for now. " +
            s"Make sure this is expected.")
        } else {
          if (entry.isDirectory) {
            targetFile.mkdirs()
          } else {
            targetFile.getParentFile.mkdirs()
            val input = zipFile.getInputStream(entry)
            val output = new FileOutputStream(targetFile)
            IOUtils.copy(input, output)
            IOUtils.closeQuietly(input)
            IOUtils.closeQuietly(output)
            if(supportPosix) {
              Files.setPosixFilePermissions(
                targetFile.toPath, modeToPermissions(entry.getUnixMode).asJava)
            }
          }
        }
      }
    } catch {
      case e: Exception => logError("exception caught during decompression:" + e)
    } finally {
      ZipFile.closeQuietly(zipFile)
    }
  }

  /**
   * Exits the JVM, trying to do it nicely, otherwise doing it nastily.
   *
   * @param status  the exit status, zero for OK, non-zero for error
   * @param maxDelayMillis  the maximum delay in milliseconds
   */
  def exit(status: Int, maxDelayMillis: Long) {
    try {
      logInfo(s"Utils.exit() with status: $status, maxDelayMillis: $maxDelayMillis")

      // setup a timer, so if nice exit fails, the nasty exit happens
      val timer = new Timer()
      timer.schedule(new TimerTask() {
        @Override
        def run() {
          Runtime.getRuntime.halt(status)
        }
      }, maxDelayMillis)
      // try to exit nicely
      System.exit(status);
    } catch {
      // exit nastily if we have a problem
      case ex: Throwable => Runtime.getRuntime.halt(status)
    } finally {
      // should never get here
      Runtime.getRuntime.halt(status)
    }
  }

  /**
   * Exits the JVM, trying to do it nicely, wait 1 second
   *
   * @param status  the exit status, zero for OK, non-zero for error
   */
  def exit(status: Int): Unit = {
    exit(status, 1000)
  }

  private[spark] def listZipFileEntries(file: File): Array[String] = {
    var zipFile: ZipFile = null
    try {
      zipFile = new ZipFile(file)
      zipFile.getEntries.asScala.map(_.getName).toArray
    } finally {
      ZipFile.closeQuietly(zipFile)
    }
  }

  private[this] def permissionsToMode(permissions: Set[PosixFilePermission]): Int = {
    posixFilePermissions.foldLeft(0) { (mode, perm) =>
      (mode << 1) | (if (permissions.contains(perm)) 1 else 0)
    }
  }

  private[this] def modeToPermissions(mode: Int): Set[PosixFilePermission] = {
    posixFilePermissions.zipWithIndex
      .filter { case (_, i) => (mode & (0x100 >>> i)) != 0 }
      .map(_._1).toSet
  }
}
