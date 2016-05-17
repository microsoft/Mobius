/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package org.apache.spark.util.csharp

import java.io._
import java.util.{Timer, TimerTask}
import scala.collection.JavaConverters._
import java.nio.file._
import java.nio.file.attribute.PosixFilePermission

import org.apache.commons.compress.archivers.zip.ZipFile
import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipArchiveOutputStream}
import org.apache.commons.io.IOUtils

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

/**
 * Utility methods used by SparkCLR.
 */
object Utils {

  val isPosix = FileSystems.getDefault().supportedFileAttributeViews().contains("posix")
  val posixPermissionToInteger: Map[PosixFilePermission, Integer] = Map(
    PosixFilePermission.OWNER_EXECUTE -> 0100,
    PosixFilePermission.OWNER_WRITE -> 0200,
    PosixFilePermission.OWNER_READ -> 0400,

    PosixFilePermission.GROUP_EXECUTE -> 0010,
    PosixFilePermission.GROUP_WRITE -> 0020,
    PosixFilePermission.GROUP_READ -> 0040,

    PosixFilePermission.OTHERS_EXECUTE -> 0001,
    PosixFilePermission.OTHERS_WRITE -> 0002,
    PosixFilePermission.OTHERS_READ ->0004)

  /**
   * List all entries of a zip file
    *
    * @param file zip file
   */
  def listZipFileEntries(file: File): Seq[String] = {

    val result = ArrayBuffer[String]()
    val zipFile = new ZipFile(file)

    try {
      val entries = zipFile.getEntries
      while (entries.hasMoreElements()) {
        result.append(entries.nextElement().getName)
      }
    } finally {
      zipFile.close()
    }

    result
  }

  /**
   * Compress all files under given directory into one zip file and drop it to the target directory
   *
   * @param sourceDir the directory where the zip file will be created
   * @param targetZipFile
   */
  def zip(sourceDir: File, targetZipFile: File): Unit = {
    if (!sourceDir.exists() || !sourceDir.isDirectory) {
      return
    }
    var fos: FileOutputStream = null
    var zos: ZipArchiveOutputStream = null
    try {
      fos = new FileOutputStream(targetZipFile)
      zos = new ZipArchiveOutputStream(fos)
      zipDir(sourceDir, sourceDir, zos)
    } catch {
      case e: Exception => throw e
    } finally {
      IOUtils.closeQuietly(zos)
      IOUtils.closeQuietly(fos)
    }
  }

  private def getUnixPermissionCode(permissionSet: Set[PosixFilePermission]): Integer = {
    var number = 0
    posixPermissionToInteger.foreach { entry =>
      if(permissionSet.contains(entry._1)) {
        number += entry._2
      }
    }
    number
  }

  private def getPosixPermissions(unixPermission: Integer): Set[PosixFilePermission] = {
    val permissions = Set[PosixFilePermission]()
    posixPermissionToInteger.foreach { entry =>
      if((unixPermission & entry._2) > 0) {
        permissions += entry._1
      }
    }
    permissions
  }

  private def zipDir(rootDir: File, sourceDir: File, out: ZipArchiveOutputStream): Unit = {
    for (file <- sourceDir.listFiles()) {
      if (file.isDirectory()) {
        zipDir(rootDir, new File(sourceDir, file.getName()), out)
      } else {
        var in: FileInputStream = null
        try {
          val entry = new ZipArchiveEntry(file.getPath.substring(rootDir.getPath.length + 1))
          if(isPosix) {
            entry.setUnixMode(getUnixPermissionCode(Files.getPosixFilePermissions(Paths.get(file.getPath)).asScala))
          }
          out.putArchiveEntry(entry)
          in = new FileInputStream(file)
          IOUtils.copy(in, out)
          out.closeArchiveEntry()
        }
        finally {
          IOUtils.closeQuietly(in)
        }
      }
    }
  }

  /**
   * Unzip a file to the given directory
   *
   * @param file file to be unzipped
   * @param targetDir target directory
   */
  def unzip(file: File, targetDir: File): Unit = {
    if (!targetDir.exists()) {
      targetDir.mkdir()
    }

    val zipFile = new ZipFile(file)
    try {
      val entries = zipFile.getEntries
      while (entries.hasMoreElements()) {
        val entry = entries.nextElement()
        val targetFile = new File(targetDir, entry.getName)

        if (targetFile.exists()) {
          println(s"Warning: target file/directory $targetFile already exists," +
            s" make sure this is expected, skip it for now.")
        } else {
          if (!targetFile.getParentFile.exists()) {
            targetFile.getParentFile.mkdir()
          }

          if (entry.isDirectory) {
            targetFile.mkdirs()
          } else {
            val input = zipFile.getInputStream(entry)
            val output = new FileOutputStream(targetFile)
            IOUtils.copy(input, output)
            IOUtils.closeQuietly(input)
            IOUtils.closeQuietly(output)
            if(isPosix) {
              Files.setPosixFilePermissions(Paths.get(targetFile.getPath), getPosixPermissions(entry.getUnixMode).asJava)
            }
          }
        }
      }
    } catch {
      case e: Exception => println("Error: exception caught during uncompression." + e)
    }
    finally {
      zipFile.close()
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
      // scalastyle:off println
      println(s"Utils.exit() with status: $status, maxDelayMillis: $maxDelayMillis")
      // scalastyle:on println

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

}
