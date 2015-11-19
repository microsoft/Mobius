// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package org.apache.spark.util.csharp

import java.io._
import java.util.zip.{ZipEntry, ZipOutputStream, ZipFile}
import org.apache.commons.io.IOUtils

import scala.collection.mutable.ArrayBuffer

/**
 * Utility methods used by SparkCLR.
 */
object Utils {

  /**
   * List all entries of a zip file
   * @param file zip file
   */
  def listZipFileEntries(file: File): Seq[String] = {

    val result = ArrayBuffer[String]()
    val zipFile = new ZipFile(file)

    try {
      val entries = zipFile.entries()
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
   * @param sourceDir the directory where the zip file will be created
   * @param targetZipFile
   */
  def zip(sourceDir: File, targetZipFile: File): Unit = {
    if (!sourceDir.exists() || !sourceDir.isDirectory) {
      return
    }

    var fos: FileOutputStream = null
    var zos: ZipOutputStream = null
    try {
      fos = new FileOutputStream(targetZipFile)
      zos = new ZipOutputStream(fos)
      zipDir(sourceDir, sourceDir, zos)
    } catch {
      case e: Exception => throw e
    } finally {
      IOUtils.closeQuietly(zos)
      IOUtils.closeQuietly(fos)
    }
  }

  private def zipDir(rootDir: File, sourceDir: File, out: ZipOutputStream): Unit = {
    for (file <- sourceDir.listFiles()) {
      if (file.isDirectory()) {
        zipDir(rootDir, new File(sourceDir, file.getName()), out)
      } else {
        var in: FileInputStream = null
        try {
          val entry = new ZipEntry(file.getPath.substring(rootDir.getPath.length + 1))
          out.putNextEntry(entry)
          in = new FileInputStream(file)
          IOUtils.copy(in, out)
        }
        finally {
          IOUtils.closeQuietly(in)
        }
      }
    }
  }

  /**
   * Unzip a file to the given directory
   * @param file file to be unzipped
   * @param targetDir target directory
   */
  def unzip(file: File, targetDir: File): Unit = {
    if(!targetDir.exists()){
      targetDir.mkdir()
    }

    val zipFile = new ZipFile(file)
    try {
      val entries = zipFile.entries()
      while (entries.hasMoreElements()) {
        val entry = entries.nextElement()
        val targetFile = new File(targetDir, entry.getName)
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
        }
      }
    } finally {
      zipFile.close()
    }
  }
}
