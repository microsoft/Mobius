// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package org.apache.spark.util.csharp

import java.io._
import java.util.zip.{ZipEntry, ZipOutputStream, ZipFile}
import util.control.Breaks._

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
   * @param directory the directory where the zip file will be created
   * @param targetZipFile
   */
  def zip(directory: File, targetZipFile: File): Unit = {
    if (!directory.exists() || !directory.isDirectory) {
      return
    }

    try {

      val fos = new FileOutputStream(targetZipFile)
      val zos = new ZipOutputStream(fos)

      for (f <- directory.list()) {

        val ze = new ZipEntry(f)
        zos.putNextEntry(ze)
        val in = new FileInputStream(new File(directory, f))

        copy(in, zos)

        in.close()
        zos.closeEntry()
      }

      zos.close()
      fos.close()
    } catch {
      case e: Exception => throw e
    }
  }

  /**
   * Unzip a file to the given directory
   * @param file file to be unzipped
   * @param targetDir target directory
   */
  def unzip(file: File, targetDir: File): Unit = {

    val zipFile = new ZipFile(file)

    try {

      val entries = zipFile.entries()

      while (entries.hasMoreElements()) {

        breakable {
          val entry = entries.nextElement()

          val targetFile = new File(targetDir, entry.getName())

          if (targetFile.exists()) {
            break
          }

          if (entry.isDirectory()) {
            targetFile.mkdirs()

          } else {
            val input = zipFile.getInputStream(entry);
            try {
              val output = new FileOutputStream(targetFile);
              try {
                copy(input, output)
              } finally {
                output.close()
              }
            } finally {
              input.close()
            }
          }
        }
      }
    } finally {
      zipFile.close()
    }

  }

  private def copy(input: InputStream, output: OutputStream): Unit = {
    val buffer = new Array[Byte](4096)
    var size: Int = -1
    while (true) {
      size = input.read(buffer)
      if (size == -1) return
      output.write(buffer, 0, size)
    }
  }

}
