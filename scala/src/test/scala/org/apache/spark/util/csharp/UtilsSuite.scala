/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package org.apache.spark.util.csharp

import java.io._

import org.apache.commons.io.FileUtils
import org.apache.spark.csharp.SparkCLRFunSuite

import scala.collection.JavaConversions._

class UtilsSuite extends SparkCLRFunSuite {

  test("Zip&unzip files") {

    // create tmp dir
    val tmpDir = new File(System.getProperty("java.io.tmpdir"), "UtilsSuite_" + System.currentTimeMillis())

    tmpDir.mkdir()
    val str = "test string"
    val size = 10

    // create some files in the tmp dir
    for (i <- 1 to size) {
      val f = new File(tmpDir, i + ".txt")
      FileUtils.writeStringToFile(f, str + i)
    }

    val targetZipFile = new File(System.getProperty("java.io.tmpdir"), "UtilsSuite_Zip_" + System.currentTimeMillis() + ".zip")

    // Compress all files under tmpDir into a zip file
    Utils.zip(tmpDir, targetZipFile)

    val entries = Utils.listZipFileEntries(targetZipFile)

    assert(entries != null)
    assert(entries.size == size)

    entries.foreach(f => assert(f.matches("\\d+\\.txt")))

    val destDir = new File(System.getProperty("java.io.tmpdir"), "UtilsSuite_Unzip_" + System.currentTimeMillis())

    destDir.mkdir()

    Utils.unzip(targetZipFile, destDir)

    val unzippedFiles = FileUtils.listFiles(destDir, null, true)

    assert(unzippedFiles != null && unzippedFiles.size() == size)
    unzippedFiles.foreach(f => assert(f.getName.matches("\\d+\\.txt")))
    unzippedFiles.foreach(f => assert(FileUtils.readFileToString(f).startsWith(str)))

    FileUtils.deleteQuietly(tmpDir)
    FileUtils.deleteQuietly(destDir)
    FileUtils.deleteQuietly(targetZipFile)
  }

  test("Zip&unzip files with sub folders") {

    // create tmp dir
    val tmpDir = new File(System.getProperty("java.io.tmpdir"), "UtilsSuite_" + System.currentTimeMillis())

    tmpDir.mkdir()
    val str = "test string"
    val size = 10

    // create some files in the tmp dir
    generateFilesInDirectory(tmpDir, size)

    val subFolder1 = new File(tmpDir, "folder1");
    subFolder1.mkdir()
    generateFilesInDirectory(subFolder1, size)

    val subFolder2 = new File(subFolder1, "folder2");
    subFolder2.mkdir()
    generateFilesInDirectory(subFolder2, size)


    val targetZipFile = new File(System.getProperty("java.io.tmpdir"), "UtilsSuite_Zip_" + System.currentTimeMillis() + ".zip")

    // Compress all files under tmpDir into a zip file
    Utils.zip(tmpDir, targetZipFile)

    val entries = Utils.listZipFileEntries(targetZipFile)

    assert(entries != null)
    assert(entries.size == size * 3)

    entries.foreach(f => assert(f.matches(".*?\\d+\\.txt")))

    val destDir = new File(System.getProperty("java.io.tmpdir"), "UtilsSuite_Unzip_" + System.currentTimeMillis())

    destDir.mkdir()

    Utils.unzip(targetZipFile, destDir)

    assert(new File(destDir, "folder1").exists())
    assert(new File(destDir + File.separator + "folder1" + File.separator + "folder2").exists())

    val unzippedFiles = FileUtils.listFiles(destDir, null, true)

    assert(unzippedFiles != null && unzippedFiles.size() == size * 3)
    unzippedFiles.foreach(f => if (!f.isDirectory) assert(f.getName.matches("\\d+\\.txt")))
    unzippedFiles.foreach(f => if (!f.isDirectory) assert(FileUtils.readFileToString(f).startsWith(str)))

    FileUtils.deleteQuietly(tmpDir)
    FileUtils.deleteQuietly(destDir)
    FileUtils.deleteQuietly(targetZipFile)
  }

  def generateFilesInDirectory(directory: File, n: Int): Unit = {
    val str = "test string"
    for (i <- 1 to n) {
      val f = new File(directory, i + ".txt")
      FileUtils.writeStringToFile(f, str + i)
    }
  }
}
