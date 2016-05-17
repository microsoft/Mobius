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
    val tmpDir = new File(System.getProperty("java.io.tmpdir"), s"UtilsSuite_${System.currentTimeMillis()}")
    tmpDir.mkdir()
    // create some files in the tmp dir
    generateFilesInDirectory(tmpDir, 10)

    val targetZipFile = new File(System.getProperty("java.io.tmpdir"), s"UtilsSuite_${System.currentTimeMillis()}.zip")
    // Compress all files under tmpDir into a zip file
    Utils.zip(tmpDir, targetZipFile)

    checkZipFile(targetZipFile, tmpDir)

    val destDir = new File(System.getProperty("java.io.tmpdir"), s"UtilsSuite_${System.currentTimeMillis()}")
    destDir.mkdir()
    Utils.unzip(targetZipFile, destDir)

    checkUnzippedFiles(tmpDir, destDir)

    FileUtils.deleteQuietly(tmpDir)
    FileUtils.deleteQuietly(destDir)
    FileUtils.deleteQuietly(targetZipFile)
  }

  test("Zip&unzip files with sub folders") {
    // create tmp dir
    val tmpDir = new File(System.getProperty("java.io.tmpdir"), s"UtilsSuite_${System.currentTimeMillis()}")
    tmpDir.mkdir()
    // create some files in the tmp dir
    generateFilesInDirectory(tmpDir, 10)

    val subFolder1 = new File(tmpDir, "folder1");
    subFolder1.mkdir()
    generateFilesInDirectory(subFolder1, 10)

    val subFolder2 = new File(subFolder1, "folder2");
    subFolder2.mkdir()
    generateFilesInDirectory(subFolder2, 10)

    val targetZipFile = new File(System.getProperty("java.io.tmpdir"), s"UtilsSuite_${System.currentTimeMillis()}.zip")
    // Compress all files under tmpDir into a zip file
    Utils.zip(tmpDir, targetZipFile)

    checkZipFile(targetZipFile, tmpDir)

    val destDir = new File(System.getProperty("java.io.tmpdir"), s"UtilsSuite_${System.currentTimeMillis()}")
    destDir.mkdir()
    Utils.unzip(targetZipFile, destDir)

    checkUnzippedFiles(tmpDir, destDir)

    FileUtils.deleteQuietly(tmpDir)
    FileUtils.deleteQuietly(destDir)
    FileUtils.deleteQuietly(targetZipFile)
  }

  test("Unzip file to a directory where some files already exist.") {
    // create tmp dir
    val tmpDir = new File(System.getProperty("java.io.tmpdir"), s"UtilsSuite_${System.currentTimeMillis()}")
    tmpDir.mkdir()
    val str = "test string"
    val size = 10
    // create some files in the tmp dir
    generateFilesInDirectory(tmpDir, size)

    val targetZipFile = new File(System.getProperty("java.io.tmpdir"), s"UtilsSuite_${System.currentTimeMillis()}.zip")

    // Compress all files under tmpDir into a zip file
    Utils.zip(tmpDir, targetZipFile)

    checkZipFile(targetZipFile, tmpDir)

    val destDir = new File(System.getProperty("java.io.tmpdir"), s"UtilsSuite_${System.currentTimeMillis()}")
    destDir.mkdir()

    // create some files which names can also be found in the zip file.
    val content = "Not replaced."
    FileUtils.writeStringToFile(new File(destDir, 1 + ".txt"), content)

    Utils.unzip(targetZipFile, destDir)

    val unzippedFiles = FileUtils.listFiles(destDir, null, true)

    assert(unzippedFiles != null && unzippedFiles.size() == size)
    unzippedFiles.foreach(f => assert(f.getName.matches("\\d+\\.txt")))
    unzippedFiles.filter(f => f.getName.split("\\.")(0) != "1")
      .foreach(f => assert(FileUtils.readFileToString(f).startsWith(str)))
    unzippedFiles.filter(f => f.getName.split("\\.")(0) == "1")
      .foreach(f => assert(FileUtils.readFileToString(f).startsWith(content)))

    FileUtils.deleteQuietly(tmpDir)
    FileUtils.deleteQuietly(destDir)
    FileUtils.deleteQuietly(targetZipFile)
  }

  private def generateFilesInDirectory(directory: File, n: Int): Unit = {
    val str = "test string"
    for (i <- 1 to n) {
      val f = new File(directory, i + ".txt")
      FileUtils.writeStringToFile(f, str + i)
    }
  }

  private def checkZipFile(zipFile: File, dir: File): Unit = {
    val paths1 = Utils.listZipFileEntries(zipFile)
    val base = dir.toPath()
    val paths2 = FileUtils.listFiles(dir, null, true).map(f => base.relativize(f.toPath).toString)

    assert(paths1.size() === paths2.size())
    paths1.zip(paths2).foreach { case (p1, p2) => assert(p1 === p2) }
  }

  private def checkUnzippedFiles(dir1: File, dir2: File): Unit = {
    val base1 = dir1.toPath
    val base2 = dir2.toPath
    val files1 = FileUtils.listFiles(dir1, null, true)
    val files2 = FileUtils.listFiles(dir2, null, true)

    assert(files1.size() === files2.size())
    files1.zip(files2).foreach { case (f1, f2) =>
      assert(base1.relativize(f1.toPath) === base2.relativize(f2.toPath))
      assert(FileUtils.contentEquals(f1, f2))
    }
  }
}
