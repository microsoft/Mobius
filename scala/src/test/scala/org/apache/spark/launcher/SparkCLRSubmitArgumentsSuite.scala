// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package org.apache.spark.launcher

import java.io.{File, OutputStream, PrintStream}

import org.apache.commons.io.FileUtils
import org.apache.spark.csharp.SparkCLRFunSuite
import org.scalatest.Matchers
import org.scalatest.concurrent.Timeouts

import scala.collection.mutable.ArrayBuffer

/**
 * Parse, validate and rebuild command line options from sparkclr-submit.cmd and submit them to spark-submit.cmd
 */
class SparkCLRSubmitArgumentsSuite extends SparkCLRFunSuite with Matchers with Timeouts {

  private val noOpOutputStream = new OutputStream {
    def write(b: Int) = {}
  }

  /** Simple PrintStream that reads data into a buffer */
  private class BufferPrintStream extends PrintStream(noOpOutputStream) {
    var lineBuffer = ArrayBuffer[String]()

    override def println(line: String) {
      lineBuffer += line
    }
  }

  /** Returns true if the script exits and the given search string is printed. */
  private def testPrematureExit(input: Array[String], searchString: String) = {

    val printStream = new BufferPrintStream()
    SparkCLRSubmitArguments.printStream = printStream

    @volatile var exitedCleanly = false
    SparkCLRSubmitArguments.exitFn = (exitCode: Int) => exitedCleanly = true

    val thread = new Thread {
      override def run() = try {
        SparkCLRSubmitArguments.main(input)
      } catch {
        // If exceptions occur after the "exit" has happened, fine to ignore them.
        // These represent code paths not reachable during normal execution.
        case e: Exception => if (!exitedCleanly) throw e
      }
    }
    thread.start()
    thread.join()
    val joined = printStream.lineBuffer.mkString("\n")
    if (!joined.contains(searchString)) {
      fail(s"Search string '$searchString' not found in $joined")
    }
  }

  test("prints usage on empty input") {
    testPrematureExit(Array[String](), "Usage: sparkclr-submit")
  }

  test("prints usage with only --help") {
    testPrematureExit(Array("--help"), "Usage: sparkclr-submit")
  }

  test("prints version with only --version") {
    testPrematureExit(Array("--version"), "Welcome to version")
  }

  test("handle option --exe is not specified") {

    val clArgs = Array(
      "--name", "myApp",
      "user_driver.zip",
      "some",
      "--weird", "args")

    testPrematureExit(clArgs, "No main executable found")
  }

  test("handle option --exe specified but does not end with .exe") {
    val clArgs = Array(
      "--name", "myApp",
      "--exe", "myApp",
      "user_driver.zip",
      "some",
      "--weird", "args")

    testPrematureExit(clArgs, "No main executable found")
  }

  test("handle no primary resource found") {
    val clArgs = Array(
      "--name", "myApp",
      "--exe", "myApp")

    testPrematureExit(clArgs, "No primary resource found")
  }

  test("handle a normal case") {
    val driverDir = new File(this.getClass.getResource(this.getClass.getSimpleName + ".class").getPath).getParentFile.getPath
    val executableName = "Test.exe"
    val executableFile = new File(driverDir, executableName)
    FileUtils.copyFile(new File(driverDir, "Test.txt"), executableFile)

    val clArgs = Array(
      "--name", "myApp",
      "--exe", executableName,
      driverDir,
      "arg1", "arg2"
    )

    val options = new SparkCLRSubmitArguments(clArgs, Map()).buildCmdOptions()

    options should include(driverDir + File.separator + "Test.exe")
    options should include("--name myApp")
    options should endWith("arg1 arg2")

    FileUtils.deleteQuietly(executableFile)
  }

  test("handle the case that env variable `SPARKCSV_JARS` is set") {
    val driverDir = new File(this.getClass.getResource(this.getClass.getSimpleName + ".class").getPath).getParentFile.getPath
    val executableName = "Test2.exe"
    val executableFile = new File(driverDir, executableName)

    FileUtils.copyFile(new File(driverDir, "Test.txt"), executableFile)

    val clArgs = Array(
      "--name", "myApp",
      "--exe", executableName,
      driverDir,
      "arg1", "arg2"
    )

    val options = new SparkCLRSubmitArguments(clArgs, Map(("SPARKCSV_JARS", "path/to/commons-csv-1.2.jar;path/to/spark-csv_2.10-1.2.0"))).buildCmdOptions()

    options should include(driverDir + File.separator + executableName)
    options should include("--jars path/to/commons-csv-1.2.jar,path/to/spark-csv_2.10-1.2.0")
    options should include("--name myApp")
    options should endWith("arg1 arg2")

    FileUtils.deleteQuietly(executableFile)
  }

  test("handle the case that env variable `SPARKCSV_JARS` and `--jars` option are both set") {
    val driverDir = new File(this.getClass.getResource(this.getClass.getSimpleName + ".class").getPath).getParentFile.getPath
    val executableName = "Test2.exe"
    val executableFile = new File(driverDir, executableName)

    FileUtils.copyFile(new File(driverDir, "Test.txt"), executableFile)

    val clArgs = Array(
      "--name", "myApp",
      "--jars", "path/to/user.jar",
      "--exe", executableName,
      driverDir,
      "arg1", "arg2"
    )

    val options = new SparkCLRSubmitArguments(clArgs, Map(("SPARKCSV_JARS", "path/to/commons-csv-1.2.jar;path/to/spark-csv_2.10-1.2.0"))).buildCmdOptions()

    options should include(driverDir + File.separator + executableName)
    options should include("--jars path/to/commons-csv-1.2.jar,path/to/spark-csv_2.10-1.2.0,path/to/user.jar")
    options should include("--name myApp")
    options should endWith("arg1 arg2")

    FileUtils.deleteQuietly(executableFile)
  }

}
