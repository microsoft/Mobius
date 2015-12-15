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

  import SparkCLRSubmitArguments._

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

  test("handle case that option --remote-sparkclr-jar is not specified in client mode") {
    val driverDir = new File(this.getClass.getResource(this.getClass.getSimpleName + ".class").getPath).getParentFile.getPath
    val executableName = "Test.exe"
    val executableFile = new File(driverDir, executableName)
    val args = Array("arg1", "arg2")
    FileUtils.copyFile(new File(driverDir, "Test.txt"), executableFile)

    val clArgs = Array(
      "--name", "myApp",
      "--exe", executableName,
      driverDir
    ) ++ args

    val options = new SparkCLRSubmitArguments(clArgs, Map()).buildCmdOptions()

    options should include(driverDir + File.separator + "Test.exe")
    options should include("--name myApp")
    options should endWith(args.mkString(" "))

    FileUtils.deleteQuietly(executableFile)
  }

  test("handle case that option --remote-sparkclr-jar is not specified in standalone cluster mode") {
    val driverDir = "hdfs://path/to/driver.zip"
    val executableName = "Test.exe"
    val args = Array("arg1", "arg2")

    val clArgs = Array(
      "--name", "myApp",
      "--deploy-mode", "cluster",
      "--master", "spark://host:port",
      "--exe", executableName,
      driverDir
    ) ++ args

    testPrematureExit(clArgs, "No remote sparkclr jar found; please specify one with option --remote-sparkclr-jar")
  }

  test("handle a normal case in local mode") {
    val driverDir = new File(this.getClass.getResource(this.getClass.getSimpleName + ".class").getPath).getParentFile.getPath

    val executableName = "Test.exe"
    val executableFile = new File(driverDir, executableName)
    FileUtils.copyFile(new File(driverDir, "Test.txt"), executableFile)

    val args = Array("arg1", "arg2")
    val clArgs = Array(
      "--name", "myApp",
      "--exe", executableName,
      driverDir
    ) ++ args

    val options = new SparkCLRSubmitArguments(clArgs, Map()).buildCmdOptions()

    options should include(driverDir + File.separator + "Test.exe")
    options should include("--name myApp")
    options should endWith(args.mkString(" "))

    FileUtils.deleteQuietly(executableFile)
  }

  test("handle the case that env variable `SPARKCSV_JARS` is set") {
    val driverDir = new File(this.getClass.getResource(this.getClass.getSimpleName + ".class").getPath).getParentFile.getPath
    val executableName = "Test2.exe"
    val appName = "myApp"
    val executableFile = new File(driverDir, executableName)

    FileUtils.copyFile(new File(driverDir, "Test.txt"), executableFile)
    val sparkClrJarPath = File.createTempFile("sparkclr", ".jar")

    val clArgs = Array(
      "--name", appName,
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
    FileUtils.deleteQuietly(sparkClrJarPath)
  }

  test("handle the case that env variable `SPARKCSV_JARS` and `--jars` option are both set") {
    val driverDir = new File(this.getClass.getResource(this.getClass.getSimpleName + ".class").getPath).getParentFile.getPath
    val executableName = "Test2.exe"
    val executableFile = new File(driverDir, executableName)

    FileUtils.copyFile(new File(driverDir, "Test.txt"), executableFile)
    val sparkClrJarPath = File.createTempFile("sparkclr", ".jar")

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
    FileUtils.deleteQuietly(sparkClrJarPath)
  }

  test("handle the case that spark APP is submitted in standalone client mode") {
    val appName = "myApp"
    val master = "spark://host:port"
    val driverDir = new File(this.getClass.getResource(this.getClass.getSimpleName + ".class").getPath).getParentFile.getPath
    val executableName = "Test.exe"
    val executableFile = new File(driverDir, executableName)
    val args = Array("arg1", "arg2")
    FileUtils.copyFile(new File(driverDir, "Test.txt"), executableFile)

    val clArgs = Array(
      "--name", "myApp",
      "--master", master,
      "--exe", executableName,
      driverDir
    ) ++ args

    val options = new SparkCLRSubmitArguments(clArgs, Map()).buildCmdOptions()

    options should fullyMatch regex
      s" --name $appName" +
        s" --master $master" +
        s" --files .*zip" +
        s" --class $csharpRunnerClass" +
        s" .*" +
        s" .* .*$executableName" +
        " " + args.mkString(" ")

    FileUtils.deleteQuietly(executableFile)
  }

  test("handle the case that spark APP is submitted in standalone client mode with --jars and --files options") {
    val appName = "myApp"
    val master = "spark://host:port"
    val driverDir = new File(this.getClass.getResource(this.getClass.getSimpleName + ".class").getPath).getParentFile.getPath
    val executableName = "Test.exe"
    val executableFile = new File(driverDir, executableName)
    val files = "file1"
    val jars = "1.jar,2.jar"
    val args = Array("arg1", "arg2")
    FileUtils.copyFile(new File(driverDir, "Test.txt"), executableFile)

    val clArgs = Array(
      "--name", appName,
      "--master", master,
      "--exe", executableName,
      "--jars", jars,
      "--files", files,
      driverDir
    ) ++ args

    val options = new SparkCLRSubmitArguments(clArgs, Map()).buildCmdOptions()

    options should fullyMatch regex
      s" --name $appName" +
        s" --master $master" +
        s" --jars $jars" +
        s" --files .*$files,.*zip" +
        s" --class $csharpRunnerClass" +
        s" .*" +
        s" .* .*$executableName" +
        " " + args.mkString(" ")

    FileUtils.deleteQuietly(executableFile)
  }

  test("handle the case that spark APP is submitted in standalone cluster mode") {
    val remoteDriverPath = "hdfs://host:port/path/to/driver.zip"
    val executableName = "Samples.exe"
    val appName = "myApp"
    val deployMode = "cluster"
    val master = "spark://host:port"
    val remoteSparkClrJarPath = "hdfs://path/to/sparkclr.jar"
    val args = Array("arg1", "arg2")

    val clArgs = Array(
      "--name", appName,
      "--deploy-mode", deployMode,
      "--remote-sparkclr-jar", remoteSparkClrJarPath,
      "--master", master,
      "--exe", executableName,
      remoteDriverPath
    ) ++ args

    val options = new SparkCLRSubmitArguments(clArgs, Map()).buildCmdOptions()

    options should fullyMatch regex
      s" --name $appName" +
        s" --deploy-mode $deployMode" +
        s" --master $master" +
        s" --files $remoteDriverPath" +
        s" --class $csharpRunnerClass" +
        s" $remoteSparkClrJarPath" +
        s" $remoteDriverPath" +
        s" $executableName" +
        " " +
        args.mkString(" ")
  }

  test("handle the case that spark APP is submitted in standalone cluster mode without `--remote-sparkclr-jar` option") {
    val remoteDriverPath = "hdfs://host:port/path/to/driver.zip"
    val executableName = "Samples.exe"
    val appName = "myApp"
    val deployMode = "cluster"
    val master = "spark://host:port"
    val args = Array("arg1", "arg2")

    val clArgs = Array(
      "--name", appName,
      "--deploy-mode", deployMode,
      "--master", master,
      "--exe", executableName,
      remoteDriverPath
    ) ++ args

    testPrematureExit(clArgs, s"No remote sparkclr jar found; please specify one with option --remote-sparkclr-jar")
  }

  test("handle the case that spark APP is submitted in standalone cluster mode with " +
    "--files and --jars option") {
    val remoteDriverPath = "hdfs://host:port/path/to/driver.zip"
    val executableName = "Samples.exe"
    val appName = "myApp"
    val deployMode = "cluster"
    val master = "spark://host:port"
    val files = "hdfs://path/to/file1,hdfs://path/to/file2"
    val jars = "hdfs://path/to/1.jar,hdfs://path/to/2.jar"
    val remoteSparkClrJarPath = "hdfs://path/to/sparkclr.jar"
    val args = Array("arg1", "arg2")

    val clArgs = Array(
      "--name", appName,
      "--deploy-mode", deployMode,
      "--master", master,
      "--files", files,
      "--remote-sparkclr-jar", remoteSparkClrJarPath,
      "--jars", jars,
      "--exe", executableName,
      remoteDriverPath
    ) ++ args

    val options = new SparkCLRSubmitArguments(clArgs, Map()).buildCmdOptions()

    options should fullyMatch regex
      s" --name $appName" +
        s" --deploy-mode $deployMode" +
        s" --master $master" +
        s" --jars $jars" +
        s" --files $files,$remoteDriverPath" +
        s" --class $csharpRunnerClass $remoteSparkClrJarPath" +
        s" $remoteDriverPath" +
        s" $executableName" +
        " " +
        args.mkString(" ")
  }

  test("handle the case that app name is not specified") {
    val remoteDriverPath = "hdfs://host:port/path/to/driver.zip"
    val executableName = "Samples.exe"
    val deployMode = "cluster"
    val master = "spark://host:port"
    val files = "hdfs://path/to/file1,hdfs://path/to/file2"
    val jars = "hdfs://path/to/1.jar,hdfs://path/to/2.jar"
    val remoteSparkClrJarPath = "hdfs://path/to/sparkclr.jar"
    val args = Array("arg1", "arg2")

    val clArgs = Array(
      "--deploy-mode", deployMode,
      "--master", master,
      "--files", files,
      "--jars", jars,
      "--remote-sparkclr-jar", remoteSparkClrJarPath,
      "--exe", executableName,
      remoteDriverPath
    ) ++ args

    val options = new SparkCLRSubmitArguments(clArgs, Map()).buildCmdOptions()

    val expectedAppName = executableName.stripSuffix(".exe")
    options should fullyMatch regex
      s" --deploy-mode $deployMode" +
        s" --master $master" +
        s" --name $expectedAppName" +
        s" --jars $jars" +
        s" --files $files,$remoteDriverPath" +
        s" --class $csharpRunnerClass $remoteSparkClrJarPath" +
        s" $remoteDriverPath" +
        s" $executableName" +
        " " +
        args.mkString(" ")
  }
}
