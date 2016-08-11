/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.deploy.csharp

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.{Semaphore, TimeUnit}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SecurityManager
import org.apache.spark.api.csharp.CSharpBackend
import org.apache.spark.deploy.{PythonRunner, SparkHadoopUtil, SparkSubmitArguments}
import org.apache.spark.util.{RedirectThread, Utils}
import org.apache.spark.util.csharp.{Utils => CSharpSparkUtils}

/**
 * Launched by sparkclr-submit.cmd. It launches CSharpBackend,
 * gets its port number and launches C# process passing the port number to it.
 * The runner implementation is mostly identical to RRunner with SparkCLR-specific customizations.
 */
// scalastyle:off println
object CSharpRunner {
  val MOBIUS_DEBUG_PORT = 5567

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      throw new IllegalArgumentException("At least one argument is expected for CSharpRunner")
    }

    val runnerSettings = initializeCSharpRunnerSettings(args)

    // determines if CSharpBackend need to be run in debug mode
    // in debug mode this runner will not launch C# process
    var runInDebugMode = runnerSettings._1
    @volatile var csharpBackendPortNumber = runnerSettings._2
    var csharpExecutable = ""
    var otherArgs: Array[String] = null

    if (!runInDebugMode) {
      if (args(0).toLowerCase.endsWith(".zip")) {
        var zipFileName = args(0)
        val driverDir = new File("").getAbsoluteFile

        if (zipFileName.toLowerCase.startsWith("hdfs://")) {
          // standalone cluster mode, need to download the zip file from hdfs.
          zipFileName = downloadDriverFile(zipFileName, driverDir.getAbsolutePath).getName
        }

        println(s"[CSharpRunner.main] Unzipping driver $zipFileName in $driverDir")
        CSharpSparkUtils.unzip(new File(zipFileName), driverDir)
        // reusing windows-specific formatting in PythonRunner
        csharpExecutable = PythonRunner.formatPath(args(1))
        otherArgs = args.slice(2, args.length)
      } else if (new File(args(0)).isDirectory) {
        // In local mode, there will no zip file generated if given a directory,
        // skip uncompression in this case
        // reusing windows-specific formatting in PythonRunner
        csharpExecutable = PythonRunner.formatPath(args(1))
        otherArgs = args.slice(2, args.length)
      } else {
        csharpExecutable = PythonRunner.formatPath(args(0))
        otherArgs = args.slice(1, args.length)
      }
    } else {
        otherArgs = args.slice(1, args.length)
    }

    var processParameters = new java.util.ArrayList[String]
    processParameters.add(formatPath(csharpExecutable))
    otherArgs.foreach( arg => processParameters.add(arg) )

    println("[CSharpRunner.main] Starting CSharpBackend!")
    // Time to wait for CSharpBackend to initialize in seconds

    val backendTimeout = sys.env.getOrElse("CSHARPBACKEND_TIMEOUT", "120").toInt

    // Launch a SparkCLR backend server for the C# process to connect to; this will let it see our
    // Java system properties etc.
    val csharpBackend = new CSharpBackend()
    val initialized = new Semaphore(0)
    val csharpBackendThread = new Thread("CSharpBackend") {
      override def run() {
        // need to get back csharpBackendPortNumber because if the value passed to init is 0
        // the port number is dynamically assigned in the backend
        csharpBackendPortNumber = csharpBackend.init(csharpBackendPortNumber)
        println("[CSharpRunner.main] Port number used by CSharpBackend is "
          + csharpBackendPortNumber) // TODO - send to logger also
        initialized.release()
        csharpBackend.run()
      }
    }

    csharpBackendThread.start()

    if (initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)) {
      if (!runInDebugMode) {
        var returnCode = -1
        try {
          val builder = new ProcessBuilder(processParameters)
          val env = builder.environment()
          env.put("CSHARPBACKEND_PORT", csharpBackendPortNumber.toString)

          for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
            env.put(key, value)
            println("[CSharpRunner.main] adding key=" + key
              + " and value=" + value + " to environment")
          }
          builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
          val process = builder.start()

          // Redirect stdin of JVM process to stdin of C# process
          new RedirectThread(System.in, process.getOutputStream, "redirect JVM input").start()
          // Redirect stdout and stderr of C# process
          new RedirectThread(process.getInputStream, System.out, "redirect CSharp stdout").start()
          new RedirectThread(process.getErrorStream, System.out, "redirect CSharp stderr").start()

          returnCode = process.waitFor()
          closeBackend(csharpBackend)
        } catch {
          case t: Throwable =>
            println("[CSharpRunner.main]" + t.getMessage + "\n" + t.getStackTrace)
        }

        println("[CSharpRunner.main] Return CSharpBackend code " + returnCode)
        CSharpSparkUtils.exit(returnCode)
      } else {
        println("***********************************************************************")
        println("* [CSharpRunner.main] Backend running debug mode. Press enter to exit *")
        println("***********************************************************************")
        Console.readLine()
        closeBackend(csharpBackend)
        CSharpSparkUtils.exit(0)
      }
    } else {
      println("[CSharpRunner.main] CSharpBackend did not initialize in "
        + backendTimeout + " seconds")
      CSharpSparkUtils.exit(-1)
    }
  }

  // when executing in YARN cluster mode, the name of the
  // executable is single-part (just the exe name)
  // this method will add "." to it
  def formatPath(csharpExecutable: String): String = {
    var formattedCSharpExecutable = csharpExecutable
    var path = Paths.get(csharpExecutable)
    if (!path.isAbsolute && path.getNameCount == 1) {
      formattedCSharpExecutable = Paths.get(".", path.toString).toString
    }
    formattedCSharpExecutable
  }

  /**
   * Download HDFS file into the supplied directory and return its local path.
   * Will throw an exception if there are errors during downloading.
   */
  private def downloadDriverFile(hdfsFilePath: String, driverDir: String): File = {
    val sparkConf = new SparkConf()
    val filePath = new Path(hdfsFilePath)

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val jarFileName = filePath.getName
    val localFile = new File(driverDir, jarFileName)

    if (!localFile.exists()) { // May already exist if running multiple workers on one node
      println(s"Copying user file $filePath to $driverDir")
      Utils.fetchFile(
        hdfsFilePath,
        new File(driverDir),
        sparkConf,
        new SecurityManager(sparkConf),
        hadoopConf,
        System.currentTimeMillis(),
        useCache = false)
    }

    if (!localFile.exists()) { // Verify copy succeeded
      throw new Exception(s"Did not see expected $jarFileName in $driverDir")
    }

    localFile
  }

  def closeBackend(csharpBackend: CSharpBackend): Unit = {
    println("[CSharpRunner.main] closing CSharpBackend")
    csharpBackend.close()
  }

  def initializeCSharpRunnerSettings(args: Array[String]): (Boolean, Int) = {
    val runInDebugMode = (args.length == 1 || args.length == 2) && args(0).equalsIgnoreCase("debug")
    var portNumber = 0
    if (runInDebugMode) {
      if (args.length == 1) {
        portNumber = MOBIUS_DEBUG_PORT
      } else if (args.length == 2 ) {
        portNumber = Integer.parseInt(args(1))
      }
    }

    (runInDebugMode, portNumber)
  }
}
// scalastyle:on println
