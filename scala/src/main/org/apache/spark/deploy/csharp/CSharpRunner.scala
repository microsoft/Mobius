// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package org.apache.spark.deploy.csharp

import java.io.File
import java.util.concurrent.{Semaphore, TimeUnit}

import org.apache.spark.api.csharp.CSharpBackend
import org.apache.spark.deploy.{SparkSubmitArguments, PythonRunner}
import org.apache.spark.util.{Utils, RedirectThread}
import org.apache.spark.util.csharp.{Utils => CSharpSparkUtils}

/**
 * Launched by sparkclr-submit.cmd. It launches CSharpBackend, gets its port number and launches C# process
 * passing the port number to it.
 * The runner implementation is mostly identical to RRunner with SparkCLR-specific customizations
 */
object CSharpRunner {
  def main(args: Array[String]): Unit = {
    //determines if CSharpBackend need to be run in debug mode
    //in debug mode this runner will not launch C# process
    var runInDebugMode = false

    if (args.length == 0) {
      throw new IllegalArgumentException("At least one argument is expected for CSharpRunner")
    }

    if (args.length == 1 && args(0).equalsIgnoreCase("debug")) {
      runInDebugMode = true
      println("Debug mode is set. CSharp executable will not be launched as a sub-process.")
    }

    var csharpExecutable = ""
    var otherArgs: Array[String] = null

    if (!runInDebugMode) {

      if(args(0).toLowerCase().endsWith(".zip")) {

        println("Unzipping driver!")
        CSharpSparkUtils.unzip(new File(args(0)), new File(args(0)).getParentFile)
        csharpExecutable = PythonRunner.formatPath(args(1)) //reusing windows-specific formatting in PythonRunner
        otherArgs = args.slice(2, args.length)

      }else if(new File(args(0)).isDirectory){
        // In local mode, there will no zip file generated if given a directory, skip uncompression in this case
        csharpExecutable = PythonRunner.formatPath(args(1)) //reusing windows-specific formatting in PythonRunner
        otherArgs = args.slice(2, args.length)

      }else {

        csharpExecutable = PythonRunner.formatPath(args(0))
        otherArgs = args.slice(1, args.length)

      }
    }

    var processParameters = new java.util.ArrayList[String]()
    processParameters.add(csharpExecutable)
    otherArgs.foreach( arg => processParameters.add(arg) )

    println("Starting CSharpBackend!")
    // Time to wait for CSharpBackend to initialize in seconds

    val backendTimeout = sys.env.getOrElse("CSHARPBACKEND_TIMEOUT", "120").toInt

    // Launch a SparkCLR backend server for the C# process to connect to; this will let it see our
    // Java system properties etc.
    val csharpBackend = new CSharpBackend()
    @volatile var csharpBackendPortNumber = 0
    val initialized = new Semaphore(0)
    val csharpBackendThread = new Thread("CSharpBackend") {
      override def run() {
        csharpBackendPortNumber = csharpBackend.init()
        println("Port number used by CSharpBackend is " + csharpBackendPortNumber) //TODO - send to logger also
        initialized.release()
        csharpBackend.run()
      }
    }

    csharpBackendThread.start()

    if (initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)) {
      if (!runInDebugMode) {
        val returnCode = try {
          val builder = new ProcessBuilder(processParameters)
          val env = builder.environment()
          env.put("CSHARPBACKEND_PORT", csharpBackendPortNumber.toString)

          for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
            env.put(key, value)
            println("adding key=" + key + " and value=" + value + " to environment")
          }
          builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
          val process = builder.start()

          new RedirectThread(process.getInputStream, System.out, "redirect CSharp output").start()

          process.waitFor()
        } catch {
          case e: Exception => println(e.getMessage + "\n" + e.getStackTrace)
        }
        finally {
          closeBackend(csharpBackend)
        }
        println("Return CSharpBackend code " + returnCode)
        System.exit(returnCode.toString.toInt)
      } else {
        println("***************************************************")
        println("* Backend running debug mode. Press enter to exit *")
        println("***************************************************")
        Console.readLine()
        closeBackend(csharpBackend)
        System.exit(0)
      }
    } else {
      // scalastyle:off println
      println("CSharpBackend did not initialize in " + backendTimeout + " seconds")
      // scalastyle:on println
      System.exit(-1)
    }
  }

  def closeBackend(csharpBackend: CSharpBackend): Unit = {
    println("closing CSharpBackend")
    csharpBackend.close()
  }
}
