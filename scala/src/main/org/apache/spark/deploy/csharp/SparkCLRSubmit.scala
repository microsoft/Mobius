// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package org.apache.spark.deploy.csharp

import java.io.File
import java.lang.reflect.{InvocationTargetException, UndeclaredThrowableException, Modifier}
import java.net.URL
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.deploy.rest.SubmitRestConnectionException
import org.apache.spark.deploy.{SparkSubmit, SparkSubmitAction, SparkSubmitArguments}
import org.apache.spark.util.{ChildFirstURLClassLoader, Utils, MutableURLClassLoader}

import scala.collection.mutable.Map

/**
 * Used to submit, kill or request status of SparkCLR applications.
 * The implementation is a simpler version of SparkSubmit and
 * "handles setting up the classpath with relevant Spark dependencies and provides
 * a layer over the different cluster managers and deploy modes that Spark supports".
 */
// Since SparkCLR is a package to Spark and not a part of spark-core it reimplements
// selected parts from SparkSubmit with SparkCLR customizations
object SparkCLRSubmit {

  def main(args: Array[String]): Unit = {
    val appArgs = new SparkSubmitArguments(args)

    appArgs.action match {
      case SparkSubmitAction.SUBMIT => submit(appArgs)
      //case SparkSubmitAction.KILL => kill(appArgs)
      //case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
    }
  }

  def submit(args: SparkSubmitArguments): Unit = {
    val (childArgs, childClasspath, sysProps, childMainClass) = SparkSubmit.prepareSubmitEnvironment(args)

    def doRunMain(): Unit = {
      if (args.proxyUser != null) {
        val proxyUser = UserGroupInformation.createProxyUser(args.proxyUser,
          UserGroupInformation.getCurrentUser())
        try {
          proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
            }
          })
        } catch {
          case e: Exception =>
            // Hadoop's AuthorizationException suppresses the exception's stack trace, which
            // makes the message printed to the output by the JVM not very helpful. Instead,
            // detect exceptions with empty stack traces here, and treat them differently.
            if (e.getStackTrace().length == 0) {
              // scalastyle:off println
              //printStream.println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
              // scalastyle:on println
              //exitFn(1)
            } else {
              throw e
            }
        }
      } else {
        runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
      }
    }

    // In standalone cluster mode, there are two submission gateways:
    //   (1) The traditional Akka gateway using o.a.s.deploy.Client as a wrapper
    //   (2) The new REST-based gateway introduced in Spark 1.3
    // The latter is the default behavior as of Spark 1.3, but Spark submit will fail over
    // to use the legacy gateway if the master endpoint turns out to be not a REST server.
    if (args.isStandaloneCluster && args.useRest) {
      try {
        // scalastyle:off println
        //printStream.println("Running Spark using the REST application submission protocol.")
        // scalastyle:on println
        doRunMain()
      } catch {
        // Fail over to use the legacy submission gateway
        case e: SubmitRestConnectionException =>
          //printWarning(s"Master endpoint ${args.master} was not a REST server. " +
            //"Falling back to legacy submission gateway instead.")
          args.useRest = false
          submit(args)
      }
      // In all other modes, just run the main class as prepared
    } else {
      doRunMain()
    }
  }
  private def runMain(
                       childArgs: Seq[String],
                       childClasspath: Seq[String],
                       sysProps: Map[String, String],
                       childMainClass: String,
                       verbose: Boolean): Unit = {
    val loader =
      if (sysProps.getOrElse("spark.driver.userClassPathFirst", "false").toBoolean) {
        new ChildFirstURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      } else {
        new MutableURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      }
    Thread.currentThread.setContextClassLoader(loader)

    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }

    for ((key, value) <- sysProps) {
      println("key=" + key + ", value=" + value)
      System.setProperty(key, value)
    }

    //override spark.app.name with C# driver name to differentiate apps in SparkUI
    //without override, app name is set to org.apache.spark.deploy.csharp.SparkCLRSubmit
    var appName = new File(childArgs(0)).getName
    println("overriding spark.app.name to " + appName)
    System.setProperty("spark.app.name", appName)

    var mainClass: Class[_] = null

    try {
      mainClass = Utils.classForName("org.apache.spark.deploy.csharp.CSharpRunner")
    } catch {
      case e: ClassNotFoundException =>
/*        e.printStackTrace(printStream)
        if (childMainClass.contains("thriftserver")) {
          // scalastyle:off println
          printStream.println(s"Failed to load main class $childMainClass.")
          printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
          // scalastyle:on println
        }
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)*/
    }

    val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method in the given main class must be static")
    }

    def findCause(t: Throwable): Throwable = t match {
      case e: UndeclaredThrowableException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: InvocationTargetException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: Throwable =>
        e
    }

    try {
      println("Invoking Main method with following args")
      childArgs.foreach( s => println(s))
      mainMethod.invoke(null, childArgs.toArray)

    } catch {
      case t: Throwable =>
        throw findCause(t)
    }
  }

  private def addJarToClasspath(localJar: String, loader: MutableURLClassLoader) {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          //printWarning(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        //printWarning(s"Skip remote jar $uri.")
    }
  }
}
