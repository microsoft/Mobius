// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package org.apache.spark.launcher

import java.io.{PrintStream, File}
import java.util.{List => JList}

import org.apache.spark._
import org.apache.spark.deploy.SparkSubmit
import org.apache.spark.deploy.csharp.CSharpRunner
import org.apache.spark.util.Utils
import org.apache.spark.util.csharp.{Utils => CSharpUtils}

import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}

object SparkCLRSubmitArguments {

  val csharpRunnerClass: String = "org.apache.spark.deploy.csharp.CSharpRunner"
  var exitFn: Int => Unit = (exitCode: Int) => CSharpUtils.exit(exitCode)
  var printStream: PrintStream = System.err

  def main(args: Array[String]): Unit = {
    val submitArguments = new SparkCLRSubmitArguments(args, sys.env, exitFn, printStream)
    System.out.println(submitArguments.buildCmdOptions())
  }
}

/**
 * Parses and encapsulates arguments from the SparkCLR-submit script.
 *
 * Current implementation needs to access "opts" attributes from SparkSubmitOptionParser, and the "opts" can only be accessed in the same package.
 * This is the reason why this class is put into current package.
 */
class SparkCLRSubmitArguments(args: Seq[String], env: Map[String, String], exitFn: Int => Unit, printStream: PrintStream) extends SparkSubmitArgumentsParser {

  import SparkCLRSubmitArguments._

  val MAIN_EXECUTABLE: String = "--exe"

  // remote file path of spark-clr*.jar on HDFS, this option is only required when deploy mode
  // is standalone cluster.
  val REMOTE_SPARKCLR_JAR_PATH: String = "--remote-sparkclr-jar"

  var mainExecutable: String = null

  var appName: String = null

  var master: String = null

  var deployMode: String = "client"

  var files: String = null

  var jars: String = env.getOrElse("SPARKCSV_JARS", "").replace(";", ",")

  var primaryResource: String = null

  var propertiesFile: String = null

  val sparkProperties: HashMap[String, String] = new HashMap[String, String]()

  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()

  var sparkCLRJarPath: String = new File(CSharpRunner.getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getPath

  var remoteSparkCLRJarPath: String = null

  var cmd: String = ""

  var options: Array[Array[String]] = null

  def this(args: Seq[String], env: Map[String, String]) {
    this(args, env, (exitCode: Int) => CSharpUtils.exit(exitCode), System.err)
  }

  private def printError(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
  }

  private def printErrorAndExit(str: String): Unit = {
    printError(str)
    exitFn(1)
  }

  /**
   * Parse a list of spark-submit command line options.
   * See SparkSubmitArguments.scala for a more formal description of available options.
   *
   * @throws IllegalArgumentException If an error is found during parsing.
   */
  protected def parse(args: List[String]): Unit = {
    val eqSeparatedOpt = "(--[^=]+)=(.+)".r

    def findCliOption(name: String, available: Array[Array[String]]): Option[Array[String]] = {
      available.find(candidates => !candidates.find(_ == name).isEmpty)
    }

    var curIdx = -1
    breakable {
      args.zipWithIndex.foreach {

        case (e, idx) if (idx <= curIdx) => // skip

        case (e, idx) => {
          curIdx = idx
          var (arg, value) = e match {
            case eqSeparatedOpt(arg, value) => (arg, value)
            case _ => (e, null)
          }

          arg match {

            case arg if !findCliOption(arg, options).isEmpty => {
              val cliOption = findCliOption(arg, options).get.head
              if (value == null) {
                if (idx == args.length) {
                  throw new IllegalArgumentException("Missing argument for option '%s'." + arg)
                }
                curIdx = idx + 1
                value = args(curIdx)
              }

              if (!handle(cliOption, value)) {
                break
              }
            }

            case arg if !findCliOption(arg, switches).isEmpty => {
              val cliOption = findCliOption(arg, switches).get.head
              if (!handle(cliOption, null)) {
                break
              }
            }
            case arg => {
              if (!handleUnknown(arg)) {
                break
              }
            }
          }
        }
      }
    }

    handleExtraArgs(args.slice(curIdx + 1, args.length))
  }

  /**
   * As "opts" is a final variable, we can't append a new element to it, or assign a new array to it.
   * As a workaround, we build a new option array `options` for parsing.
   */
  private def updateOpts(): Unit = {
    // Compared to the original options, we remove `--class` option, add two new options `--exe`
    // and `--sparkclr-jar`, so the total length of options is `opts.length + 1`
    options = new Array[Array[String]](opts.length + 1)
    options(0) = Array(MAIN_EXECUTABLE)
    options(1) = Array(REMOTE_SPARKCLR_JAR_PATH)

    var idx = 2
    opts.foreach {
      case opt =>
        if (opt(0) != CLASS) {
          options(idx) = opt
          idx = idx + 1
        }
    }
  }

  /**
   * Merge values from the default properties file with those specified through --conf.
   * When this is called, `sparkProperties` is already filled with configs from the latter.
   */
  private def mergeDefaultSparkProperties(): Unit = {

    /** Default properties present in the currently defined defaults file. */
    lazy val defaultSparkProperties: HashMap[String, String] = {
      val defaultProperties = new HashMap[String, String]()
      Option(propertiesFile).foreach { filename =>
        Utils.getPropertiesFromFile(filename).foreach { case (k, v) =>
          defaultProperties(k) = v
        }
      }
      defaultProperties
    }

    // Use common defaults file, if not specified by user
    propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
    // Honor --conf before the defaults file
    defaultSparkProperties.foreach { case (k, v) =>
      if (!sparkProperties.contains(k)) {
        sparkProperties(k) = v
      }
    }
  }

  /** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {

    var appendToCmd = true

    opt match {
      case MAIN_EXECUTABLE =>
        mainExecutable = value
        appendToCmd = false

      case REMOTE_SPARKCLR_JAR_PATH =>
        remoteSparkCLRJarPath = value
        appendToCmd = false

      case MASTER =>
        master = value

      case NAME =>
        appName = value

      case PROPERTIES_FILE =>
        propertiesFile = value

      case DEPLOY_MODE =>
        if (value != "client" && value != "cluster") {
          SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
        }
        deployMode = value

      case FILES =>
        files = Utils.resolveURIs(value)
        appendToCmd = false

      case JARS =>
        if (jars != "") {
          jars = s"$jars,$value"
        } else {
          jars = value
        }
        appendToCmd = false

      case HELP =>
        printUsageAndExit()

      case VERSION =>
        printVersionAndExit()

      case _ => // do nothing here, let's spark-submit.cmd do the left things.
    }

    if (appendToCmd) {
      if (value != null) {
        cmd += s" $opt $value"
      } else {
        cmd += s" $opt"
      }
    }

    true
  }

  /**
   * Handle unrecognized command line options.
   *
   * The first unrecognized option is treated as the "primary resource". Everything else is
   * treated as application arguments.
   */
  override protected def handleUnknown(opt: String): Boolean = {
    // need to give user hints that "--class" option is not supported in csharpspark-submit.cmd, use --main-executable instead.

    if (opt == CLASS) {
      SparkSubmit.printErrorAndExit(s"Option '$CLASS' is not supported in SparkCLR submission.")
    }

    if (opt.startsWith("-")) {
      SparkSubmit.printErrorAndExit(s"Unrecognized option '$opt'.")
    }

    primaryResource = opt

    false
  }

  override protected def handleExtraArgs(extra: JList[String]): Unit = {
    childArgs ++= extra
  }

  private def inferSubmitArguments(): Unit = {
    //figure out deploy mode
    deployMode = Option(deployMode).orElse(env.get("DEPLOY_MODE")).orNull
    master = Option(master).orElse(sparkProperties.get("spark.master")).orElse(env.get("MASTER")).orNull
    master match {
      case "yarn-cluster" => deployMode = "cluster"
      case "yarn-client" => deployMode = "client"
      case _ =>
    }
  }

  /**
   * Only check SparkCLR specific arguments, let's spark-submit.cmd to do all the other validations.
   */
  private def validateSubmitArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }

    if (primaryResource == null) {
      printErrorAndExit("No primary resource found; Please specify one with a zip file or a directory)")
    }

    if (mainExecutable == null || !mainExecutable.toLowerCase().endsWith(".exe")) {
      printErrorAndExit("No main executable found; please specify one with --exe")
    }

    if (deployMode == "cluster" && master.startsWith("spark://")) {
      if (remoteSparkCLRJarPath == null) {
        printErrorAndExit(s"No remote sparkclr jar found; please specify one with option $REMOTE_SPARKCLR_JAR_PATH")
      }

      if (!remoteSparkCLRJarPath.toLowerCase.startsWith("hdfs://")) {
        printErrorAndExit("Remote sparkclr jar shouldn't be a local file.")
      }
    } else {
      if (remoteSparkCLRJarPath != null) {
        printError(s"No need to specify $REMOTE_SPARKCLR_JAR_PATH option in current deploy mode.")
      }
    }
  }

  /**
   * local mode
   */
  private def concatLocalCmdOptions(): Unit = {

    if (jars != null && !jars.trim.isEmpty) cmd += s" --jars $jars"

    cmd += s" --class $csharpRunnerClass $sparkCLRJarPath $primaryResource"

    findMainExecutable()

    if (mainExecutable != null) cmd += s" $mainExecutable "

    if (childArgs.nonEmpty) cmd += (" " + childArgs.mkString(" "))

  }

  private def concatCmdOptions(): Unit = {

    if (appName == null) cmd = cmd.trim + s" --name " + mainExecutable.stripSuffix(".exe")

    master match {

      case m if m == null || m.startsWith("local") => concatLocalCmdOptions()

      case m if m.toLowerCase.startsWith("spark://") && deployMode == "cluster" => {
        val remoteDriverPath = primaryResource
        files = files match {
          case null => remoteDriverPath
          case _ => files + ("," + remoteDriverPath)
        }

        if(jars != null && !jars.isEmpty) cmd += (s" --jars $jars")

        cmd += (s" --files $files --class $csharpRunnerClass $remoteSparkCLRJarPath" +
          s" $remoteDriverPath $mainExecutable")
        if (childArgs.length > 1) cmd += (" " + childArgs.mkString(" "))
      }

      case _ => {

        if (jars != null && !jars.isEmpty) cmd = cmd.trim + s" --jars $jars"

        findMainExecutable()
        val zippedPrimaryResource: File = zipPrimaryResource()

        files match {
          case null => files = zippedPrimaryResource.getPath
          case _ => files += ("," + zippedPrimaryResource.getPath)
        }

        if (files != null) cmd += s" --files $files"

        deployMode match {

          case "client" => {
            cmd += (s" --class $csharpRunnerClass $sparkCLRJarPath " + primaryResource)
          }

          case "cluster" => {
            cmd += (s" --class $csharpRunnerClass $sparkCLRJarPath " + zippedPrimaryResource.getName)
          }

          case _ =>
        }

        if (mainExecutable != null) cmd += s" $mainExecutable"

        if (childArgs.nonEmpty) cmd += (" " + childArgs.mkString(" "))

      }
    }
  }

  private def findMainExecutable(): Unit = {

    primaryResource match {

      case pr if (new File(pr)).isDirectory => {
        deployMode match {
          case "cluster" =>
          case _ => mainExecutable = new File(new File(pr).getAbsoluteFile, mainExecutable).getPath
        }
      }

      case pr if pr.endsWith(".zip") => {
        deployMode match {
          case "cluster" =>
          case _ => mainExecutable = new File(new File(primaryResource).getAbsoluteFile.getParent, mainExecutable).getPath
        }
      }

      case _ =>
    }
  }

  /**
   * In order not to miss any driver dependencies, all files under user driver directory (SparkCLR DLLs and CSharpWorker.exe should also be included)
   * will assembled into a zip file and shipped by --file parameter
   * @return
   */
  private def zipPrimaryResource(): File = {

    var zippedResource: File = null

    primaryResource match {

      case pr if new File(pr).isDirectory => {
        zippedResource = new File(System.getProperty("java.io.tmpdir"), new File(primaryResource).getName + "_" + System.currentTimeMillis() + ".zip")
        CSharpUtils.zip(new File(primaryResource), zippedResource)
      }

      case pr if pr.endsWith(".exe") => {
        zippedResource = new File(System.getProperty("java.io.tmpdir"), System.currentTimeMillis() + ".zip")
        CSharpUtils.zip(new File(primaryResource).getParentFile, zippedResource)
      }

      case pr if pr.endsWith(".zip") => zippedResource = new File(primaryResource)

      case _ =>
    }

    if (zippedResource != null && !primaryResource.endsWith(".zip")) {
      SparkSubmit.printStream.println("Zip driver directory " + new File(primaryResource).getAbsolutePath + " to " + zippedResource.getPath)
    }

    zippedResource
  }

  def buildCmdOptions(): String = {

    updateOpts()

    // Set parameters from command line arguments
    try {
      parse(args.toList)
    } catch {
      case e: IllegalArgumentException =>
        SparkSubmit.printErrorAndExit(e.getMessage())
    }

    mergeDefaultSparkProperties()
    inferSubmitArguments()
    validateSubmitArguments()
    concatCmdOptions()

    " " + cmd.trim
  }

  /** Follow the convention in pom.xml that SparkCLR has the same version with Spark */
  private def printVersionAndExit(): Unit = {
    printStream.println( """Welcome to version %s-SNAPSHOT""".format(SPARK_VERSION))
    printStream.println("Type --help for more information.")
    exitFn(1)
  }

  /**
   * Besides SparkCLR only options, copy&paste other options from Spark directly.
   */
  private def printUsageAndExit(exitCode: Int = 1): Unit = {
    printStream.println(
      """Usage: sparkclr-submit [options] <app zip file | app directory> [app arguments]
        |
        |Options:
        |
        |SparkCLR only:
        |  --exe                       [Mandatory] name of driver .exe file
        |  --remote-sparkclr-jar       remote HDFS file path of SparkCLR jar, this option is
        |                              required when deploy mode is standalone cluster.
        |
        |Spark common:
        |  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
        |  --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
        |                              on one of the worker machines inside the cluster ("cluster")
        |                              (Default: client).
        |  --name NAME                 A name of your application.
        |  --jars JARS                 Comma-separated list of local jars to include on the driver
        |                              and executor classpaths.
        |  --packages                  Comma-separated list of maven coordinates of jars to include
        |                              on the driver and executor classpaths. Will search the local
        |                              maven repo, then maven central and any additional remote
        |                              repositories given by --repositories. The format for the
        |                              coordinates should be groupId:artifactId:version.
        |  --repositories              Comma-separated list of additional remote repositories to
        |                              search for the maven coordinates given with --packages.
        |  --files FILES               Comma-separated list of files to be placed in the working
        |                              directory of each executor.
        |
        |  --conf PROP=VALUE           Arbitrary Spark configuration property.
        |  --properties-file FILE      Path to a file from which to load extra properties. If not
        |                              specified, this will look for conf/spark-defaults.conf.
        |
        |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 512M).
        |  --driver-java-options       Extra Java options to pass to the driver.
        |  --driver-library-path       Extra library path entries to pass to the driver.
        |  --driver-class-path         Extra class path entries to pass to the driver. Note that
        |                              jars added with --jars are automatically included in the
        |                              classpath.
        |
        |  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).
        |
        |  --proxy-user NAME           User to impersonate when submitting the application.
        |
        |  --help, -h                  Show this help message and exit
        |        |  --verbose, -v               Print additional debug output
        |        |  --version,                  Print the version of current Spark
        |
        | Spark standalone with cluster deploy mode only:
        |  --driver-cores NUM          Cores for driver (Default: 1).
        |
        | Spark standalone or Mesos with cluster deploy mode only:
        |  --supervise                 If given, restarts the driver on failure.
        |
        | Spark standalone and Mesos only:
        |  --total-executor-cores NUM  Total cores for all executors.
        |
        | Spark standalone and YARN only:
        |  --executor-cores NUM        Number of cores per executor. (Default: 1 in YARN mode,
        |                              or all available cores on the worker in standalone mode)
        |
        | YARN-only:
        |  --driver-cores NUM          Number of cores used by the driver, only in cluster mode
        |                              (Default: 1).
        |  --queue QUEUE_NAME          The YARN queue to submit to (Default: "default").
        |  --num-executors NUM         Number of executors to launch (Default: 2).
        |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
        |                              working directory of each executor.
        |  --principal PRINCIPAL       Principal to be used to login to KDC, while running on
        |                              secure HDFS.
        |  --keytab KEYTAB             The full path to the file that contains the keytab for the
        |                              principal specified above. This keytab will be copied to
        |                              the node running the Application Master via the Secure
        |                              Distributed Cache, for renewing the login tickets and the
        |                              delegation tokens periodically.
      """.stripMargin
    )
    exitFn(exitCode)
  }
}
