// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.spark.csharp

import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Sorting

/**
  * Spark driver implementation in scala used for SparkCLR perf benchmarking
  */
object PerfBenchmark {

  val perfResults = collection.mutable.Map[String, ListBuffer[Long]]()
  val executionTimeList = scala.collection.mutable.ListBuffer.empty[Long]

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      val jar = new File(PerfBenchmark.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
      println(s"Usage   : $jar  run-times  data")
      println(s"Example : $jar  10         hdfs:///perfdata/freebasedeletions/*")
      println(s"Example : $jar  1          hdfs:///perf/data/deletions/deletions.csv-00000-of-00020")
      sys.exit(1)
    }

    val PerfSuite = Class.forName("com.microsoft.spark.csharp.PerfSuite")
    val sparkConf = new SparkConf().setAppName("SparkCLR perf suite - scala")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)

    RunPerfSuites(args, sparkContext, sqlContext, "com.microsoft.spark.csharp.FreebaseDeletionsBenchmark")

    sparkContext.stop
    ReportResult()
  }

  def RunPerfSuites(args: Array[String], sparkContext: SparkContext, sqlContext: SQLContext, className: String): Unit = {
    val freebaseDeletionsBenchmarkClass = Class.forName(className)
    val perfSuites = freebaseDeletionsBenchmarkClass.getDeclaredMethods

    for (perfSuiteMethod <- perfSuites) {
      val perfSuiteName = perfSuiteMethod.getName
      if (perfSuiteName.startsWith("Run")) {
        //TODO - use annotation type
        executionTimeList.clear
        var runCount = args(0).toInt
        for (k <- 1 to runCount) {
          println(s"Starting perf suite $perfSuiteName : times[$k]-$runCount")
          perfSuiteMethod.invoke(freebaseDeletionsBenchmarkClass, args, sparkContext, sqlContext: SQLContext)
        }
        val executionTimeListRef = scala.collection.mutable.ListBuffer.empty[Long]
        for (v <- executionTimeList) {
          executionTimeListRef += v
        }
        perfResults += (perfSuiteName -> executionTimeListRef)
      }
    }
  }

  def ReportResult(): Unit = {
    println("** Printing results of the perf run (scala) **")
    var allMedianCosts = collection.immutable.SortedMap[String, Long]()
    for (result <- perfResults.keys) {
      val perfResult = perfResults(result)
      //multiple enumeration happening - ignoring that for now
      val min = perfResult.min
      val max = perfResult.max
      val runCount = perfResult.length
      val avg = perfResult.sum / runCount
      val median = getMedian(perfResult.toList)
      val values = perfResult.mkString(",")
      println(s"** Execution time for $result in seconds. Min=$min, Max=$max, Average=$avg, Median=$median, Number of runs=$runCount, Individual execution duration=[$values] **")
      allMedianCosts += (result -> median)
    }
    println("** *** **")
    val currentTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz").format(new java.util.Date)
    println(s"${currentTime} Scala version: Run count = ${perfResults.head._2.length}, all median time costs[${allMedianCosts.size}]: ${allMedianCosts.map(kv => kv._1 + "=" + kv._2).mkString("; ")}")
  }

  def getMedian(valuesList: List[Long]) = {
    val itemCount = valuesList.length
    val values = valuesList.toArray
    java.util.Arrays.sort(values)
    if (itemCount == 1)
      values(0)

    if (itemCount % 2 == 0) {
      (values(itemCount / 2) + values(itemCount / 2 - 1)) / 2
    }

    values((itemCount - 1) / 2)
  }
}

