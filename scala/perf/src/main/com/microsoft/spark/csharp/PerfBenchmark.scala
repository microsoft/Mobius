// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.spark.csharp

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer
import scala.util.Sorting

/**
  * Spark driver implementation in scala used for SparkCLR perf benchmarking
  */
object PerfBenchmark {

  val perfResults = collection.mutable.Map[String, ListBuffer[Long]]()
  val executionTimeList = scala.collection.mutable.ListBuffer.empty[Long]

  def main(args: Array[String]): Unit = {
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

    for ( perfSuiteMethod <- perfSuites) {
      val perfSuiteName = perfSuiteMethod.getName
      if (perfSuiteName.startsWith("Run")) { //TODO - use annotation type
        executionTimeList.clear
        var runCount = args(0).toInt
        while (runCount > 0) {
          println(s"Starting perf suite $perfSuiteName, runCount=$runCount")
          perfSuiteMethod.invoke(freebaseDeletionsBenchmarkClass, args, sparkContext, sqlContext: SQLContext)
          runCount = runCount - 1
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
    for(result <- perfResults.keys) {
        val perfResult = perfResults(result)
        //multiple enumeration happening - ignoring that for now
        val min = perfResult.min
        val max = perfResult.max
        val runCount = perfResult.length
        val avg = perfResult.sum / runCount
        val median = getMedian(perfResult.toList)
        val values = new StringBuilder
        for (value <- perfResult) {
          values.append(value + ", ")
        }

        println(s"** Execution time for $result in seconds. Min=$min, Max=$max, Average=$avg, Median=$median, Number of runs=$runCount, Individual execution duration=[$values] **")
      }
    println("** *** **")

  }

  def getMedian(valuesList: List[Long]) = {
    val itemCount = valuesList.length
    val values = valuesList.toArray
    java.util.Arrays.sort(values)
    if (itemCount == 1)
      values(0)

    if (itemCount%2 == 0) {
      (values(itemCount/2) + values(itemCount/2 - 1))/2
    }

    values((itemCount-1)/2)
  }
}

