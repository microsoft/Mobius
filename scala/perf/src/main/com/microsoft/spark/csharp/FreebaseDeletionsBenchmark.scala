// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.spark.csharp

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Duration

/**
  * Perf benchmark that users Freebase deletions data
  * This data is licensed under CC-BY license (http://creativecommons.org/licenses/by/2.5)
  * Data is available for download at https://developers.google.com/freebase/data)
  * Data format - CSV, size - 8 GB uncompressed
  * Columns in the dataset are
  *     1. creation_timestamp (Unix epoch time in milliseconds)
  *     2. creator
  *     3. deletion_timestamp (Unix epoch time in milliseconds)
  *     4. deletor
  *     5. subject (MID)
  *     6. predicate (MID)
  *     7. object (MID/Literal)
  *     8. language_code
  */
object FreebaseDeletionsBenchmark {

  @PerfSuite
  def RunRDDLineCount(args: Array[String], sc: SparkContext, sqlContext: SQLContext): Unit = {
    val startTime = System.currentTimeMillis

    val lines = sc.textFile(args(1))
    val count = lines.count
    val elapsed = System.currentTimeMillis - startTime

    val elapsedDuration = new Duration(elapsed)
    val totalSeconds = elapsedDuration.milliseconds/1000
    PerfBenchmark.executionTimeList += totalSeconds

    println("Count of lines " + count + ". Time elapsed " + elapsedDuration)
  }

  @PerfSuite
  def RunRDDMaxDeletionsByUser(args: Array[String], sc: SparkContext, sqlContext: SQLContext): Unit = {
    val startTime = System.currentTimeMillis

    val lines = sc.textFile(args(1))
    val parsedRows = lines.map(s => {
      val columns = s.split(',')
      //data has some bad records - use bool flag to indicate corrupt rows
      if (columns.length > 4)
        Tuple5(true, columns(0), columns(1), columns(2), columns(3))
      else
        Tuple5(false, "X", "X", "X", "X") //invalid row placeholder
    })

    val flaggedRows = parsedRows.filter(s => s._1) //select good rows
    val selectedDeletions = flaggedRows.filter(s => s._3.equals(s._5)) //select deletions made by same creators
    val userDeletions = selectedDeletions.map(s => new Tuple2(s._3, 1))
    val userDeletionsCount = userDeletions.reduceByKey((x, y) => x + y)
    val zeroValue = ("zerovalue", 0)
    val userWithMaxDeletions = userDeletionsCount.fold(zeroValue)( (kvp1, kvp2) => {
      if (kvp1._2 > kvp2._2)
        kvp1
      else
        kvp2
    })

    val elapsed = System.currentTimeMillis - startTime
    val elapsedDuration = new Duration(elapsed)
    val totalSeconds = elapsedDuration.milliseconds/1000
    PerfBenchmark.executionTimeList += totalSeconds

    println(s"User with max deletions is " + userWithMaxDeletions._1 + ", count of deletions="
                  + userWithMaxDeletions._2 + s". Elapsed time=$elapsedDuration")
  }

  @PerfSuite
  def RunDFLineCount(args: Array[String], sc: SparkContext, sqlContext: SQLContext): Unit = {
    val startTime = System.currentTimeMillis

    val rows = sqlContext.read.format("com.databricks.spark.csv").load(args(1))
    val rowCount = rows.count

    val elapsed = System.currentTimeMillis - startTime
    val elapsedDuration = new Duration(elapsed)
    val totalSeconds = elapsedDuration.milliseconds/1000
    PerfBenchmark.executionTimeList += totalSeconds

    println(s"Count of rows $rowCount. Time elapsed $elapsedDuration")
  }

  @PerfSuite
  def RunDFMaxDeletionsByUser(args: Array[String], sc: SparkContext, sqlContext: SQLContext): Unit = {
    val startTime = System.currentTimeMillis

    val rows = sqlContext.read.format("com.databricks.spark.csv").load(args(1))
    val filtered = rows.filter("C1 = C3")
    val aggregated = filtered.groupBy("C1").agg(("C1", "count"))
    aggregated.registerTempTable("freebasedeletions")
    val max = sqlContext.sql("select max(`COUNT(C1)`) from freebasedeletions")
    val maxArray = max.collect
    val maxValue = maxArray(0)
    val maxDeletions = sqlContext.sql("select * from freebasedeletions where `COUNT(C1)` = " + maxValue.get(0))
    maxDeletions.show
    //TODO - add perf suite for subquery
    val elapsed = System.currentTimeMillis - startTime
    val elapsedDuration = new Duration(elapsed)
    val totalSeconds = elapsedDuration.milliseconds/1000
    PerfBenchmark.executionTimeList += totalSeconds

    println(s"User with max deletions & count of deletions is listed above. Time elapsed $elapsedDuration")
  }

}
