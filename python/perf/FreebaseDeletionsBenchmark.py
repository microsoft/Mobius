# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

"""
Perf benchmark that users Freebase deletions data
This data is licensed under CC-BY license (http:# creativecommons.org/licenses/by/2.5)
Data is available for downloading : "Freebase Deleted Triples" at https://developers.google.com/freebase
Data format - CSV, size - 8 GB uncompressed
Columns in the dataset are
    1. creation_timestamp (Unix epoch time in milliseconds)
    2. creator
    3. deletion_timestamp (Unix epoch time in milliseconds)
    4. deletor
    5. subject (MID)
    6. predicate (MID)
    7. object (MID/Literal)
    8. language_code
"""
from __future__ import print_function
import time
from datetime import datetime
from PerfBenchmark import PerfBenchmark

class FreebaseDeletionsBenchmark:

    @staticmethod
    def RunRDDLineCount(args, sparkContext, sqlContext):
        startTime = time.time()

        lines = sparkContext.textFile(args[2])
        count = lines.count()
        elapsedDuration = time.time() - startTime
        PerfBenchmark.ExecutionTimeList.append(elapsedDuration)

        print(str(datetime.now()) + " RunRDDLineCount: Count of lines " + str(count) + ". Elapsed time = " + ("%.3f s" % elapsedDuration))


    @staticmethod
    def RunRDDMaxDeletionsByUser(args, sparkContext, sqlContext):
        def map_columns(line) :
            columns = line.split(',')
            # data has some bad records - use bool flag to indicate corrupt rows
            if len(columns) > 4 :
                return (True, columns[0], columns[1], columns[2], columns[3])
            else:
                return (False, "X", "X", "X", "X") # invalid row placeholde

        startTime = time.time()
        lines = sparkContext.textFile(args[2])
        parsedRows = lines.map(lambda s: map_columns(s))

        flaggedRows = parsedRows.filter(lambda s: s[0]) # select good rows
        selectedDeletions = flaggedRows.filter(lambda s: s[2] == s[4]) # select deletions made by same creators
        userDeletions = selectedDeletions.map(lambda s: (s[2], 1))
        userDeletionsparkContextount = userDeletions.reduceByKey(lambda x, y : x + y)
        zeroValue = ("zerovalue", 0)
        userWithMaxDeletions = userDeletionsparkContextount.fold(zeroValue, lambda kvp1, kvp2 : kvp1 if (kvp1[1] > kvp2[1]) else kvp2)

        elapsedDuration = time.time() - startTime
        PerfBenchmark.ExecutionTimeList.append(elapsedDuration)

        print(str(datetime.now()) + " RunRDDMaxDeletionsByUser: User with max deletions is " + str(userWithMaxDeletions[0]) + ", count of deletions=" + \
            str(userWithMaxDeletions[1]) + ". Elapsed time = " + ("%.3f s" % elapsedDuration))



    @staticmethod
    def RunDFLineCount(args, sparkContext, sqlContext):
        startTime = time.time()

        rows = sqlContext.read.format("com.databricks.spark.csv").load(args[2])
        rowCount = rows.count()

        elapsedDuration = time.time() - startTime
        PerfBenchmark.ExecutionTimeList.append(elapsedDuration)

        print(str(datetime.now()) + " RunDFLineCount: Count of rows " + str(rowCount) + ". Elapsed time = " + ("%.3f s" % elapsedDuration))


    @staticmethod
    def RunDFMaxDeletionsByUser(args, sparkContext, sqlContext):
        startTime = time.time()
        rows = sqlContext.read.format("com.databricks.spark.csv").load(args[2])
        filtered = rows.filter("_c1 = _c3")
        aggregated = filtered.groupBy("_c1").agg({"_c1" : "count"})
        aggregated.registerTempTable("freebasedeletions")
        max = sqlContext.sql("select max(`count(_c1)`) from freebasedeletions")
        maxArray = max.collect()
        maxValue = maxArray[0]
        maxDeletions = sqlContext.sql("select * from freebasedeletions where `count(_c1)` = " + str(maxValue[0]))
        maxDeletions.show()
        # TODO - add perf suite for subquery
        elapsedDuration = time.time() - startTime
        PerfBenchmark.ExecutionTimeList.append(elapsedDuration)

        print(str(datetime.now()) + " RunDFMaxDeletionsByUser: User with max deletions & count of deletions is listed above. Elapsed time = " + ("%.3f s" % elapsedDuration))
