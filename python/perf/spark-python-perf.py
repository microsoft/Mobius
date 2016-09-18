# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.
#coding:utf8

import sys, os, time
from datetime import datetime
from PerfBenchmark import PerfBenchmark
from FreebaseDeletionsBenchmark import FreebaseDeletionsBenchmark

if __name__ == "__main__":
    argCount = len(sys.argv)
    if not argCount == 3:
        exeName = os.path.basename(__file__)
        sys.stderr.write("Usage:     " + exeName + "  runCount   data\n")
        sys.stderr.write("Example-1: " + exeName + "  10          hdfs:///perfdata/freebasedeletions/* \n")
        sys.stderr.write("Example-2: " + exeName + "  1           hdfs:///perf/data/deletions/deletions.csv-00000-of-00020\n")
        exit(-1)

    print(str(datetime.now()) + " runCount = " + str(sys.argv[1]) + ", data = " + str(sys.argv[2]))

    from pyspark import SparkContext,SQLContext
    beginTime = time.time()

    sparkContext = SparkContext()
    sqlContext = SQLContext(sparkContext)

    PerfBenchmark.RunPerfSuite(FreebaseDeletionsBenchmark, sys.argv, sparkContext, sqlContext)

    sparkContext.stop()

    PerfBenchmark.ReportResult()

    print(str(datetime.now()) + " " + os.path.basename(__file__) + " : Finished python version benchmark test. Whole time = " + ("%.3f" % (time.time() - beginTime)) + " s.")
