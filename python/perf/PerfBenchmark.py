# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

import time, inspect, re
from datetime import datetime

class PerfBenchmark(object):
    PerfNameResults = {} ## <string, List<long>>
    ExecutionTimeList = []  # List<long>

    @staticmethod
    def RunPerfSuite(perfClass, args, sparkContext, sqlContext):
        for name, fun in inspect.getmembers(perfClass, lambda fun : inspect.isfunction(fun) and fun.__name__.startswith("Run")) :
            PerfBenchmark.ExecutionTimeList = []
            runCount = int(args[1])
            for k in range(runCount):
                print(str(datetime.now()) + " Starting perf suite : " + str(perfClass.__name__) + "." + str(name) + " times[" + str(k + 1) + "]-" + str(runCount))
                fun(args, sparkContext, sqlContext)

            executionTimeListRef = []
            for v in PerfBenchmark.ExecutionTimeList :
                executionTimeListRef.append(v)

            PerfBenchmark.PerfNameResults[name] = executionTimeListRef


    @staticmethod
    def ReportResult() :
        print(str(datetime.now()) + " ** Printing results of the perf run (python) **")
        allMedianCosts = {}
        for name in PerfBenchmark.PerfNameResults :
            perfResult = PerfBenchmark.PerfNameResults[name]
            # print(str(datetime.now()) + " " + str(result) + " time costs : " + ", ".join(("%.3f" % e) for e in perfResult))
            # multiple enumeration happening - ignoring that for now
            precision = "%.0f"
            minimum = precision % min(perfResult)
            maximum = precision % max(perfResult)
            runCount = len(perfResult)
            avg = precision % (sum(perfResult) / runCount)
            median = precision % PerfBenchmark.GetMedian(perfResult)
            values = ", ".join((precision % e) for e in perfResult)
            print(str(datetime.now()) + " ** Execution time for " + str(name) + " in seconds: " + \
                "Min=" + str(minimum) + ", Max=" + str(maximum) + ", Average=" + str(avg) + ", Median=" + str(median) + \
                ".  Run count=" + str(runCount) + ", Individual execution duration=[" + values + "]")
            allMedianCosts[name] = median

        print(str(datetime.now()) + " ** *** **")
        print(time.strftime('%Y-%m-%d %H:%M:%S ') + re.sub(r'(\w)\S*\s*', r'\1', time.strftime('%Z')) + " Python version: Run count = " + str(runCount) + ", all median time costs[" + str(len(allMedianCosts)) + "] : " + \
            "; ".join((e + "=" + allMedianCosts[e]) for e in sorted(allMedianCosts)))

    @staticmethod
    def GetMedian(values) :
        itemCount = len(values)
        values.sort()
        if itemCount == 1:
          return values[0]

        if itemCount % 2 == 0:
          return (values[int(itemCount / 2)] + values[int(itemCount / 2 - 1)]) / 2

        return values[int((itemCount - 1) / 2)]
