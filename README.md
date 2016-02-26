# Mobius: C# API for Spark

[Mobius](https://github.com/Microsoft/SparkCLR) adds C# language binding to [Apache Spark](https://spark.apache.org/), enabling the implementation of Spark driver code and data processing operations in C#.

For example, the word count sample in Apache Spark can be implemented in C# as follows :

```c#
var lines = sparkContext.TextFile(@"hdfs://path/to/input.txt");  
var words = lines.FlatMap(s => s.Split(' '));
var wordCounts = words.Map(w => new KeyValuePair<string, int>(w.Trim(), 1))  
                      .ReduceByKey((x, y) => x + y);  
var wordCountCollection = wordCounts.Collect();  
wordCounts.SaveAsTextFile(@"hdfs://path/to/wordcount.txt");  
```

A simple DataFrame application using TempTable may look like the following:

```c#
var reqDataFrame = sqlContext.TextFile(@"hdfs://path/to/requests.csv");
var metricDataFrame = sqlContext.TextFile(@"hdfs://path/to/metrics.csv");
reqDataFrame.RegisterTempTable("requests");
metricDataFrame.RegisterTempTable("metrics");
// C0 - guid in requests DataFrame, C3 - guid in metrics DataFrame  
var joinDataFrame = GetSqlContext().Sql(  
    "SELECT joinedtable.datacenter" +
         ", MAX(joinedtable.latency) maxlatency" +
         ", AVG(joinedtable.latency) avglatency " + 
    "FROM (" +
       "SELECT a.C1 as datacenter, b.C6 as latency " +  
       "FROM requests a JOIN metrics b ON a.C0  = b.C3) joinedtable " +   
    "GROUP BY datacenter");
joinDataFrame.ShowSchema();
joinDataFrame.Show();
```

A simple DataFrame application using DataFrame DSL may look like the following:

```  c#
// C0 - guid, C1 - datacenter
var reqDataFrame = sqlContext.TextFile(@"hdfs://path/to/requests.csv")  
                             .Select("C0", "C1");    
// C3 - guid, C6 - latency   
var metricDataFrame = sqlContext.TextFile(@"hdfs://path/to/metrics.csv", ",", false, true)
                                .Select("C3", "C6"); //override delimiter, hasHeader & inferSchema
var joinDataFrame = reqDataFrame.Join(metricDataFrame, reqDataFrame["C0"] == metricDataFrame["C3"])
                                .GroupBy("C1");
var maxLatencyByDcDataFrame = joinDataFrame.Agg(new Dictionary<string, string> { { "C6", "max" } });
maxLatencyByDcDataFrame.ShowSchema();
maxLatencyByDcDataFrame.Show();
```

Refer to [SparkCLR\csharp\Samples](csharp/Samples) directory and [sample usage](csharp/Samples/Microsoft.Spark.CSharp/samplesusage.md) for complete samples.

## API Documentation

Refer to [Mobius C# API documentation](csharp/Adapter/documentation/SparkCLR_API_Documentation.md) for the list of Spark's data processing operations supported in SparkCLR.

## API Usage

Mobius API usage samples are available at:

* [Samples project](csharp/Samples/Microsoft.Spark.CSharp/) which uses a comprehensive set of Mobius APIs to implement samples that are also used for functional validation of APIs

* [Examples folder](./examples) which contains standalone C# projects that can be used as templates to start developing Mobius applications

* Performance test scenarios implemented in [C#](csharp/Perf/Microsoft.Spark.CSharp) and [Scala](scala/perf) for side by side comparison of Spark driver code

## Documents

Refer to the [docs folder](docs) for design overview and other info on Mobius

## Build Status

|Ubuntu 14.04.3 LTS |Windows |Unit test coverage |
|-------------------|:------:|:-----------------:|
|[![Build status](https://travis-ci.org/Microsoft/SparkCLR.svg?branch=master)](https://travis-ci.org/Microsoft/SparkCLR) |[![Build status](https://ci.appveyor.com/api/projects/status/lflkua81gg0swv6i/branch/master?svg=true)](https://ci.appveyor.com/project/SparkCLR/sparkclr/branch/master) |[![codecov.io](https://codecov.io/github/Microsoft/SparkCLR/coverage.svg?branch=master)](https://codecov.io/github/Microsoft/SparkCLR?branch=master) |

## Getting Started

| |Windows |Linux |
|---|:------:|:----:|
|Build & run unit tests |[windows-instructions.md](notes/windows-instructions.md#building-sparkclr) |[linux-instructions.md](notes/linux-instructions.md#building-sparkclr) |
|Run samples (functional tests) in local mode |[windows-instructions.md](notes/windows-instructions.md#running-samples) |[linux-instructions.md](notes/linux-instructions.md#running-samples) |
|Run examples in local mode |[running-sparkclr-app.md](notes/running-sparkclr-app.md#running-examples-in-local-mode) |[running-sparkclr-app.md](notes/running-sparkclr-app.md#linux-instructions) |
|Run Mobius app in standalone cluster |[running-sparkclr-app.md](notes/running-sparkclr-app.md#standalone-cluster) |[running-sparkclr-app.md](notes/running-sparkclr-app.md#linux-instructions) |
|Run Mobius app in YARN cluster |[running-sparkclr-app.md](notes/running-sparkclr-app.md#yarn-cluster) |[running-sparkclr-app.md](notes/running-sparkclr-app.md#linux-instructions) |

Note: Refer to [linux-compatibility.md](notes/linux-compatibility.md) for using Mobius with Spark on Linux

## Supported Spark Versions

Mobius is built and tested with [Spark 1.4.1](https://github.com/Microsoft/SparkCLR/tree/branch-1.4), [Spark 1.5.2](https://github.com/Microsoft/SparkCLR/tree/branch-1.5) and [Spark 1.6.0](https://github.com/Microsoft/SparkCLR/tree/master).

## License

[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=plastic)](https://github.com/Microsoft/SparkCLR/blob/master/LICENSE)

Mobius is licensed under the MIT license. See [LICENSE](LICENSE) file for full license information.


## Community

[![Issue Stats](http://issuestats.com/github/Microsoft/SparkCLR/badge/pr)](http://issuestats.com/github/Microsoft/SparkCLR)
[![Issue Stats](http://issuestats.com/github/Microsoft/SparkCLR/badge/issue)](http://issuestats.com/github/Microsoft/SparkCLR)
[![Join the chat at https://gitter.im/Microsoft/SparkCLR](https://badges.gitter.im/Microsoft/SparkCLR.svg)](https://gitter.im/Microsoft/SparkCLR?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

* Mobius project welcomes contributions. To contribute, follow the instructions in [CONTRIBUTING.md](notes/CONTRIBUTING.md)

* Options to ask your question to the Mobius community
  * create issue on [GitHub](https://github.com/Microsoft/SparkCLR)
  * create post with "sparkclr" tag in [Stack Overflow](https://stackoverflow.com/questions/tagged/sparkclr)
  * send email to sparkclr-user@googlegroups.com
  * join chat at [Mobius room in Gitter](https://gitter.im/Microsoft/SparkCLR)
