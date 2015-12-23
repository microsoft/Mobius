# SparkCLR

[SparkCLR](https://github.com/Microsoft/SparkCLR) (pronounced Sparkler) adds C# language binding to [Apache Spark](https://spark.apache.org/), enabling the implementation of Spark driver code and data processing operations in C#.

For example, the word count sample in Apache Spark can be implemented in C# as follows :

```c#
var lines = sparkContext.TextFile(@"hdfs://path/to/input.txt");  
var words = lines.FlatMap(s => s.Split(new[] { " " }, StringSplitOptions.None));
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

## Documents

Refer to the [docs folder](docs).

## Build Status

|Ubuntu 14.04.3 LTS |Windows |
|-------------------|:------:|
|[![Build status](https://travis-ci.org/Microsoft/SparkCLR.svg?branch=master)](https://travis-ci.org/Microsoft/SparkCLR) |[![Build status](https://ci.appveyor.com/api/projects/status/lflkua81gg0swv6i/branch/master?svg=true)](https://ci.appveyor.com/project/SparkCLR/sparkclr/branch/master) |

## Building, Running and Debugging SparkCLR

* [Windows Instructions](windows-instructions.md)
* [Linux Instructions](linux-instructions.md)

(Note: Tested only with Spark 1.5.2)

## License

[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=plastic)](https://github.com/Microsoft/SparkCLR/blob/master/LICENSE)

SparkCLR is licensed under the MIT license. See [LICENSE](LICENSE) file for full license information.

## Contribution

[![Issue Stats](http://issuestats.com/github/Microsoft/SparkCLR/badge/pr)](http://issuestats.com/github/Microsoft/SparkCLR)
[![Issue Stats](http://issuestats.com/github/Microsoft/SparkCLR/badge/issue)](http://issuestats.com/github/Microsoft/SparkCLR)

We welcome contributions. To contribute, follow the instructions in [CONTRIBUTING.md](CONTRIBUTING.md). 
