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

Refer to `SparkCLR\csharp\Samples` directory for complete samples.

## Documents

Refer to the [docs folder](https://github.com/Microsoft/SparkCLR/tree/master/docs).

## Building SparkCLR

[![Build Status](https://ci.appveyor.com/api/projects/status/github/Microsoft/SparkCLR?svg=true&branch=master)](https://ci.appveyor.com/project/skaarthik/SparkCLR/branch/master)

### Prerequisites

* [Apache Maven](http://maven.apache.org) for spark-clr project implemented in Scala.
* MSBuild in [Visual Studio](https://www.visualstudio.com/) 2013 and above.
* .NET Framework 4.5 and above.
* [Nuget command-line utility](https://docs.nuget.org/release-notes) 3.2 and above.

### Instructions

* Navigate to `SparkCLR\scala` directory and run the following command to build spark-clr*.jar

	```
	mvn package
	```

* Start Developer Command Prompt for Visual Studio, and navigate to `SparkCLR\csharp` directory.

	- If `nuget.exe` is not already in your PATH, then run the following commands to add it.

		```  
		set PATH=<fullpath to nuget.exe>;%PATH%  
		```
  
	- Then  build the rest of the .NET binaries  

		```  
		Build.cmd  
		```
  
* Optional: Under `SparkCLR\csharp` directory, run the following command to clean the .NET binaries built above  

    ```
    Clean.cmd
    ```   

## Running Samples

### Prerequisites

DataFrame TextFile API uses `spark-csv` package to load data from CSV file. 
Latest [commons-csv-*.jar](http://commons.apache.org/proper/commons-csv/download_csv.cgi) and [spark-csv*.jar (Scala version:2.10)](http://spark-packages.org/package/databricks/spark-csv) should be downloaded manually.

The following environment variables should be set properly:

* `JAVA_HOME`

* `SPARK_HOME` currently SparkCLR only supports Spark 1.4.x.

* `HADOOP_HOME` hadoop version should be consistent with spark version. For example, if spark build version is spark-1.4.1-bin-hadoop2.6, then only hadoop release 2.6.x should be used.

* `SPARKCSV_JARS` should include full paths to `commons-csv*.jar`, `spark-csv*.jar` for CSV and kafka*.jar, metrics-core*.jar, spark-streaming-kafka*.jar for Kafka. 

	For example:     
	```
	set SPARKCSV_JARS=%SPARKCLR_HOME%\lib\commons-csv-1.2.jar;%SPARKCLR_HOME%\lib\spark-csv_2.10-1.2.0.jar;%SPARKCLR_HOME%\lib\kafka_2.10-0.8.2.1.jar;%SPARKCLR_HOME%\lib\metrics-core-2.2.0.jar;%SPARKCLR_HOME%\lib\spark-streaming-kafka_2.10-1.4.1.jar
	```

* `SPARKCLR_HOME` should point to a directory prepared with following sub-directories:  

  * **lib** ( `spark-clr*.jar` )  
  * **bin** ( `Microsoft.Spark.CSharp.Adapter.dll`, `CSharpWorker.exe`)  
  * **samples** ( The contents of `SparkCLR\csharp\Samples\Microsoft.Spark.CSharp\bin\[Debug|Release]\*`, including `Microsoft.Spark.CSharp.Adapter.dll`, `CSharpWorker.exe`, `SparkCLRSamples.exe`, `SparkCLRSamples.exe.Config` etc. ) 
  * **scripts** ( `sparkclr-submit.cmd` )  
  * **data** ( `SparkCLR\csharp\Samples\Microsoft.Spark.CSharp\data\*` )  

### Running in Local mode

Set `CSharpWorkerPath` in `SparkCLRSamples.exe.config` and run the following command: 

```
sparkclr-submit.cmd --verbose --exe SparkCLRSamples.exe  %SPARKCLR_HOME%\samples spark.local.dir C:\temp\SparkCLRTemp sparkclr.sampledata.loc %SPARKCLR_HOME%\data
```   

Note that SparkCLR jar version (**1.4.1**) should be aligned with Apache Spark version.  

Setting `spark.local.dir` parameter is important. When local Spark instance distributes SparkCLR driver executables to Windows `%TEMP%` directory, anti-virus software may detect and report the executables showed up in `%TEMP%` directory as malware.

### Running in Standalone mode
```
sparkclr-submit.cmd --verbose --master spark://host:port --exe SparkCLRSamples.exe  %SPARKCLR_HOME%\samples sparkclr.sampledata.loc hdfs://path/to/sparkclr/sampledata
```

### Running in YARN mode

```
sparkclr-submit.cmd --verbose --master yarn-cluster --exe SparkCLRSamples.exe %SPARKCLR_HOME%\samples sparkclr.sampledata.loc hdfs://path/to/sparkclr/sampledata
```

## Running Unit Tests

* In Visual Studio: "Test" -> "Run" -> "All Tests"

* In Developer Command Prompt for VS, navigate to `SparkCLR\csharp` and run the following command: 
    ```
    Test.cmd
    ```

## Debugging Tips

CSharpBackend and C# driver are separately launched for debugging SparkCLR Adapter or driver.

For example, to debug SparkCLR samples:

* Launch CSharpBackend.exe using `sparkclr-submit.cmd debug` and get the port number displayed in the console.  
* Navigate to `csharp/Samples/Microsoft.Spark.CSharp` and edit `App.Config` to use the port number from the previous step for `CSharpBackendPortNumber` config and also set `CSharpWorkerPath` config values.  
* Run `SparkCLRSamples.exe` in Visual Studio.

## License

SparkCLR is licensed under the MIT license. See [LICENSE](LICENSE) file for full license information.

## Contribution

We welcome contributions. To contribute, follow the instructions in [CONTRIBUTING.md](CONTRIBUTING.md). 
