# SparkCLR
SparkCLR (pronounced sparkler) adds C# language binding to Apache Spark enabling the implementation of Spark driver code and data processing operations in C#.
For example, the word count sample in Apache Spark can be implemented in C# as follows  
```c#
    var lines = sparkContext.TextFile(@"hdfs://path/to/input.txt");  
    var words = lines.FlatMap(s => s.Split(new[] { " " }, StringSplitOptions.None));
    var wordCounts = words.Map(w => new KeyValuePair<string, int>(w.Trim(), 1))  
                                    .ReduceByKey((x, y) => x + y);  
    var wordCountCollection = wordCounts.Collect();  
    wordCounts.SaveAsTextFile(@"hdfs://path/to/wordcount.txt");  
```
A simple DataFrame application using TempTable may look like the following
```c#
    var requestsDataFrame = sqlContext.TextFile(@"hdfs://path/to/requests.csv");
    var metricsDateFrame = sqlContext.TextFile(@"hdfs://path/to/metrics.csv");
    requestsDataFrame.RegisterTempTable("requests");
    metricsDateFrame.RegisterTempTable("metrics");
    // C0 - guid in requests DF, C3 - guid in metrics DF  
    var join = GetSqlContext().Sql(  
        "SELECT joinedtable.datacenter, MAX(joinedtable.latency) maxlatency, AVG(joinedtable.latency) avglatency " +
        "FROM (SELECT a.C1 as datacenter, b.C6 as latency " +  
               "FROM requests a JOIN metrics b ON a.C0  = b.C3) joinedtable " +   
        "GROUP BY datacenter");
	join.ShowSchema();
	join.Show();
```
A simple DataFrame application using DataFrame DSL may look like the following
```  c#
    // C0 - guid, C1 - datacenter
    var requestsDataFrame = sqlContext.TextFile(@"hdfs://path/to/requests.csv")  
                                      .Select("C0", "C1");    
    // C3 - guid, C6 - latency   
    var metricsDateFrame = sqlContext.TextFile(@"hdfs://path/to/metrics.csv", ",", false, true)
                                     .Select("C3", "C6"); //override delimiter, hasHeader & inferSchema
    var joinDataFrame = requestsDataFrame.Join(metricsDateFrame, requestsDataFrame["C0"] == metricsDateFrame["C3"])
                                         .GroupBy("C1");
    var maxLatencyByDcDataFrame = joinDataFrame.Agg(new Dictionary<string, string> { { "C6", "max" } });
    maxLatencyByDcDataFrame.ShowSchema();
    maxLatencyByDcDataFrame.Show();
```
Refer to SparkCLR\csharp\Samples directory for complete samples

## Documents
Refer to the docs @ https://github.com/Microsoft/SparkCLR/tree/master/docs

## Building SparkCLR
### Prerequisites
* [Apache Maven](http://maven.apache.org) for spark-clr project implemented in scala
* MSBuild in Visual Studio 2013 and above
* .NET Framework 4.5 and above
* [Nuget command-line utility](https://docs.nuget.org/release-notes) 3.2 and above

### Instructions
* Navigate to SparkCLR\scala directory and run the following command to build spark-clr*.jar   
```mvn package```
* Start Developer Command Prompt for Visual Studio, navigate to SparkCLR\csharp directory, run the following commands to add nuget.exe to the path and build the rest of .Net binaries  
```set PATH=<fullpath to nuget.exe>;%PATH%```  
```build.cmd```
* Under SparkCLR|csharp directory, run the following command to clean the .NET binaries built above  
```clean.cmd```   
## Running Samples
### Prerequisites
Set the following environment variables  
* ```JAVA_HOME```  
* ```SCALA_HOME```  
* ```SPARKCLR_HOME```  
* ```SPARKCSV_JARS``` (if sqlContext.TextFile method is used to create DataFrame from csv files)

Directory pointed by ```SPARKCLR_HOME``` should have the following directories and files  
* **lib** (spark-clr*.jar)  
* **bin** (Microsoft.Spark.CSharp.Adapter.dll, CSharpWorker.exe)  
* **scripts** (sparkclr-submit.cmd)  
* **samples** (SparkCLRSamples.exe, Microsoft.Spark.CSharp.Adapter.dll, CSharpWorker.exe)  
* **data** (all the data files used by samples)  

### Running in Local mode
Set ```CSharpWorkerPath``` in SparkCLRSamples.exe.config and run the following. Note that SparkCLR jar version (**1.4.1**) should be aligned with Apache Spark version.  

```sparkclr-submit.cmd --verbose D:\SparkCLRHome\lib\spark-clr-1.4.1-SNAPSHOT.jar D:\SparkCLRHome\SparkCLRSamples.exe spark.local.dir D:\temp\SparkCLRTemp sparkclr.sampledata.loc D:\SparkCLRHome\data```   

Setting spark.local.dir parameter is optional and it is useful if local setup of Spark uses %TEMP% directory in windows to which adding SparkCLR driver exe file may cause problems (AV programs might automatically delete executables placed in these directories)

### Running in Standalone cluster mode
```sparkclr-submit.cmd --verbose D:\SparkCLRHome\lib\spark-clr-1.4.1-SNAPSHOT.jar D:\SparkCLRHome\SparkCLRSamples.exe sparkclr.sampledata.loc hdfs://path/to/sparkclr/sampledata```

### Running in YARN mode
To be added

## Running Unit Tests
* In Visual Studio: "Test" -> "Run" -> "All Tests"
* In Developer Command Prompt for VS, navigate to SparkCLR\csharp and run the following command  
```test.cmd```

## Debugging Tips
CSharpBackend and C# driver are separately launched for debugging SparkCLR Adapter or driver
For example, to debug SparkCLR samples  
* Launch CSharpBackend using ```sparkclr-submit.cmd debug``` and get the port number displayed in the console  
* Navigate to csharp/Samples/Microsoft.Spark.CSharp and edit App.Config to use the port number from the previous step for CSharpBackendPortNumber config and also set CSharpWorkerPath config  
* Run SparkCLRSamples.exe in Visual Studio

## License
SparkCLR is licensed under the MIT license. See LICENSE file in the project root for full license information.
