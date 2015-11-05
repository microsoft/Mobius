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
A simple DataFrame application using DataFrame DSL may look like the following
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
Refer to `SparkCLR\csharp\Samples` directory for complete samples

## Documents
Refer to the docs @ https://github.com/Microsoft/SparkCLR/tree/master/docs

## Building SparkCLR
### Prerequisites
* [Apache Maven](http://maven.apache.org) for spark-clr project implemented in scala
* MSBuild in Visual Studio 2013 and above
* .NET Framework 4.5 and above
* [Nuget command-line utility](https://docs.nuget.org/release-notes) 3.2 and above

### Instructions
* Navigate to `SparkCLR\scala` directory and run the following command to build spark-clr*.jar   
        ```
        mvn package
        ```
* Start Developer Command Prompt for Visual Studio, navigate to `SparkCLR\csharp` directory, run the following commands to add `nuget.exe` to the path  
        ```  
        set PATH=<fullpath to nuget.exe>;%PATH%  
        ```  
        And build the rest of .Net binaries  
        ```  
        build.cmd  
        ```  
* Optional. Under `SparkCLR\csharp` directory, run the following command to clean the .NET binaries built above  
        ```
        clean.cmd
        ```   

## Running Samples
### Prerequisites
DataFrame TextFile API uses `spark-csv` package to load data from CSV file. Latest [commons-csv-*.jar](http://commons.apache.org/proper/commons-csv/download_csv.cgi) and [spark-csv*.jar (Scala version:2.10)](http://spark-packages.org/package/databricks/spark-csv) should be downloaded manually.

The following environment variables should be set properly:
* ```JAVA_HOME```  
* ```SCALA_HOME```  
* ```SPARKCSV_JARS``` should include fullpaths to `commons-csv*.jar` and `spark-csv*.jar`. For example:     
    ```
    set SPARKCSV_JARS=%SPARKCLR_HOME%\lib\commons-csv-1.2.jar;%SPARKCLR_HOME%\lib\spark-csv_2.10-1.2.0.jar
    ```
* ```SPARKCLR_HOME``` should point to a directory prapared with following subdirectories:  
  * **lib** (`spark-clr*.jar`)  
  * **bin** (`SparkCLR\csharp\Samples\Microsft.Spark.CSharp\bin\[Debug|Release]\*`, including `Microsoft.Spark.CSharp.Adapter.dll`, `CSharpWorker.exe`, `SparkCLRSamples.exe`, `SparkCLRSamples.exe.Config` and etc.)  
  * **scripts** (`sparkclr-submit.cmd`)  
  * **data** (`SparkCLR\csharp\Samples\Microsoft.Spark.CSharp\data\*`)  

### Running in Local mode
Set `CSharpWorkerPath` in `SparkCLRSamples.exe.config` and run the following. Note that SparkCLR jar version (**1.4.1**) should be aligned with Apache Spark version.  
```
sparkclr-submit.cmd --verbose %SPARKCLR_HOME%\lib\spark-clr-1.4.1-SNAPSHOT.jar %SPARKCLR_HOME%\bin\SparkCLRSamples.exe spark.local.dir C:\temp\SparkCLRTemp sparkclr.sampledata.loc %SPARKCLR_HOME%\data
```   

Setting `spark.local.dir` parameter is important. When local Spark instance distributes SparkCLR driver executables to Windows `%TEMP%` directory, anti-virus software may detect and report the executables showed up in `%TEMP%` directory as malware.

### Running in Standalone cluster mode
```
sparkclr-submit.cmd --verbose  %SPARKCLR_HOME%\lib\spark-clr-1.4.1-SNAPSHOT.jar  %SPARKCLR_HOME%\bin\SparkCLRSamples.exe sparkclr.sampledata.loc hdfs://path/to/sparkclr/sampledata
```

### Running in YARN mode
To be added

## Running Unit Tests
* In Visual Studio: "Test" -> "Run" -> "All Tests"
* In Developer Command Prompt for VS, navigate to `SparkCLR\csharp` and run the following command  
        ```
        test.cmd
        ```

## Debugging Tips
CSharpBackend and C# driver are separately launched for debugging SparkCLR Adapter or driver
For example, to debug SparkCLR samples  
* Launch CSharpBackend using ```sparkclr-submit.cmd debug``` and get the port number displayed in the console  
* Navigate to `csharp/Samples/Microsoft.Spark.CSharp` and edit `App.Config` to use the port number from the previous step for CSharpBackendPortNumber config and also set CSharpWorkerPath config  
* Run `SparkCLRSamples.exe` in Visual Studio

## License
SparkCLR is licensed under the MIT license. See LICENSE file in the project root for full license information.
