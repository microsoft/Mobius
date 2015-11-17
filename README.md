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

Refer to [SparkCLR\csharp\Samples](csharp/Samples) directory for complete samples.

## Documents

Refer to the [docs folder](docs).

## Building SparkCLR

[![Build Status](https://ci.appveyor.com/api/projects/status/github/Microsoft/SparkCLR?svg=true&branch=master)](https://ci.appveyor.com/project/skaarthik/SparkCLR/branch/master)

### Prerequisites

* Windows Server 2012 or above; or, 64-bit Windows 7 or above.
* Developer Command Prompt for [Visual Studio](https://www.visualstudio.com/) 2013 or above, which comes with .NET Framework 4.5 or above. Note: [Visual Studio 2015 Community. Edition](https://www.visualstudio.com/en-us/products/visual-studio-community-vs.aspx) is **FREE**.
* 64-bit JDK 7u85 or above; or, 64-bit JDK 8u60 or above. OpenJDK for Windows can be downloaded from [http://www.azul.com/downloads/zulu/zulu-windows/](http://www.azul.com/downloads/zulu/zulu-windows/); Oracle JDK8 for Windows is available at Orcale website.

JDK should be downloaded manually, and the following environment variables should be set properly in the Developer Command Prompt for Visual Studio:

* `JAVA_HOME`


### Instructions

* In the Developer Command Prompt for Visual Studio where `JAVA_HOME` is set properly, navigate to [SparkCLR](./) directory: 

	```  
	Build.cmd  
	```

* Optional: 
	- Under [SparkCLR\scala](./scala) directory, run the following command to clean spark-clr*.jar built above: 

		```  
		mvn clean
		```  

 	- Under [SparkCLR\csharp](./csharp) directory, run the following command to clean the .NET binaries built above:

		```  
		Clean.cmd  
		```  
		
[Build.cmd](build.cmd) downloads necessary build tools; after the build is done, it prepares the folowing directories under `SparkCLR\run`:

  * **lib** ( `spark-clr*.jar` )  
  * **bin** ( `Microsoft.Spark.CSharp.Adapter.dll`, `CSharpWorker.exe`)  
  * **samples** ( The contents of `SparkCLR\csharp\Samples\Microsoft.Spark.CSharp\bin\Release\*`, including `Microsoft.Spark.CSharp.Adapter.dll`, `CSharpWorker.exe`, `SparkCLRSamples.exe`, `SparkCLRSamples.exe.Config` etc. ) 
  * **scripts** ( `sparkclr-submit.cmd` )  
  * **data** ( `SparkCLR\csharp\Samples\Microsoft.Spark.CSharp\data\*` )    

## Running Samples

### Prerequisites

JDK should be downloaded manually, and the following environment variables should be set properly in the Developer Command Prompt for Visual Studio:

* `JAVA_HOME`

### Running in Local mode

In the Developer Command Prompt for Visual Studio where `JAVA_HOME` is set properly, navigate to [SparkCLR](./) directory:

```  
Runsamples.cmd  
```

[Runsamples.cmd](./Runsamples.cmd) downloads Apache Spark 1.4.1, sets up `SPARK_HOME` environment variable, points `SPARKCLR_HOME` to `SparkCLR\run` directory created by [Build.cmd](./build.cmd), and invokes [sparkclr-submit.cmd](./scripts/sparkclr-submit.cmd), with `spark.local.dir` set to `SparkCLR\run\Temp`.

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
