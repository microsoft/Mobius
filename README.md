# SparkCLR
SparkCLR adds C# language binding to Apache Spark enabling the implementation of Spark driver code and data processing operations in C#. 
For example, the word count sample in Apache Spark can be implemented in C# as follows
```
	var lines = sparkContext.TextFile(@"hdfs://path/to/input.txt");
	var words = lines.FlatMap(s => s.Split(new[] { " " }, StringSplitOptions.None));
	var wordCounts = words.Map(w => new KeyValuePair<string, int>(w.Trim(), 1))
                                    .ReduceByKey((x, y) => x + y);
	var wordCountCollection = wordCounts.Collect();
	wordCounts.SaveAsTextFile(@"hdfs://path/to/wordcount.txt");
```

A simple DataFrame application using TempTable may look like the following
```
	var requestsDataFrame = sqlContext.TextFile(@"hdfs://path/to/requests.csv");
	var metricsDateFrame = sqlContext.TextFile(@"hdfs://path/to/metrics.csv"); 
	requestsDataFrame.RegisterTempTable("requests");
	metricsDateFrame.RegisterTempTable("metrics");
	//C0 - guid in requests DF, C3 - guid in metrics DF
	var join = GetSqlContext().Sql(
		"SELECT joinedtable.datacenter, max(joinedtable.latency) maxlatency, avg(joinedtable.latency) avglatency " +
		"FROM (SELECT a.C1 as datacenter, b.C6 as latency from requests a JOIN metrics b ON a.C0  = b.C3) joinedtable " +
		"GROUP BY datacenter");
	join.ShowSchema();
	join.Show();
```

A simple DataFrame application using DataFrame DSL may look like the following
```
	//C0 - guid, C1 - datacenter
	var requestsDataFrame = sqlContext.TextFile(@"hdfs://path/to/requests.csv")
					.Select("C0", "C1");
	//C3 - guid, C6 - latency
	var metricsDateFrame = sqlContext.TextFile(@"hdfs://path/to/metrics.csv", ",", false, true)
					.Select("C3", "C6"); //override delimiter, hasHeader & inferSchema
	var joinDataFrame = requestsDataFrame.Join(metricsDateFrame, requestsDataFrame["C0"] == metricsDateFrame["C3"])
					.GroupBy("C1");
	var maxLatencyByDcDataFrame = joinDataFrame.Agg(new Dictionary<string, string> { { "C6", "max" } });
	maxLatencyByDcDataFrame.ShowSchema();
	maxLatencyByDcDataFrame.Show();
```
Refer to SparkCLR\csharp\Samples folder for complete samples

## Building SparkCLR
### Prerequisites
* Maven for spark-clr project implemented in scala
* MSBuild for C# projects

### Instructions
* Navigate to SparkCLR\scala folder and run ```mvn package```. This will build spark-clr*.jar
* Navigate to SparkCLR\csharp folder and run the following commands to build rest of the .NET binaries
```set PATH=%PATH%;c:\Windows\Microsoft.NET\Framework\v4.0.30319``` (if MSBuild is not already in the path) 
```msbuild SparkCLR.sln```

## Running Samples
### Prerequisites
Set the following environment variables
* ```JAVA_HOME```
* ```SCALA_HOME```
* ```SPARKCLR_HOME```
* ```SPARKCSV_JARS``` (if sqlContext.TextFile method is used to create DataFrame from csv files)

Folder pointed by SPARKCLR_HOME should have the following folders and files
* lib (spark-clr*.jar)
* bin (Microsoft.Spark.CSharp.Adapter.dll, CSharpWorker.exe)
* scripts (sparkclr-submit.cmd)
* samples (SparkCLRSamples.exe, Microsoft.Spark.CSharp.Adapter.dll, CSharpWorker.exe)
* data (all the data files used by samples)

### Running in Local mode
Set ```CSharpWorkerPath``` in SparkCLRSamples.exe.config and run the following

```sparkclr-submit.cmd --verbose D:\SparkCLRHome\lib\spark-clr-1.4.1-SNAPSHOT.jar D:\SparkCLRHome\SparkCLRSamples.exe spark.local.dir D:\temp\SparkCLRTemp sparkclr.sampledata.loc D:\SparkCLRHome\data```

Setting spark.local.dir parameter is optional and it is useful if local setup of Spark uses %TEMP% folder in windows to which adding SparkCLR driver exe file may cause problems (AV programs might automatically delete executables placed in these folders)

### Running in Standalone cluster mode
```sparkclr-submit.cmd --verbose D:\SparkCLRHome\lib\spark-clr-1.4.1-SNAPSHOT.jar D:\SparkCLRHome\SparkCLRSamples.exe sparkclr.sampledata.loc hdfs://path/to/sparkclr/sampledata```

### Running in YARN mode
//TODO

## Running Unit Tests

## Debugging Tips
CSharpBackend and C# driver are separately launched for debugging SparkCLR Adapter or driver
For example, to debug SparkCLR samples
* Launch CSharpBackend using ```sparkclr-submit.cmd debug``` and get the port number displayed in the console
* Navigate to csharp/Samples/Microsoft.Spark.CSharp and edit App.Config to use the port number from the previous step for CSharpBackendPortNumber config and also set CSharpWorkerPath config
* Run SparkCLRSamples.exe in Visual Studio

## License
SparkCLR is licensed under the MIT license. See LICENSE file in the project root for full license information.

