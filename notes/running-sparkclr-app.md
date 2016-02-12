## Pre-Requisites
The following software need to be installed and appropriate environment variables must to be set to run SparkCLR applications.

| |Version | Environment variables |Notes |
|---|----|-----------------------------------------------------|------|
|JDK |7u85 or 8u60 ([OpenJDK](http://www.azul.com/downloads/zulu/zulu-windows/) or [Oracle JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html)) |JAVA_HOME | After setting JAVA_HOME, run `set PATH=%PATH%;%JAVA_HOME%\bin` to add java to PATH |
|Spark |[1.5.2 or 1.6.0](http://spark.apache.org/downloads.html) | SPARK_HOME |Spark can be downloaded from Spark download website. Alternatively, if you used [`RunSamples.cmd`](../csharp/Samples/Microsoft.Spark.CSharp/samplesusage.md) to run SparkCLR samples, you can find `toos\spark*` directory (under [`build`](../build) directory) that can be used as SPARK_HOME  |
|winutils.exe | see [Running Hadoop on Windows](https://wiki.apache.org/hadoop/WindowsProblems) for details |HADOOP_HOME |Spark in Windows needs this utility in `%HADOOP_HOME%\bin` directory. It can be copied over from any Hadoop distribution. Alternative, if you used [`RunSamples.cmd`](../csharp/Samples/Microsoft.Spark.CSharp/samplesusage.md) to run SparkCLR samples, you can find `toos\winutils` directory (under [`build`](../build) directory) that can be used as HADOOP_HOME  |
|SparkCLR |[v1.5.200](https://github.com/Microsoft/SparkCLR/releases) or v1.6.000-SNAPSHOT | SPARKCLR_HOME |If you downloaded a [SparkCLR release](https://github.com/Microsoft/SparkCLR/releases), SPARKCLR_HOME should be set to the directory named `runtime` (for example, `D:\downloads\spark-clr_2.10-1.5.200\runtime`). Alternatively, if you used [`RunSamples.cmd`](../csharp/Samples/Microsoft.Spark.CSharp/samplesusage.md) to run SparkCLR samples, you can find `runtime` directory (under [`build`](../build) directory) that can be used as SPARKCLR_HOME. **Note** - setting SPARKCLR_HOME is _optional_ and it is set by sparkclr-submit.cmd if not set. |

## Windows Instructions
### Local Mode
To use SparkCLR with Spark available locally in a machine, navigate to `%SPARKCLR_HOME%\scripts` directory and run the following command

`sparkclr-submit.cmd <spark arguments> --exe <SparkCLR driver name> <path to driver> <driver arguments>`

**Notes**
 * `<spark arguments>` - Standard arguments support by Apache Spark except `--class`. See [spark-submit.cmd arguments] (http://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit) for details
 * `<SparkCLR driver name>` - name of the C# application that implement SparkCLR driver
 * `<path to driver>` - directory contains driver executable and all its dependencies
 * `<driver arguments>` - command line arguments to driver executable

**Sample Commands**
 * `sparkclr-submit.cmd` `--total-executor-cores 2` `--exe SparkClrPi.exe C:\Git\SparkCLR\examples\Pi\bin\Debug`
 * `sparkclr-submit.cmd` `--conf spark.local.dir=C:\sparktemp` `--exe SparkClrPi.exe C:\Git\SparkCLR\examples\Pi\bin\Debug`
 * `sparkclr-submit.cmd` `--jars c:\dependency\some.jar` `--exe SparkClrPi.exe C:\Git\SparkCLR\examples\Pi\bin\Debug`

### Debug Mode
Debug mode is used to step through the C# code in Visual Studio during a debugging session. With this mode, driver-side operations can be debugged. 

1. Navigate to `%SPARKCLR_HOME%\scripts` directory and run `sparkclr-submit.cmd debug`
2. Look for the message in the console output that looks like "Port number used by CSharpBackend is <portnumber>". Note down the port number and use it in the next step
3. Add the following XML snippet to App.Config in the Visual Studio project for SparkCLR application that you want to debug and start debugging
```
<appSettings>
    <add key="CSharpWorkerPath" value="/path/to/CSharpWorker.exe"/>
    <add key="CSharpBackendPortNumber" value="port_number_from_previous_step"/>
</appSettings>
```

**Notes**
* `CSharpWorkerPath` - the folder containing CSharpWorker.exe should also contain Microsoft.Spark.CSharp.Adapter.dll, executable that has the SparkCLR driver application and any dependent binaries. Typically, the path to CSharpWorker.exe in the build output directory of SparkCLR application is used for this configuration value
* If a jar file is required by Spark (for example, spark-xml_2.10-0.3.1.jar to process XML files) then the local path to the jar file must set using the command `set SPARKCLR_DEBUGMODE_EXT_JARS=C:\ext\spark-xml\spark-xml_2.10-0.3.1.jar` before launching CSharpBackend in step #1

### Standalone Cluster
#### Client Mode
SparkCLR `runtime` folder and the build output of SparkCLR driver application must be copied over to the machine where you submit SparkCLR apps to a Spark Standalone cluster. Once copying is done, instructions are same as that of [localmode](RunningSparkCLRApp.md#local-mode) but specifying master URL (`--master <spark://host:port>`) is required in addition.

**Sample Commands**
 * `sparkclr-submit.cmd` `--master spark://93.184.216.34:7077` `--total-executor-cores 2` `--exe SparkClrPi.exe C:\Git\SparkCLR\examples\Pi\bin\Debug`
 * `sparkclr-submit.cmd` `--master spark://93.184.216.34:7077` `--conf spark.local.dir=C:\sparktemp` `--exe SparkClrPi.exe C:\Git\SparkCLR\examples\Pi\bin\Debug`
 * `sparkclr-submit.cmd` `--master spark://93.184.216.34:7077` `--jars c:\dependency\some.jar` `--exe SparkClrPi.exe C:\Git\SparkCLR\examples\Pi\bin\Debug`

#### Cluster Mode
To submit SparkCLR app in Cluster mode, both spark-clr*.jar and app binaries need be made available in HDFS. Let's say `Pi.zip` includes all files under `Pi\bin\[debug|release]`:
````
hdfs dfs -copyFromLocal \path\to\pi.zip hdfs://path/to/pi
hdfs dfs -copyFromLocal \path\to\runtime\lib\spark-clr*.jar hdfs://path/to/spark-clr-jar

cd \path\to\runtime
scripts\sparkclr-submit.cmd ^
    --total-executor-cores 2 ^
    --deploy-mode cluster ^
    --master <spark://host:port> ^
    --remote-sparkclr-jar hdfs://path/to/spark-clr-jar/spark-clr_2.10-1.5.200.jar ^
    --exe Pi.exe ^
    hdfs://path/to/pi/pi.zip ^
    spark.local.dir <full-path to temp directory on any spark worker>
````

### YARN Cluster
#### Client Mode
To be added

#### Cluster Mode
To be added

## Linux Instructions
The instructions above cover running SparkCLR applications in Windows. With the following tweaks, the same instructions can be used to run SparkCLR applications in Linux.
* Instead of `RunSamples.cmd`, use `run-samples.sh`
* Instead of `sparkclr-submit.cmd`, use `sparkclr-submit.sh`

## Running Examples in Local Mode
The following sample commands show how to run SparkCLR examples in local mode. Using the instruction above, the following sample commands can be tweaked to run in other modes

### Pi Example
* Run `sparkclr-submit.cmd --exe SparkClrPi.exe C:\Git\SparkCLR\examples\Pi\bin\Debug`

Computes the _approximate_ value of Pi using two appropaches and displays the value.

### JDBC Example
* Download a JDBC driver for the SQL Database you want to use
* `sparkclr-submit.cmd --jars C:\SparkCLRDependencies\sqljdbc4.jar --exe SparkClrJdbc.exe C:\Git\SparkCLR\examples\JdbcDataFrame\bin\Debug <jdbc connection string> <table name>`

The schema and row count of the table name provided as the commandline argument to SparkClrJdbc.exe is displayed.

### Spark-XML Example
* Download [books.xml](https://github.com/databricks/spark-xml/blob/master/src/test/resources/books.xml) and the location of this file is the first argument to SparkClrXml.exe below
* 
`sparkclr-submit.cmd --jars C:\SparkCLRDependencies\spark-xml_2.10-0.3.1.jar --exe SparkClrXml.exe C:\Git\SparkCLR\examples\SparkXml\bin\Debug C:\SparkCLRData\books.xml C:\SparkCLRData\booksModified.xml`

Displays the number of XML elements in the input XML file provided as the first argument to SparkClrXml.exe and writes the modified XML to the file specified in the second commandline argument.


### EventHubs Example
* Get the following jar files
  * qpid-amqp-1-0-client-0.32.jar
  * qpid-amqp-1-0-common-0.32.jar
  * eventhubs-client-0.9.1.jar
  * spark-streaming-eventhubs_2.10-0.1.0.jar
* Publish sample events to EventHubs to be used in this example using EventPublisher class (remember to update packages.config, connection parameters to EventHub and uncomment commented out statements to build and use EventPublisher)
* Update EventHubs connection parameters in SparkCLREventHubsExample implementation and build
* `sparkclr-submit.cmd --master local[4] --conf spark.local.dir=d:\temp --jars C:\SparkCLRDependencies\spark-streaming-eventhubs_2.10-0.1.0.jar,C:\SparkCLRDependencies\eventhubs-client-0.9.1.jar,C:\SparkCLRDependencies\qpid-amqp-1-0-client-0.32.jar,C:\SparkCLRDependencies\qpid-amqp-1-0-common-0.32.jar --exe SparkCLREventHub.exe C:\Git\SparkCLR\examples\EventHub\bin\Debug`

This example aggregates events published to EventHub in the format [timestamp],[loglevel],[logmessage] by time and log-level and prints the count of events per window.

Note that all the dependencies listed above are available in maven that can be downloaded. [spark-streaming-eventhubs*.jar](https://github.com/hdinsight/spark-eventhubs) is not yet updated to support Spark version beyond 1.3.1 or published to Maven. A fork of this repo is available with preview releases at https://github.com/SparkCLR/spark-eventhubs/releases and the jar file can be downloaded from this location. 
 
