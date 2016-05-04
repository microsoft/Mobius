## Pre-Requisites
The following software need to be installed and appropriate environment variables must to be set to run Mobius applications.

| |Version | Environment variables |Notes |
|---|----|-----------------------------------------------------|------|
|JDK |7u85 or 8u60 ([OpenJDK](http://www.azul.com/downloads/zulu/zulu-windows/) or [Oracle JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html)) |JAVA_HOME | After setting JAVA_HOME, run `set PATH=%PATH%;%JAVA_HOME%\bin` to add java to PATH |
|Spark |[1.5.2 or 1.6.*](http://spark.apache.org/downloads.html) | SPARK_HOME |Spark can be downloaded from Spark download website. Alternatively, if you used [`RunSamples.cmd`](../csharp/Samples/Microsoft.Spark.CSharp/samplesusage.md) to run Mobius samples, you can find `toos\spark*` directory (under [`build`](../build) directory) that can be used as SPARK_HOME  |
|winutils.exe | see [Running Hadoop on Windows](https://wiki.apache.org/hadoop/WindowsProblems) for details |HADOOP_HOME |Spark in Windows needs this utility in `%HADOOP_HOME%\bin` directory. It can be copied over from any Hadoop distribution. Alternative, if you used [`RunSamples.cmd`](../csharp/Samples/Microsoft.Spark.CSharp/samplesusage.md) to run Mobius samples, you can find `toos\winutils` directory (under [`build`](../build) directory) that can be used as HADOOP_HOME  |
|Mobius |[v1.5.200](https://github.com/Microsoft/Mobius/releases) or v1.6.100-PREVIEW-1 | SPARKCLR_HOME |If you downloaded a [Mobius release](https://github.com/Microsoft/Mobius/releases), SPARKCLR_HOME should be set to the directory named `runtime` (for example, `D:\downloads\spark-clr_2.10-1.5.200\runtime`). Alternatively, if you used [`RunSamples.cmd`](../csharp/Samples/Microsoft.Spark.CSharp/samplesusage.md) to run Mobius samples, you can find `runtime` directory (under [`build`](../build) directory) that can be used as SPARKCLR_HOME. **Note** - setting SPARKCLR_HOME is _optional_ and it is set by sparkclr-submit.cmd if not set. |

## Dependencies
Some features in Mobius depend on classes outside of Spark and Mobius. A selected set of jar files that Mobius depends on are available in Mobius release under "runtime\dependencies" folder. These jar files are used with "--jars" parameter in Mobius (that is sparkclr-submit.cmd) and they get passed to Spark (spark-submit.cmd). 

The following tables lists the Mobius features and their dependencies. The version numbers in the jar files below are just for completeness in names and a different version of the jar file may work with Mobius.

|Mobius Feature | Dependencies |
|----|-----|
|Using CSV files with DataFrame API | <ui><li>spark-csv_2.10-1.3.0.jar</li><li>commons-csv-1.1.jar</li></ui> |
|Kafka messages processing with DStream API | spark-streaming-kafka-assembly_2.10-1.6.1.jar |

Note that additional external jar files may need to be specificed as dependencies for a Mobius application depending on the Mobius features used (like EventHubs event processing or using Hive). These jars are not included in Mobius release under "dependencies" folder.

## Windows Instructions
### Local Mode
To use Mobius with Spark available locally in a machine, navigate to `%SPARKCLR_HOME%\scripts` directory and run the following command

`sparkclr-submit.cmd <spark arguments> --exe <Mobius driver name> <path to driver> <driver arguments>`

**Notes**
 * `<spark arguments>` - Standard arguments support by Apache Spark except `--class`. See [spark-submit.cmd arguments] (http://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit) for details
 * `<Mobius driver name>` - name of the C# application that implement Mobius driver
 * `<path to driver>` - directory contains driver executable and all its dependencies
 * `<driver arguments>` - command line arguments to driver executable

**Sample Commands**
 * `sparkclr-submit.cmd` `--total-executor-cores 2` `--exe SparkClrPi.exe C:\Git\Mobius\examples\Pi\bin\Debug`
 * `sparkclr-submit.cmd` `--conf spark.local.dir=C:\sparktemp` `--exe SparkClrPi.exe C:\Git\Mobius\examples\Pi\bin\Debug`
 * `sparkclr-submit.cmd` `--jars c:\dependency\some.jar` `--exe SparkClrPi.exe C:\Git\Mobius\examples\Pi\bin\Debug`

### Debug Mode
Debug mode is used to step through the C# code in Visual Studio during a debugging session. With this mode, driver-side operations can be debugged. 

1. Navigate to `%SPARKCLR_HOME%\scripts` directory and run `sparkclr-submit.cmd debug`
2. Look for the message in the console output that looks like "Port number used by CSharpBackend is <portnumber>". Note down the port number and use it in the next step
3. Add the following XML snippet to App.Config in the Visual Studio project for Mobius application that you want to debug and start debugging
```
<appSettings>
    <add key="CSharpWorkerPath" value="/path/to/CSharpWorker.exe"/>
    <add key="CSharpBackendPortNumber" value="port_number_from_previous_step"/>
</appSettings>
```

**Notes**
* `CSharpWorkerPath` - the folder containing CSharpWorker.exe should also contain Microsoft.Spark.CSharp.Adapter.dll, executable that has the Mobius driver application and any dependent binaries. Typically, the path to CSharpWorker.exe in the build output directory of Mobius application is used for this configuration value
* If a jar file is required by Spark (for example, spark-xml_2.10-0.3.1.jar to process XML files) then the local path to the jar file must set using the command `set SPARKCLR_DEBUGMODE_EXT_JARS=C:\ext\spark-xml\spark-xml_2.10-0.3.1.jar` before launching CSharpBackend in step #1

### Standalone Cluster
#### Client Mode
Mobius `runtime` folder and the build output of Mobius driver application must be copied over to the machine where you submit Mobius apps to a Spark Standalone cluster. Once copying is done, instructions are same as that of [localmode](running-mobius-app.md#local-mode) but specifying master URL (`--master <spark://host:port>`) is required in addition.

**Sample Commands**
 * `sparkclr-submit.cmd` `--master spark://93.184.216.34:7077` `--total-executor-cores 2` `--exe SparkClrPi.exe C:\Git\Mobius\examples\Pi\bin\Debug`
 * `sparkclr-submit.cmd` `--master spark://93.184.216.34:7077` `--conf spark.local.dir=C:\sparktemp` `--exe SparkClrPi.exe C:\Git\Mobius\examples\Pi\bin\Debug`
 * `sparkclr-submit.cmd` `--master spark://93.184.216.34:7077` `--jars c:\dependency\some.jar` `--exe SparkClrPi.exe C:\Git\Mobius\examples\Pi\bin\Debug`

#### Cluster Mode
To submit Mobius app in Cluster mode, both spark-clr*.jar and app binaries need be made available in HDFS. Let's say `Pi.zip` includes all files under `Pi\bin\[debug|release]`:
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
Mobius `runtime` folder and the build output of Mobius application must be copied over to the machine where you submit Mobius application to a YARN cluster, also make sure following enviroment variables are set properly before submission.

* `HADOOP_HOME`
* `HADOOP_CONF_DIR`
* `YARN_CONF_DIR`
* `SPARK_HOME`
* `SPARK_DIST_CLASSPATH`

#### Client Mode
Sample command
````
sparkclr-submit.cmd ^
    --master yarn ^
    --deploy-mode client ^
    --num-executors 10 ^
    --executor-cores 2 ^
    --executor-memory 1G ^
    --conf spark.speculation=true ^
    --driver-memory 1G ^
    --exe SparkClrPi.exe ^
    C:\Git\Mobius\examples\Pi\bin\Debug
````

#### Cluster Mode
Sample command
````
sparkclr-submit.cmd ^
    --master yarn ^
    --deploy-mode cluster ^
    --num-executors 10 ^
    --executor-cores 2 ^
    --executor-memory 1G ^
    --conf spark.speculation=true ^
    --driver-memory 1G ^
    --exe SparkClrPi.exe ^
    C:\Git\Mobius\examples\Pi\bin\Debug
````

## Linux Instructions
The instructions above cover running Mobius applications in Windows. With the following tweaks, the same instructions can be used to run Mobius applications in Linux.
* Instead of `RunSamples.cmd`, use `run-samples.sh`
* Instead of `sparkclr-submit.cmd`, use `sparkclr-submit.sh`

## Running Mobius Examples in Local Mode
| Type          | Examples      |
| ------------- |--------------|
| Batch | <ul><li>[Pi](#pi-example-batch)</li><li>[Word Count](#wordcount-example-batch)</li></ul> |
| SQL | <ul><li>[JDBC](#jdbc-example-sql)</li><li>[Spark-XML](#spark-xml-example-sql)</li><li>[Hive](#hive-example-sql)</li></ul> |
| Streaming | <ul><li>[Kafka](#kafka-example-streaming)</li><li>[EventHubs](#eventhubs-example-streaming)</li><li>[HDFS Word Count](#hdfswordcount-example-streaming)</li></ul> |

The following sample commands show how to run Mobius examples in local mode. Using the instruction above, the following sample commands can be tweaked to run in other modes

### Pi Example (Batch)
* Run `sparkclr-submit.cmd --exe SparkClrPi.exe C:\Git\Mobius\examples\Batch\Pi\bin\Debug`

Computes the _approximate_ value of Pi using two appropaches and displays the value.

### WordCount Example (Batch)
* Run `sparkclr-submit.cmd --exe SparkClrPi.exe C:\Git\Mobius\examples\Batch\WordCount\bin\Debug <inputFile>`

### JDBC Example (Sql)
* Download a JDBC driver for the SQL Database you want to use
* `sparkclr-submit.cmd --jars C:\MobiusDependencies\sqljdbc4.jar --exe SparkClrJdbc.exe C:\Git\Mobius\examples\Sql\JdbcDataFrame\bin\Debug <jdbc connection string> <table name>`

The schema and row count of the table name provided as the commandline argument to SparkClrJdbc.exe is displayed.

### Spark-XML Example (Sql)
* Download [books.xml](https://github.com/databricks/spark-xml/blob/master/src/test/resources/books.xml) and the location of this file is the first argument to SparkClrXml.exe below
* 
`sparkclr-submit.cmd --jars C:\MobiusDependencies\spark-xml_2.10-0.3.1.jar --exe SparkClrXml.exe C:\Git\Mobius\examples\Sql\SparkXml\bin\Debug C:\MobiusData\books.xml C:\MobiusData\booksModified.xml`

Displays the number of XML elements in the input XML file provided as the first argument to SparkClrXml.exe and writes the modified XML to the file specified in the second commandline argument.

### Hive Example (Sql)
* 
`sparkclr-submit.cmd --jars <jar files used for using Hive in Spark> --exe HiveDataFrame.exe C:\Git\Mobius\examples\Sql\HiveDataFrame\bin\Debug`

Reads data from a csv file, creates a Hive table and reads data from it
### EventHubs Example (Streaming)
* Get the following jar files
  * qpid-amqp-1-0-client-0.32.jar
  * qpid-amqp-1-0-common-0.32.jar
  * eventhubs-client-0.9.1.jar
  * spark-streaming-eventhubs_2.10-0.1.0.jar
* Publish sample events to EventHubs to be used in this example using EventPublisher class (remember to update packages.config, connection parameters to EventHub and uncomment commented out statements to build and use EventPublisher)
* Update EventHubs connection parameters in SparkCLREventHubsExample implementation and build
* `sparkclr-submit.cmd --master local[4] --conf spark.local.dir=d:\temp --jars C:\MobiusDependencies\spark-streaming-eventhubs_2.10-0.1.0.jar,C:\MobiusDependencies\eventhubs-client-0.9.1.jar,C:\MobiusDependencies\qpid-amqp-1-0-client-0.32.jar,C:\MobiusDependencies\qpid-amqp-1-0-common-0.32.jar --exe SparkCLREventHub.exe C:\Git\Mobius\examples\Streaming\EventHub\bin\Debug`

This example aggregates events published to EventHub in the format [timestamp],[loglevel],[logmessage] by time and log-level and prints the count of events per window.

Note that all the dependencies listed above are available in maven that can be downloaded. [spark-streaming-eventhubs*.jar](https://github.com/hdinsight/spark-eventhubs) is not yet updated to support Spark version beyond 1.3.1 or published to Maven. A fork of this repo is available with preview releases at https://github.com/SparkCLR/spark-eventhubs/releases and the jar file can be downloaded from this location. 

### HdfsWordCount Example (Streaming)
* Remove `<checkpoint directory>` (used in next step) if it already exists.
* Run `sparkclr-submit.cmd --exe SparkClrHdfsWordCount.exe C:\Git\Mobius\examples\Streaming\HdfsWordCount\bin\Debug <checkpoint directory> <input directory>`

Counts words in new text files created in the given directory using Mobius streaming.

### Kafka Example (Streaming)
* Publish sample messages to Kafka to be used in this example using MessagePublisher class (remember to include reference to KafkaNet library (https://github.com/Microsoft/CSharpClient-for-Kafka), connection parameters to Kafka and uncomment commented out statements to build and use MessagePublisher)
* Update Kafka parameters in SparkClrKafkaExample implementation and build
* `sparkclr-submit.cmd --master local[4] --conf spark.local.dir=d:\temp --exe SparkClrKafka.exe C:\Git\Mobius\examples\Streaming\Kafka\bin\Debug`
