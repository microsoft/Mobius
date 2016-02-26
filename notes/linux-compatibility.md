The current implementation of Mobius requires a minor customization to be applied on Apache Spark for use in Linux. To build the customized Apache Spark, follow the steps below:

1. Download and unpack the binary package of Apache Spark 1.6.0.
2. Download and unpack the source package of Apache Spark 1.6.0, apply the diff patch [PythonWorkerFactory.scala.patch](./PythonWorkerFactory.scala.patch) on **core/src/main/scala/org/apache/spark/api/python/PythonWorkerFactory.scala**, and build Spark following the [instructions](http://spark.apache.org/docs/latest/building-spark.html).
3. Replace lib/spark-assembly\*hadoop\*.jar in the binary package with assembly/target/scala-2.10/spark-assembly\*hadoop\*.jar built in Step 2. Use/deploy this modified binary package for Spark.