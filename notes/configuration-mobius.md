# Mobius Configuration
|Type |Property Name |Usage |
|-----|-----|-----|
|Worker  |spark.mobius.CSharpWorker.maxProcessCount  |Sets max number of C# worker processes in Spark executors |
|Streaming (Kafka)  |spark.mobius.streaming.kafka.CSharpReader.enabled  |Enables use of C# Kafka reader in Mobius streaming applications |
|Streaming (Kafka)  |spark.mobius.streaming.kafka.maxMessagesPerTask.&lt;topicName&gt;  |Sets the max number of messages per RDD partition created from specified Kafka topic to uniformly spread load across tasks that process them  |
|Streaming (Kafka)  |spark.mobius.streaming.kafka.fetchRate  |Set the number of Kafka metadata fetch operation per batch |
|Streaming (Kafka)  |spark.mobius.streaming.kafka.numReceivers  |Set the number of threads used to materialize the RDD created by applying the user read function to the original KafkaRDD. |
|Streaming (UpdateStateByKey)  |spark.mobius.streaming.parallelJobs  |Sets 0-based max number of parallel jobs for UpdateStateByKey so that next N batches can start its tasks on time even if previous batch not completed yet. default: 0, recommended: 1. It's a special version of spark.streaming.concurrentJobs which does not observe UpdateStateByKey's state ordering properly  |


