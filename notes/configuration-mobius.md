# Mobius Configuration
|Type |Property Name |Usage |
|-----|-----|-----|
|Worker  |spark.mobius.CSharpWorker.maxProcessCount  |Sets max number of C# worker processes in Spark executors |
|Streaming (Kafka)  |spark.mobius.streaming.kafka.CSharpReader.enabled  |Enables use of C# Kafka reader in Mobius streaming applications |
|Streaming (Kafka)  |spark.mobius.streaming.kafka.maxMessagesPerTask.&lt;topicName&gt;  |Sets the max number of messages per RDD partition created from specified Kafka topic to uniformly spread load across tasks that process them  |
|Streaming (Checkpoint)  |spark.mobius.streaming.localCheckpoint.enabled  | Enables `CSharpStateDStream` to use  `RDD.localCheckpoint()`  instead of `RDD.checkpoint()` for checkpointing  |
|Streaming (Checkpoint)  |spark.mobius.streaming.localCheckpoint.replicas  | When `spark.mobius.streaming.localCheckpoint.enabled` is set to `true`, specify the number of replicas for each block |