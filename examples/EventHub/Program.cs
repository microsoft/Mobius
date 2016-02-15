// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// Sample SparkCLR application that processes events from EventHub in the format [timestamp],[loglevel],[logmessage]
    /// EventPublisher class may be used to publish sample events to consume
    /// </summary>
    class SparkCLREventHubsExample
    {
        static void Main(string[] args)
        {
            var sparkContext = new SparkContext(new SparkConf().SetAppName("SparkCLREventHub Example"));
            var eventhubsParams = new Dictionary<string, string>()
            {
                {"eventhubs.policyname", "<policyname>"},
                {"eventhubs.policykey", "<policykey>"},
                {"eventhubs.namespace", "<namespace>"},
                {"eventhubs.name", "<name>"},
                {"eventhubs.partition.count", "<partitioncount>"},
                {"eventhubs.consumergroup", "$default"},
                {"eventhubs.checkpoint.dir", "<hdfs path to eventhub checkpoint dir>"},
                {"eventhubs.checkpoint.interval", "<interval>"},
            };

            const string checkpointPath = "<hdfs path to spark checkpoint dir>";
            //const string outputPath = "<hdfs path to output dir>";

            const long slideDuration = 5000;
            StreamingContext sparkStreamingContext = StreamingContext.GetOrCreate(checkpointPath,
                () =>
                {
                    var ssc = new StreamingContext(sparkContext, slideDuration);
                    ssc.Checkpoint(checkpointPath);

                    var stream = EventHubsUtils.CreateUnionStream(ssc, eventhubsParams);
                    var countByLogLevelAndTime = stream
                                                    .Map(bytes => Encoding.UTF8.GetString(bytes))
                                                    .Filter(s => s.Contains(","))
                                                    .Map(line => line.Split(','))
                                                    .Map(columns => new KeyValuePair<string, int>(string.Format("{0},{1}", columns[0], columns[1]), 1))
                                                    .ReduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, 5, 5, 3)
                                                    .Map(kvp => string.Format("{0},{1}", kvp.Key, kvp.Value));
                    
                    countByLogLevelAndTime.ForeachRDD(dimensionalCount =>
                    {
                        //dimensionalCount.SaveAsTextFile(string.Format("{0}/{1}", outputPath, Guid.NewGuid()));
                        var dimensionalCountCollection = dimensionalCount.Collect();
                        foreach (var dimensionalCountItem in dimensionalCountCollection)
                        {
                            Console.WriteLine(dimensionalCountItem);
                        }
                    });

                    return ssc;
                });

            sparkStreamingContext.Start();
            sparkStreamingContext.AwaitTermination();
        }
    }
}