// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;

using Microsoft.Spark.CSharp.Streaming;
using Microsoft.Spark.CSharp.Samples;

using NUnit.Framework;

namespace Microsoft.Spark.CSharp
{
    class DStreamSamples
    {
        private static int count;
        private static bool stopFileServer;
        private static void StartFileServer(StreamingContext ssc, string directory, string pattern, int loops = 1)
        {
            string testDir = Path.Combine(directory, "test");
            if (!Directory.Exists(testDir))
                Directory.CreateDirectory(testDir);

            stopFileServer = false;

            string[] files = Directory.GetFiles(directory, pattern);

            Task.Run(() =>
            {
                int loop = 0;
                while (!stopFileServer)
                {
                    if (loop++ < loops)
                    {
                        DateTime now = DateTime.Now;
                        foreach (string path in files)
                        {
                            string text = File.ReadAllText(path);
                            File.WriteAllText(testDir + "\\" + now.ToBinary() + "_" + Path.GetFileName(path), text);
                        }
                    }
                    System.Threading.Thread.Sleep(200);
                }

                ssc.Stop();
            });
            
            System.Threading.Thread.Sleep(1);
        }

        [Sample("experimental")]
        internal static void DStreamTextFileSample()
        {
            count = 0;

            string directory = SparkCLRSamples.Configuration.SampleDataLocation;
            string checkpointPath = Path.Combine(directory, "checkpoint");

            SparkContext sc = SparkCLRSamples.SparkContext;
            var b = sc.Broadcast<int>(0);

            StreamingContext ssc = StreamingContext.GetOrCreate(checkpointPath,
                () =>
                {

                    StreamingContext context = new StreamingContext(sc, 2000L); // batch interval is in milliseconds
                    context.Checkpoint(checkpointPath);

                    var lines = context.TextFileStream(Path.Combine(directory, "test"));
                    lines = context.Union(lines, lines);
                    var words = lines.FlatMap(l => l.Split(' '));
                    var pairs = words.Map(w => new Tuple<string, int>(w, 1));

                    // since operations like ReduceByKey, Join and UpdateStateByKey are
                    // separate dstream transformations defined in CSharpDStream.scala
                    // an extra CSharpRDD is introduced in between these operations
                    var wordCounts = pairs.ReduceByKey((x, y) => x + y);
                    var join = wordCounts.Window(2, 2).Join(wordCounts, 2);
                    var initialStateRdd = sc.Parallelize( new[] {new Tuple<string, int>("AAA", 88), new Tuple<string, int>("BBB", 88)});
                    var state = join.UpdateStateByKey(new UpdateStateHelper(b).Execute, initialStateRdd);

                    state.ForeachRDD((time, rdd) =>
                    {
                        // there's chance rdd.Take conflicts with ssc.Stop
                        if (stopFileServer)
                            return;

                        object[] taken = rdd.Take(10);
                        Console.WriteLine("-------------------------------------------");
                        Console.WriteLine("Time: {0}", time);
                        Console.WriteLine("-------------------------------------------");
                        foreach (object record in taken)
                        {
                            Console.WriteLine(record);
                            
                            var countByWord = (Tuple<string, int>)record;
                            Assert.AreEqual(countByWord.Item2, countByWord.Item1 == "The" || countByWord.Item1 == "lazy" || countByWord.Item1 == "dog" ? 92 : 88);
                        }
                        Console.WriteLine();

                        stopFileServer = true;
                    });

                    return context;
                });

            StartFileServer(ssc, directory, "words.txt");

            ssc.Start();

            ssc.AwaitTermination();
        }

        private static string brokers = ConfigurationManager.AppSettings["KafkaTestBrokers"] ?? "127.0.0.1:9092";
        private static string topic = ConfigurationManager.AppSettings["KafkaTestTopic"] ?? "test";
        // expected partitions
        private static int partitions = int.Parse(ConfigurationManager.AppSettings["KafkaTestPartitions"] ?? "10");
        // total message count
        private static uint  messages = uint.Parse(ConfigurationManager.AppSettings["KafkaMessageCount"] ?? "100");

        /// <summary>
        /// start a local kafka service and create a 'test' topic with some data before running this sample
        /// e.g. create 2 partitions with 100 messages which will be repartitioned into 10 RDD partitions
        /// TODO: automate kafka service
        /// </summary>
        [Sample("experimental")]
        internal static void DStreamDirectKafkaWithRepartitionSample()
        {
            count = 0;

            string directory = SparkCLRSamples.Configuration.SampleDataLocation;
            string checkpointPath = Path.Combine(directory, "checkpoint");

            StreamingContext ssc = StreamingContext.GetOrCreate(checkpointPath,
                () =>
                {
                    var conf = new SparkConf();
                    SparkContext sc = new SparkContext(conf);
                    StreamingContext context = new StreamingContext(sc, 2000L);
                    context.Checkpoint(checkpointPath);

                    var kafkaParams = new List<Tuple<string, string>> {
                        new Tuple<string, string>("metadata.broker.list", brokers),
                        new Tuple<string, string>("auto.offset.reset", "smallest")
                    };
                                    
                    conf.Set("spark.mobius.streaming.kafka.numPartitions." + topic, partitions.ToString());
                    var dstream = KafkaUtils.CreateDirectStream(context, new List<string> { topic }, kafkaParams, Enumerable.Empty<Tuple<string, long>>());

                    dstream.ForeachRDD((time, rdd) => 
                        {
                            long batchCount = rdd.Count();
                            int numPartitions = rdd.GetNumPartitions();

                            Console.WriteLine("-------------------------------------------");
                            Console.WriteLine("Time: {0}", time);
                            Console.WriteLine("-------------------------------------------");
                            Console.WriteLine("Count: " + batchCount);
                            Console.WriteLine("Partitions: " + numPartitions);
                            
                            // only first batch has data and is repartitioned into 10 partitions
                            if (count++ == 0)
                            {
                                Assert.AreEqual(messages, batchCount);
                                Assert.IsTrue(numPartitions >= partitions);
                            }
                            else
                            {
                                Assert.AreEqual(0, batchCount);
                                Assert.IsTrue(numPartitions == 0);
                            }
                        });

                    return context;
                });

            ssc.Start();
            ssc.AwaitTermination();
        }

        /// <summary>
        /// A sample shows that ConstantInputDStream is an input stream that always returns the same mandatory input RDD at every batch time.
        /// </summary>
        [Sample("experimental")]
        internal static void DStreamConstantDStreamSample()
        {
            var sc = SparkCLRSamples.SparkContext;
            var ssc = new StreamingContext(sc, 2000L);

            const int count = 100;
            const int partitions = 2;

            // create the RDD
            var seedRDD = sc.Parallelize(Enumerable.Range(0, 100), 2);
            var dstream = new ConstantInputDStream<int>(seedRDD, ssc);

            dstream.ForeachRDD((time, rdd) =>
            {
                long batchCount = rdd.Count();
                int numPartitions = rdd.GetNumPartitions();

                Console.WriteLine("-------------------------------------------");
                Console.WriteLine("Time: {0}", time);
                Console.WriteLine("-------------------------------------------");
                Console.WriteLine("Count: " + batchCount);
                Console.WriteLine("Partitions: " + numPartitions);
                Assert.AreEqual(count, batchCount);
                Assert.AreEqual(partitions, numPartitions);
            });

            ssc.Start();
            ssc.AwaitTermination();
        }

        /// <summary>
        /// when windowDuration not >= slideDuration * 5
        /// DStreamReduceByKeyAndWindow does winodwed reduce once
        /// </summary>
        [Sample("experimental")]
        internal static void DStreamReduceByKeyAndSmallWindowSample()
        {
            slideDuration = 6;
            DStreamReduceByKeyAndWindowSample();
        }

        /// <summary>
        /// when windowDuration >= slideDuration * 5
        /// DStreamReduceByKeyAndWindow reduces twice based on previousRDD
        /// by first invReduce on old RDDs and then reduce on new RDDs
        /// </summary>
        [Sample("experimental")]
        internal static void DStreamReduceByKeyAndLargeWindowSample()
        {
            slideDuration = 4;
            DStreamReduceByKeyAndWindowSample();
        }

        private static int slideDuration;
        private static void DStreamReduceByKeyAndWindowSample()
        {
            count = 0;

            const long bacthIntervalMs = 2000; // batch interval is in milliseconds
            const int windowDuration = 26;     // window duration in seconds
            const int numPartitions = 2;

            var sc = SparkCLRSamples.SparkContext;
            var ssc = new StreamingContext(sc, bacthIntervalMs);

            // create the RDD
            var seedRDD = sc.Parallelize(Enumerable.Range(0, 100), numPartitions);
            var numbers = new ConstantInputDStream<int>(seedRDD, ssc);
            var pairs = numbers.Map(n => new Tuple<int, int>(n % numPartitions, n));
            var reduced = pairs.ReduceByKeyAndWindow(
                    (int x, int y) => (x + y),
                    (int x, int y) => (x - y),
                    windowDuration,
                    slideDuration,
                    numPartitions
                );

            reduced.ForeachRDD((time, rdd) =>
            {
                count++;
                var taken = rdd.Collect();
                int partitions = rdd.GetNumPartitions();

                Console.WriteLine("-------------------------------------------");
                Console.WriteLine("Time: {0}", time);
                Console.WriteLine("-------------------------------------------");
                Console.WriteLine("Batch: " + count);
                Console.WriteLine("Count: " + taken.Length);
                Console.WriteLine("Partitions: " + partitions);

                Assert.AreEqual(taken.Length, 2);
                Assert.AreEqual(partitions, numPartitions);

                foreach (object record in taken)
                {
                    Tuple<int, int> sum = (Tuple<int, int>)record;
                    Console.WriteLine("Key: {0}, Value: {1}", sum.Item1, sum.Item2);
                    // when batch count reaches window size, sum of even/odd number stay at windowDuration / slideDuration * (2450, 2500) respectively
                    Assert.AreEqual(sum.Item2, (count > windowDuration / slideDuration ? windowDuration : count * slideDuration) / (bacthIntervalMs / 1000) * (sum.Item1 == 0 ? 2450 : 2500));
                }
            });

            ssc.Start();
            ssc.AwaitTermination();
        }

        [Sample("experimental")]
        internal static void DStreamCSharpInputSample()
        {
            const int numPartitions = 5;

            var sc = SparkCLRSamples.SparkContext;
            var ssc = new StreamingContext(sc, 2000L); // batch interval is in milliseconds

            var inputDStream = CSharpInputDStreamUtils.CreateStream<string>(
                ssc,
                numPartitions,
                (double time, int pid) =>
                {
                    var list = new List<string>() { string.Format("PluggableInputDStream-{0}-{1}", pid, time) };
                    return list.AsEnumerable();
                });

            inputDStream.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                int partitions = rdd.GetNumPartitions();

                Console.WriteLine("-------------------------------------------");
                Console.WriteLine("Time: {0}", time);
                Console.WriteLine("-------------------------------------------");
                Console.WriteLine("Count: " + taken.Length);
                Console.WriteLine("Partitions: " + partitions);

                foreach (object record in taken)
                {
                    Console.WriteLine(record);
                }
            });

            ssc.Start();
            ssc.AwaitTermination();
        }
    }


    // Use this helper class to test broacast variable in streaming application
    [Serializable]
    internal class UpdateStateHelper
    {
        private Broadcast<int> b;

        internal UpdateStateHelper(Broadcast<int> b)
        {
            this.b = b;
        }

        internal int Execute(IEnumerable<Tuple<int, int>> vs, int s)
        {
            int result = vs.Sum(x => x.Item1 + x.Item2) + s + b.Value;
            return result;
        }
    }

}
