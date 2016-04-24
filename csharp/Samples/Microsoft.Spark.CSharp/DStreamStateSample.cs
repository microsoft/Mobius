// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;

using Microsoft.Spark.CSharp.Streaming;

namespace Microsoft.Spark.CSharp.Samples
{
    class DStreamStateSample
    {
        private static bool stopFileServer;
        private static void StartFileServer(string directory, string pattern, int loop)
        {
            string testDir = Path.Combine(directory, "test1");
            if (!Directory.Exists(testDir))
                Directory.CreateDirectory(testDir);

            stopFileServer = false;

            string[] files = Directory.GetFiles(directory, pattern);

            Task.Run(() =>
            {
                while (!stopFileServer)
                {
                    DateTime now = DateTime.Now;
                    foreach (string path in files)
                    {
                        string text = File.ReadAllText(path);
                        File.WriteAllText(testDir + "\\" + now.ToBinary() + "_" + Path.GetFileName(path), text);
                    }
                    System.Threading.Thread.Sleep(10000);
                }

                System.Threading.Thread.Sleep(3000);

                foreach (var file in Directory.GetFiles(testDir, "*"))
                    File.Delete(file);
            });
        }

        [Sample("experimental")]
        internal static void DStreamMapWithStateSample()
        {
            string directory = SparkCLRSamples.Configuration.SampleDataLocation;
            string checkpointPath = Path.Combine(directory, "checkpoint");

            StreamingContext ssc = StreamingContext.GetOrCreate(checkpointPath,
                () =>
                {
                    SparkContext sc = SparkCLRSamples.SparkContext;
                    StreamingContext context = new StreamingContext(sc, 10000);
                    context.Checkpoint(checkpointPath);

                    var lines = context.TextFileStream(Path.Combine(directory, "test1"));
                    lines = context.Union(lines, lines);
                    var words = lines.FlatMap(l => l.Split(' '));
                    var pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));

                    var wordCounts = pairs.ReduceByKey((x, y) => x + y);
                    var initialState = sc.Parallelize(new[] { new KeyValuePair<string, int>("NOT_A_WORD", 1024), new KeyValuePair<string, int>("dog", 10000), }, 1);
                    var stateSpec = new StateSpec<string, int, int, KeyValuePair<string, int>>((word, count, state) =>
                    {
                        if (state.IsTimingOut())
                        {
                            Console.WriteLine("Found timing out word: {0}", word);
                            return new KeyValuePair<string, int>(word, state.Get());
                        }

                        var sum = 0;
                        if (state.Exists())
                        {
                            sum = state.Get();
                        }
                        state.Update(sum + count);
                        Console.WriteLine("word: {0}, count: {1}", word, sum + count);
                        return new KeyValuePair<string, int>(word, sum + count);
                    }).NumPartitions(1).InitialState(initialState).Timeout(TimeSpan.FromSeconds(30));

                    var snapshots = wordCounts.MapWithState(stateSpec).StateSnapshots();
                    snapshots.ForeachRDD((double time, RDD<dynamic> rdd) =>
                    {
                        Console.WriteLine("-------------------------------------------");
                        Console.WriteLine("Snapshots @ Time: {0}", time);
                        Console.WriteLine("-------------------------------------------");

                        foreach (KeyValuePair<string, int> record in rdd.Collect())
                        {
                            Console.WriteLine("[{0}, {1}]", record.Key, record.Value);
                        }
                        Console.WriteLine();
                    });

                    return context;
                });

            ssc.Start();

            StartFileServer(directory, "words.txt", 100);

            ssc.AwaitTermination();
            ssc.Stop();
        }
    }
}