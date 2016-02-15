// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;

using Microsoft.Spark.CSharp.Streaming;
using Microsoft.Spark.CSharp.Samples;

namespace Microsoft.Spark.CSharp.Samples
{
    class DStreamStateSample
    {
        private static int count;
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

                    // since operations like ReduceByKey, Join and UpdateStateByKey are
                    // separate dstream transformations defined in CSharpDStream.scala
                    // an extra CSharpRDD is introduced in between these operations
                    var wordCounts = pairs.ReduceByKey((x, y) => x + y);
                    //var join = wordCounts.Join(wordCounts, 2);
                    //var state = join.UpdateStateByKey<string, Tuple<int, int>, int>((vs, s) => vs.Sum(x => x.Item1 + x.Item2) + s);
                    //var initialStateRDD = sc.Parallelize(new KeyValuePair<string, int>[] { new KeyValuePair<string,int>("the", 100) });
                    StateSpec<string, int, int, int> stateSpec = new StateSpec<string, int, int, int>((word, count, state) =>
                    {

                        var sum = 0;
                        try
                        {
                            sum = state.Get();
                        }
                        catch (Exception)
                        {
                            Console.WriteLine("Can't find state, key:" + word);
                        }

                        state.Update(sum + count);
                        Console.WriteLine(word + ": " + count);

                        return sum;

                    });

                    var snapshots = wordCounts.MapWithState(stateSpec).StateSnapshots();

                    snapshots.ForeachRDD((double time, RDD<dynamic> rdd) =>
                    {
                        Console.WriteLine("-------------------------------------------");
                        Console.WriteLine("Snapshots @ Time: {0}", time);
                        Console.WriteLine("-------------------------------------------");
                        Console.WriteLine("Word distinct count:" + rdd.Count());
                        foreach (object record in rdd.Collect())
                        {
                            Console.WriteLine(record);
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
