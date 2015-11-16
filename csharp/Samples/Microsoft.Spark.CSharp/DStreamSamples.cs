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

namespace Microsoft.Spark.CSharp
{
    class DStreamSamples
    {
        private static int count;
        private static bool stopFileServer;
        private static void StartFileServer(string directory, string pattern, int loop)
        {
            string testDir = Path.Combine(directory, "test");
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
                    System.Threading.Thread.Sleep(200);
                }

                foreach (var file in Directory.GetFiles(testDir, "*"))
                    File.Delete(file);
            });
        }

        [Sample("experimental")]
        internal static void DStreamTextFileSamples()
        {
            SparkContext sc = SparkCLRSamples.SparkContext;
            string directory = SparkCLRSamples.Configuration.SampleDataLocation;
            sc.SetCheckpointDir(directory);
            StreamingContext ssc = new StreamingContext(sc, 2000);

            var lines = ssc.TextFileStream(directory);
            var words = lines.FlatMap(l => l.Split(' '));
            var pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));
            var wordCounts = pairs.ReduceByKey((x, y) => x + y);
            var join = wordCounts.Join(wordCounts, 2);
            var state = join.UpdateStateByKey<string, Tuple<int, int>, int>((vs, s) => vs.Sum(x => x.Item1 + x.Item2) + s);

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
                }
                Console.WriteLine();

                stopFileServer = count++ > 3;
            });

            ssc.Start();

            StartFileServer(directory, "words.txt", 100);
            while (!stopFileServer)
            {
                System.Threading.Thread.Sleep(1000);
            }

            // wait ForeachRDD to complete to let ssc.Stop() gracefully
            System.Threading.Thread.Sleep(2000);

            ssc.Stop();
        }
    }
}
