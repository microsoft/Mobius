// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// Counts words in new text files created in the given directory
    /// This sample implements the same example available at https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/HdfsWordCount.scala.
    /// Usage: HdfsWordCount <checkpointDirectory> <inputDirectory>
    ///     <checkpointDirectory> is the directory that Spark Streaming will use to save checkpoint data. 
    ///     <inputDirectory> is the directory that Spark Streaming will use to find and read new text files. 
 
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: HdfsWordCount <checkpointDirectory> <inputDirectory>");
                return;
            }

            string checkpointPath = args[0];
            string inputDir = args[1];

            StreamingContext ssc = StreamingContext.GetOrCreate(checkpointPath,
                () =>
                {
                    var sparkConf = new SparkConf();
                    sparkConf.SetAppName("HdfsWordCount");
                    var sc = new SparkContext(sparkConf);
                    StreamingContext context = new StreamingContext(sc, 30000);
                    context.Checkpoint(checkpointPath);

                    var lines = context.TextFileStream(inputDir);
                    var words = lines.FlatMap(l => l.Split(' '));
                    var pairs = words.Map(w => new KeyValuePair<string, int>(w, 1));
                    var wordCounts = pairs.ReduceByKey((x, y) => x + y);

                    wordCounts.ForeachRDD((time, rdd) =>
                    {
                        Console.WriteLine("-------------------------------------------");
                        Console.WriteLine("Time: {0}", time);
                        Console.WriteLine("-------------------------------------------");
                        object[] taken = rdd.Take(10);
                        foreach (object record in taken)
                        {
                            Console.WriteLine(record);
                        }
                        Console.WriteLine();
                    });

                    return context;
                });

            ssc.Start();
            ssc.AwaitTermination();
            ssc.Stop();
        }
    }
}
