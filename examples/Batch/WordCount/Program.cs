// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// Mobius WordCount example
    /// Word Count
    /// Reference: https://github.com/apache/spark/blob/master/examples/src/main/python/wordcount.py
    /// </summary>
    public static class WordCountExample
    {
        private static ILoggerService Logger;
        public static int Main(string[] args)
        {
            LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance); //this is optional - DefaultLoggerService will be used if not set
            Logger = LoggerServiceFactory.GetLogger(typeof(WordCountExample));

            if (args.Length != 1)
            {
                Console.Error.WriteLine("Usage: WordCount  <file>");
                return 1;
            }

            var sparkContext = new SparkContext(new SparkConf().SetAppName("MobiusWordCount"));

            try
            {
                var lines = sparkContext.TextFile(args[0]);
                var counts = lines
                    .FlatMap(x =>  x.Split(' '))
                    .Map(w => new Tuple<string, int>(w, 1))
                    .ReduceByKey((x, y) => x + y);

                foreach (var wordcount in counts.Collect())
                {
                    Console.WriteLine("{0}: {1}", wordcount.Item1, wordcount.Item2);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError("Error performing Word Count");
                Logger.LogException(ex);
            }

            sparkContext.Stop();
            return 0;
        }

    }
}