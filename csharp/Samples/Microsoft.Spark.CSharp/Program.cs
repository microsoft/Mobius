// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Samples
{
    /// <summary>
    /// Samples for SparkCLR
    /// </summary>
    public class SparkCLRSamples
    {
        internal static Configuration Configuration;
        internal static SparkContext SparkContext;
        internal static ILoggerService Logger;

        static void Main(string[] args)
        {
            LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance); //this is optional - DefaultLoggerService will be used if not set
            Logger = LoggerServiceFactory.GetLogger(typeof(SparkCLRSamples));
            Configuration = CommandlineArgumentProcessor.ProcessArugments(args);

            PrintLogLocation();
            if (Configuration.IsDryrun)
            {
                SamplesRunner.RunSamples();
            }
            else
            {
                SparkContext = CreateSparkContext();
                SparkContext.SetCheckpointDir(Path.GetTempPath()); 
                SamplesRunner.RunSamples();

                PrintLogLocation();
                ConsoleWriteLine("Main", "Completed RunSamples. Calling SparkContext.Stop() to tear down ...");
                ConsoleWriteLine("Main", "If the program does not terminate in 10 seconds, please manually terminate java process !!!");
                SparkContext.Stop();
            }
        }

        // Creates and returns a context
        private static SparkContext CreateSparkContext()
        {
            var conf = new SparkConf();
            if (Configuration.SparkLocalDirectoryOverride != null)
            {
                conf.Set("spark.local.dir", Configuration.SparkLocalDirectoryOverride);
            }
            return new SparkContext(conf);
        }

        private static void PrintLogLocation()
        {
            ConsoleWriteLine("Main",
                             string.Format(@"Logs by SparkCLR and Apache Spark are available at {0}\SparkCLRLogs",
                                           Environment.GetEnvironmentVariable("TEMP")));
        }

        private static void ConsoleWriteLine(string functionName, string message)
        {
            var p = AppDomain.CurrentDomain.FriendlyName;
            Console.WriteLine("[{0}.{1}] {2}", p, functionName, message);
        }
    }
}
