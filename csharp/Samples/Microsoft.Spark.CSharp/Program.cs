// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
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
            bool status = true;
            if (Configuration.IsDryrun)
            {
                status = SamplesRunner.RunSamples();
            }
            else
            {
                SparkContext = CreateSparkContext();
                SparkContext.SetCheckpointDir(Path.GetTempPath());

                status = SamplesRunner.RunSamples();

                PrintLogLocation();
                ConsoleWriteLine("Completed running samples. Calling SparkContext.Stop() to tear down ...");
                //following comment is necessary due to known issue in Spark. See https://issues.apache.org/jira/browse/SPARK-8333
                ConsoleWriteLine("If this program (SparkCLRSamples.exe) does not terminate in 10 seconds, please manually terminate java process launched by this program!!!");
                //TODO - add instructions to terminate java process
                SparkContext.Stop();
            }

            if (Configuration.IsValidationEnabled && !status)
            {
                Environment.Exit(1);
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
            ConsoleWriteLine(string.Format(@"Logs by SparkCLR and Apache Spark are available at {0}\SparkCLRLogs",
                                           Environment.GetEnvironmentVariable("TEMP")));
        }

        private static void ConsoleWriteLine(string message)
        {
            const string exeName = "SparkCLRSamples.exe";
            var callingMethod = new StackTrace().GetFrames()[1].GetMethod();
            var callingMethodName = callingMethod.Name;
            WriteColorCodedConsoleMessage(ConsoleColor.DarkCyan, string.Format("[{0}.{1}] {2}", exeName, callingMethodName, message));
        }

        //the benefits of color coding can be seen only in debug mode because in other modes
        //the console output is redirected to another process and color coding will be lost
        internal static void WriteColorCodedConsoleMessage(ConsoleColor colorCoding, string message)
        {
            ConsoleColor currentForegroundColor = Console.ForegroundColor;
            Console.ForegroundColor = colorCoding;
            Console.WriteLine(message);
            Console.ForegroundColor = currentForegroundColor;
        }
    }
}
