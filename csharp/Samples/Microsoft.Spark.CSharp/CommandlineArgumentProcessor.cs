// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using Microsoft.Spark.CSharp.Services;
using System.IO;

namespace Microsoft.Spark.CSharp.Samples
{
    /// <summary>
    /// Simple commandline argument parser
    /// </summary>
    internal class CommandlineArgumentProcessor
    {
        private const string UsageFileName = "samplesusage.md";

        private static readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(CommandlineArgumentProcessor));
        internal static Configuration ProcessArugments(string[] args)
        {
            if (args.Length == 0)
            {
                PrintUsage();
                Environment.Exit(0);
            }

            var configuration = new Configuration();
            logger.LogInfo(string.Format("Arguments to SparkCLRSamples.exe are {0}", string.Join(",", args)));
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].Equals("--help", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("-h", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("-?", StringComparison.InvariantCultureIgnoreCase))
                {
                    PrintUsage();
                    Environment.Exit(0);
                }
                else if (args[i].Equals("spark.local.dir", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--temp", StringComparison.InvariantCultureIgnoreCase))
                {
                    configuration.SparkLocalDirectoryOverride = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.sampledata.loc", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--data", StringComparison.InvariantCultureIgnoreCase))
                {
                    configuration.SampleDataLocation = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.samples.torun", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--torun", StringComparison.InvariantCultureIgnoreCase))
                {
                    configuration.SamplesToRun = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.samples.category", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--cat", StringComparison.InvariantCultureIgnoreCase))
                {
                    configuration.SamplesCategory = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.enablevalidation", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--validate", StringComparison.InvariantCultureIgnoreCase))
                {
                    configuration.IsValidationEnabled = true;
                }
                else if (args[i].Equals("sparkclr.samples.checkpointdir", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--checkpointdir", StringComparison.InvariantCultureIgnoreCase))
                {
                    configuration.CheckpointDir = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.dryrun", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--dryrun", StringComparison.InvariantCultureIgnoreCase))
                {
                    configuration.IsDryrun = true;
                }
            }

            return configuration;
        }

        private static void PrintUsage()
        {
            Console.Write(File.ReadAllText(UsageFileName));
        }
    }
}
