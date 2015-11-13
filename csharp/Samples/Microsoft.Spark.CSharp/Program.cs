// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Samples
{
    /// <summary>
    /// Samples for SparkCLR
    /// </summary>
    public class SparkCLRSamples
    {
        internal static Configuration Configuration = new Configuration();
        internal static SparkContext SparkContext;
        internal static ILoggerService Logger;

        static void Main(string[] args)
        {
            LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance); //this is optional - DefaultLoggerService will be used if not set
            Logger = LoggerServiceFactory.GetLogger(typeof(SparkCLRSamples));
            ProcessArugments(args);
            SparkContext = CreateSparkContext();
            SparkContext.SetCheckpointDir(Path.GetTempPath()); 
            RunSamples();
            SparkContext.Stop();
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

        //finds all methods that are marked with [Sample] attribute and 
        //runs all of them if sparkclr.samples.torun commandline arg is not used
        //or just runs the ones that are provided as comma separated list
        private static void RunSamples()
        {
            var samples = Assembly.GetEntryAssembly().GetTypes()
                      .SelectMany(type => type.GetMethods(BindingFlags.NonPublic | BindingFlags.Static))
                      .Where(method => method.GetCustomAttributes(typeof(SampleAttribute), false).Length > 0)
                      .OrderByDescending(method => method.Name);

            int numSamples = 0;
            List<string> completed = new List<string>();
            List<string> errors = new List<string>();
            Stopwatch sw = Stopwatch.StartNew();
            foreach (var sample in samples)
            {
                string sampleName = sample.Name;
                bool runSample = true;
                if (Configuration.SamplesToRun != null)
                {
                    if (!Configuration.SamplesToRun.Contains(sampleName)) //assumes method/sample names are unique
                    {
                        runSample = false;
                    }
                }

                if (runSample)
                {
                    try
                    {
                        numSamples++;
                        Logger.LogInfo(string.Format("----- Running sample {0} -----", sampleName));
                        sample.Invoke(null, new object[] {});
                        Logger.LogInfo(string.Format("----- Finished runnning sample {0} -----", sampleName));
                        completed.Add(sampleName);
                    }
                    catch (Exception ex)
                    {
                        Logger.LogError(string.Format("----- Error runnning sample {0} -----{1}{2}", 
                            sampleName, Environment.NewLine, ex));
                        errors.Add(sampleName);
                    }
                }
            }
            sw.Stop();
            ReportOutcome(numSamples, completed, errors, sw.Elapsed);
        }

        private static void ReportOutcome(int numSamples, IList<string> completed, IList<string> errors, TimeSpan duration)
        {
            StringBuilder msg = new StringBuilder();

            msg.Append("----- ")
                .Append("Finished running ")
                .Append(Pluralize(numSamples, "sample"))
                .Append(" in ").Append(duration)
                .AppendLine(" -----");

            msg.Append("----- ")
                .Append(" Completion counts:")
                .Append(" Success=").Append(completed.Count)
                .Append(" Failed=").Append(errors.Count)
                .AppendLine(" -----");

            msg.AppendLine("Successful samples:");
            foreach (string s in completed)
            {
                msg.Append("    ").AppendLine(s);
            }

            msg.AppendLine("Failed samples:");
            foreach (string s in errors)
            {
                msg.Append("    ").AppendLine(s);
            }

            if (errors.Count == 0)
            {
                Logger.LogInfo(msg.ToString());
            }
            else
            {
                Logger.LogWarn(msg.ToString());
            }
        }

        private static string Pluralize(int num, string things)
        {
            return num + " " + things + (num == 1 ? "" : "s");
        }

        //simple commandline arg processor
        private static void ProcessArugments(string[] args)
        {
            Logger.LogInfo(string.Format("Arguments to SparkCLRSamples are {0}", string.Join(",", args)));
            for (int i=0; i<args.Length;i++)
            {
                if (args[i].Equals("spark.local.dir", StringComparison.InvariantCultureIgnoreCase))
                {
                    Configuration.SparkLocalDirectoryOverride = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.sampledata.loc", StringComparison.InvariantCultureIgnoreCase))
                {
                    Configuration.SampleDataLocation = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.samples.torun", StringComparison.InvariantCultureIgnoreCase))
                {
                    Configuration.SamplesToRun = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.enablevalidation", StringComparison.InvariantCultureIgnoreCase))
                {
                    Configuration.IsValidationEnabled = true;
                }
            }
        }
    }

    /// <summary>
    /// Attribute that marks a method as a sample
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    internal class SampleAttribute : Attribute
    {}
}
