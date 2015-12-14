// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace Microsoft.Spark.CSharp.Samples
{
    /// <summary>
    /// Runs samples
    /// </summary>
    internal class SamplesRunner
    {
        private static Regex samplesToRunRegex;
        private static Regex samplesCategoryRegex;
        private static Stopwatch stopWatch;
        // track <SampleName, Category, SampleSucceeded, Duration> for reporting
        private static readonly List<Tuple<string, string, bool, TimeSpan>> samplesRunInfoList = new List<Tuple<string, string, bool, TimeSpan>>();

        internal static bool RunSamples()
        {
            var samples = Assembly.GetEntryAssembly().GetTypes()
                      .SelectMany(type => type.GetMethods(BindingFlags.NonPublic | BindingFlags.Static))
                      .Where(method => method.GetCustomAttributes(typeof(SampleAttribute), false).Length > 0)
                      .OrderByDescending(method => method.Name);
        
            samplesToRunRegex = GetRegex();
            samplesCategoryRegex = GetCategoryRegex();
            stopWatch = Stopwatch.StartNew();

            foreach (var sample in samples)
            {
                var sampleRunInfo = RunSample(sample);
                if (sampleRunInfo != null)
                {
                    samplesRunInfoList.Add(sampleRunInfo);
                }
            }
            stopWatch.Stop();
            return ReportOutcome();
        }

        private static Tuple<string, string, bool, TimeSpan> RunSample(MethodInfo sample)
        {
            var sampleName = sample.Name;
            var sampleAttributes = (SampleAttribute[]) sample.GetCustomAttributes(typeof(SampleAttribute), false);
            var categoryNames = string.Join<SampleAttribute>(",", sampleAttributes);

            if (samplesCategoryRegex != null)
            {
                if (!sampleAttributes.Any(attribute => attribute.Match(samplesCategoryRegex)))
                {
                    return null;
                }
            }

            if (samplesToRunRegex != null)
            {
                //assumes method/sample names are unique, and different samples are delimited by ','
                var containedInSamplesToRun = SparkCLRSamples.Configuration.SamplesToRun.ToLowerInvariant().Trim().Split(',').Contains(sampleName.ToLowerInvariant());
                if (!containedInSamplesToRun && !samplesToRunRegex.IsMatch(sampleName))
                {
                    return null;
                }
            }

            var clockStart = stopWatch.Elapsed;
            var duration = stopWatch.Elapsed - clockStart;
            try
            {
                if (!SparkCLRSamples.Configuration.IsDryrun)
                {
                    SparkCLRSamples.WriteColorCodedConsoleMessage(ConsoleColor.Cyan, string.Format("----- Running sample {0} -----", sampleName));
                    sample.Invoke(null, new object[] { });
                    duration = stopWatch.Elapsed - clockStart;
                    SparkCLRSamples.WriteColorCodedConsoleMessage(ConsoleColor.Green, string.Format("----- Successfully finished running sample {0} (duration={1}) -----", sampleName, duration));
                }

                return new Tuple<string, string, bool, TimeSpan>(sampleName, categoryNames, true, duration);
            }
            catch (Exception ex)
            {
                duration = stopWatch.Elapsed - clockStart;
                SparkCLRSamples.WriteColorCodedConsoleMessage(ConsoleColor.Red, string.Format("----- Error running sample {0} (duration={3}) -----{1}{2}", sampleName, Environment.NewLine, ex, duration));
                return new Tuple<string, string, bool, TimeSpan>(sampleName, categoryNames, false, duration);
            }

        }

        private static Regex GetCategoryRegex()
        {
            Regex categoryRegex = null;
            if (!string.IsNullOrEmpty(SparkCLRSamples.Configuration.SamplesCategory))
            {
                var s = SparkCLRSamples.Configuration.SamplesCategory;
                if (s.StartsWith("/") && s.EndsWith("/") && s.Length > 2)
                {
                    // forward-slashes enclose .Net regular expression 
                    categoryRegex = new Regex(s.Substring(1, s.Length - 2));
                }
                else
                {
                    // default to Unix or Windows command line wild card matching, case insensitive
                    categoryRegex = new Regex("^" + Regex.Escape(s).Replace(@"\*", ".*").Replace(@"\?", ".") + "$",
                        RegexOptions.IgnoreCase);
                }
            }
            return categoryRegex;
        }

        private static Regex GetRegex()
        {
            Regex regex = null;
            if (!string.IsNullOrEmpty(SparkCLRSamples.Configuration.SamplesToRun))
            {
                var s = SparkCLRSamples.Configuration.SamplesToRun;
                if (s.StartsWith("/") && s.EndsWith("/") && s.Length > 2)
                {
                    // forward-slashes enclose .Net regular expression 
                    regex = new Regex(s.Substring(1, s.Length - 2));
                }
                else
                {
                    // default to Unix or Windows command line wild card matching, case insensitive
                    regex = new Regex("^" + Regex.Escape(s).Replace(@"\*", ".*").Replace(@"\?", ".") + "$",
                        RegexOptions.IgnoreCase);
                }
            }
            return regex;
        }

        private static bool ReportOutcome()
        {
            var succeededSamples = samplesRunInfoList.Where(x => x.Item3).ToList();
            var failedSamples = samplesRunInfoList.Where(x => !x.Item3).ToList();

            var summary = new StringBuilder().Append("----- ")
                .Append("Finished running ")
                .Append(string.Format("{0} samples(s) [succeeded={1}, failed={2}]", samplesRunInfoList.Count, succeededSamples.Count, failedSamples.Count))
                .Append(" in ").Append(stopWatch.Elapsed)
                .AppendLine(" -----").ToString();

            SparkCLRSamples.WriteColorCodedConsoleMessage(ConsoleColor.Yellow, summary);

            if (succeededSamples.Count > 0)
            {
                SparkCLRSamples.WriteColorCodedConsoleMessage(ConsoleColor.Green, "Successfully completed samples:");
                foreach (var s in succeededSamples)
                {
                    var sampleResult = new StringBuilder().Append("    ")
                        .Append(string.Format("{0} (category: {1}), duration={2}", s.Item1, s.Item2, s.Item4)).ToString();
                    SparkCLRSamples.WriteColorCodedConsoleMessage(ConsoleColor.Green, sampleResult);
                }
            }

            if (failedSamples.Count > 0)
            {
                SparkCLRSamples.WriteColorCodedConsoleMessage(ConsoleColor.Red, "Failed samples:");
                foreach (var s in failedSamples)
                {
                    var sampleResult = new StringBuilder().Append("    ")
                        .Append(string.Format("{0} (category: {1}), duration={2}", s.Item1, s.Item2, s.Item4)).ToString();
                    SparkCLRSamples.WriteColorCodedConsoleMessage(ConsoleColor.Red, sampleResult);
                }
            }

            return failedSamples.Count == 0;
        }
    }
}
