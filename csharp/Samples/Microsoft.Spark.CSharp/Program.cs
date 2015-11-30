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
        internal static Configuration Configuration = new Configuration();
        internal static SparkContext SparkContext;
        internal static ILoggerService Logger;

        static void Main(string[] args)
        {
            LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance); //this is optional - DefaultLoggerService will be used if not set
            Logger = LoggerServiceFactory.GetLogger(typeof(SparkCLRSamples));
            ProcessArugments(args);

            PrintLogLocation();
            if (Configuration.IsDryrun)
            {
                RunSamples();
            }
            else
            {
                SparkContext = CreateSparkContext();
                SparkContext.SetCheckpointDir(Path.GetTempPath()); 
                RunSamples();

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
            // track <SampleName, Category, Duration> in "completed" and "error" list, for reporting
            var completed = new List<Tuple<string, string, TimeSpan>>();
            var errors = new List<Tuple<string, string, TimeSpan>>();

            Regex regex = null;
            if (!string.IsNullOrEmpty(Configuration.SamplesToRun))
            {
                var s = Configuration.SamplesToRun; 
                if (s.StartsWith("/") && s.EndsWith("/") && s.Length > 2)
                {
                    // forward-slashes enclose .Net regular expression 
                    regex = new Regex(s.Substring(1, s.Length - 2));
                }
                else
                {
                    // default to Unix or Windows command line wild card matching, case insensitive
                    regex = new Regex("^" + Regex.Escape(s).Replace(@"\*", ".*").Replace(@"\?", ".") + "$", RegexOptions.IgnoreCase);
                }
            }

            Regex categoryRegex = null;
            if (!string.IsNullOrEmpty(Configuration.SamplesCategory))
            {
                var s = Configuration.SamplesCategory;
                if (s.StartsWith("/") && s.EndsWith("/") && s.Length > 2)
                {
                    // forward-slashes enclose .Net regular expression 
                    categoryRegex = new Regex(s.Substring(1, s.Length - 2));
                }
                else
                {
                    // default to Unix or Windows command line wild card matching, case insensitive
                    categoryRegex = new Regex("^" + Regex.Escape(s).Replace(@"\*", ".*").Replace(@"\?", ".") + "$", RegexOptions.IgnoreCase);
                }
            }

            var sw = Stopwatch.StartNew();

            foreach (var sample in samples)
            {
                var sampleName = sample.Name;
                var runSample = true;
                var sampleAttributes = (SampleAttribute[])sample.GetCustomAttributes(typeof(SampleAttribute), false);
                var categoryNames = string.Join<SampleAttribute>(",", sampleAttributes);

                if (categoryRegex != null) 
                {
                    runSample = sampleAttributes.Any(attribute => attribute.Match(categoryRegex));
                    if (!runSample)
                    {
                        continue;
                    }
                }

                if (regex != null)
                {
                    if ((Configuration.SamplesToRun.IndexOf(sampleName, StringComparison.InvariantCultureIgnoreCase) < 0) //assumes method/sample names are unique
                        && !regex.IsMatch(sampleName)) 
                    {
                        runSample = false;
                        continue;
                    }
                }

                var clockStart = sw.Elapsed;
                var duration = sw.Elapsed - clockStart; 
                try
                {
                    numSamples++;

                    if (!Configuration.IsDryrun)
                    {
                        Console.WriteLine("----- Running sample {0} -----", sampleName);;
                        sample.Invoke(null, new object[] {});
                        duration = sw.Elapsed - clockStart;
                        Console.WriteLine("----- Finished running sample {0}, duration={1} -----", sampleName, duration);
                    }

                    completed.Add(new Tuple<string, string, TimeSpan>(sampleName, categoryNames, duration));
                }
                catch (Exception ex)
                {
                    duration = sw.Elapsed - clockStart;
                    Console.WriteLine("----- Error running sample {0} -----{1}{2}, duration={3}",
                                      sampleName, Environment.NewLine, ex, duration);
                    errors.Add(new Tuple<string, string, TimeSpan>(sampleName, categoryNames, duration));
                }
            }
            sw.Stop();
            ReportOutcome(numSamples, completed, errors, sw.Elapsed);
        }

        private static void ReportOutcome(int numSamples, IList<Tuple<string, string, TimeSpan>> completed, IList<Tuple<string, string, TimeSpan>> errors, TimeSpan duration)
        {
            if (completed == null)
            {
                throw new ArgumentNullException("completed");
            }
            if (errors == null)
            {
                throw new ArgumentNullException("errors");
            }

            var msg = new StringBuilder();

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
            foreach (var s in completed)
            {
                msg.Append("    ").AppendLine(string.Format("{0} (category: {1}), duration={2}", s.Item1, s.Item2, s.Item3));
            }

            msg.AppendLine("Failed samples:");
            foreach (var s in errors)
            {
                msg.Append("    ").AppendLine(string.Format("{0} (category: {1}), duration={2}", s.Item1, s.Item2, s.Item3));
            }

            if (errors.Count == 0)
            {
                Console.WriteLine(msg.ToString());
            }
            else
            {
                Console.WriteLine("[Warning]{0}", msg);
            }
        }

        private static string Pluralize(int num, string things)
        {
            return num + " " + things + (num == 1 ? "" : "s");
        }
        private static void PrintUsage()
        {
            var p = AppDomain.CurrentDomain.FriendlyName;
            Console.WriteLine("   ");
            Console.WriteLine(" {0} supports following options:", p);
            Console.WriteLine("   ");
            Console.WriteLine("   [--temp | spark.local.dir] <TEMP_DIR>                 TEMP_DIR is the directory used as \"scratch\" space in Spark, including map output files and RDDs that get stored on disk. ");
            Console.WriteLine("                                                         See http://spark.apache.org/docs/latest/configuration.html for details.");
            Console.WriteLine("   ");
            Console.WriteLine("   [--data | sparkclr.sampledata.loc] <SAMPLE_DATA_DIR>  SAMPLE_DATA_DIR is the directory where Sample data resides. ");
            Console.WriteLine("   ");
            Console.WriteLine("   [--torun | sparkclr.samples.torun] <SAMPLE_LIST>      SAMPLE_LIST specifies a list of samples to run. ");
            Console.WriteLine("                                                         Case-insensitive command line wild card matching by default. Or, use \"/\" (forward slash) to enclose regular expression. ");
            Console.WriteLine("   ");
            Console.WriteLine("   [--cat | sparkclr.samples.category] <SAMPLE_CATEGORY> SAMPLE_CATEGORY can be \"all\", \"default\", \"experimental\" or any new categories. ");
            Console.WriteLine("                                                         Case-insensitive command line wild card matching by default. Or, use \"/\" (forward slash) to enclose regular expression. ");
            Console.WriteLine("   ");
            Console.WriteLine("   [--validate | sparkclr.enablevalidation]              Enable validation. ");
            Console.WriteLine("   ");
            Console.WriteLine("   [--dryrun | sparkclr.dryrun]                          Dry-run mode. ");
            Console.WriteLine("   ");
            Console.WriteLine("   [--help | -h | -?]                                    Display usage. ");
            Console.WriteLine("   ");
            Console.WriteLine("   ");
            Console.WriteLine(" Usage examples:  ");
            Console.WriteLine("   ");
            Console.WriteLine("   Example 1 - run default samples:");
            Console.WriteLine("   ");
            Console.WriteLine(@"     {0} --temp C:\gitsrc\SparkCLR\run\Temp --data C:\gitsrc\SparkCLR\run\data ", p);
            Console.WriteLine("   ");
            Console.WriteLine("   Example 2 - dryrun default samples:");
            Console.WriteLine("   ");
            Console.WriteLine(@"     {0} --dryrun ", p);
            Console.WriteLine("   ");
            Console.WriteLine("   Example 3 - dryrun all samples:");
            Console.WriteLine("   ");
            Console.WriteLine(@"     {0} --dryrun --cat all ", p);
            Console.WriteLine("   ");
            Console.WriteLine("   Example 4 - dryrun PiSample (commandline wildcard matching, case-insensitive):");
            Console.WriteLine("   ");
            Console.WriteLine(@"     {0} --dryrun --torun pi*", p);
            Console.WriteLine("   ");
            Console.WriteLine("   Example 5 - dryrun all DF* samples (commandline wildcard matching, case-insensitive):");
            Console.WriteLine("   ");
            Console.WriteLine(@"     {0} --dryrun --cat a* --torun DF*", p);
            Console.WriteLine("   ");
            Console.WriteLine("   Example 6 - dryrun all RD* samples (regular expression):");
            Console.WriteLine("   ");
            Console.WriteLine(@"     {0} --dryrun --cat a* --torun /\bRD.*Sample.*\b/", p);
            Console.WriteLine("   ");
            Console.WriteLine("   Example 7 - dryrun specific samples (case insensitive): ");
            Console.WriteLine("   ");
            Console.WriteLine("     {0} --dryrun --torun \"DFShowSchemaSample,DFHeadSample\"", p);
            Console.WriteLine("   ");
        }

        private static void PrintLogLocation()
        {
            ConsoleWriteLine("Main",
                             string.Format(@"Logs by SparkCLR and Apache Spark are available at {0}\SparkCLRLogs",
                                           Environment.GetEnvironmentVariable("TEMP")));
        }

        //simple commandline arg processor
        private static void ProcessArugments(string[] args)
        {
            if (args.Length == 0)
            {
                PrintUsage();
                Environment.Exit(0);
            }

            Logger.LogInfo(string.Format("Arguments to SparkCLRSamples are {0}", string.Join(",", args)));
            for (int i=0; i<args.Length;i++)
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
                    Configuration.SparkLocalDirectoryOverride = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.sampledata.loc", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--data", StringComparison.InvariantCultureIgnoreCase))
                {
                    Configuration.SampleDataLocation = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.samples.torun", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--torun", StringComparison.InvariantCultureIgnoreCase))
                {
                    Configuration.SamplesToRun = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.samples.category", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--cat", StringComparison.InvariantCultureIgnoreCase))
                {
                    Configuration.SamplesCategory = args[i + 1];
                }
                else if (args[i].Equals("sparkclr.enablevalidation", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--validate", StringComparison.InvariantCultureIgnoreCase))
                {
                    Configuration.IsValidationEnabled = true;
                }
                else if (args[i].Equals("sparkclr.dryrun", StringComparison.InvariantCultureIgnoreCase)
                    || args[i].Equals("--dryrun", StringComparison.InvariantCultureIgnoreCase))
                {
                    Configuration.IsDryrun = true;
                }
            }
        }

        private static void ConsoleWriteLine(string functionName, string message)
        {
            var p = AppDomain.CurrentDomain.FriendlyName;
            Console.WriteLine("[{0}.{1}] {2}", p, functionName, message);
        }
    }

    /// <summary>
    /// Attribute that marks a method as a sample
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    internal class SampleAttribute : Attribute
    {
        public const string CATEGORY_ALL = "all";   // run all sample tests
        public const string CATEGORY_DEFAULT = "default"; // run default tests

        private string category;

        public SampleAttribute(string category)
        {
            this.category = category;
        }

        public SampleAttribute()
        {
            this.category = CATEGORY_DEFAULT;
        }

        public string Category
        {
            get
            {
                return category;
            }
        }

        /// <summary>
        /// whether this category matches the target category
        /// </summary>
        // public bool Match(string targetCategory)
        public bool Match(Regex targetCategory)
        {
            if (null == targetCategory)
            {
                throw new ArgumentNullException("targetCategory");
            }

            return targetCategory.IsMatch(CATEGORY_ALL)
                || targetCategory.IsMatch(this.category);
        }

        public override string ToString()
        {
            return category;
        }
    }
}
