// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Samples
{
    internal class CommandlineArgumentProcessor
    {
        private static readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(CommandlineArgumentProcessor));
        internal static Configuration ProcessArugments(string[] args)
        {
            if (args.Length == 0)
            {
                PrintUsage();
                Environment.Exit(0);
            }

            var configuration = new Configuration();
            logger.LogInfo(string.Format("Arguments to SparkCLRSamples are {0}", string.Join(",", args)));
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
    }
}
