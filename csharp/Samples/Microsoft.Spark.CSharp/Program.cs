// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Samples
{
    /// <summary>
    /// Samples for SparkCLR
    /// </summary>
    public class SparkCLRSamples
    {
        internal static Configuration Configuration = new Configuration();
        internal static SparkContext SparkContext;
        static void Main(string[] args)
        {
            ProcessArugments(args);

            using (SparkCLREnvironment.Initialize())
            {
                SparkContext = CreateSparkContext();
                RunSamples();
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

            foreach (var sample in samples)
            {
                bool runSample = true;
                if (Configuration.SamplesToRun != null)
                {
                    if (!Configuration.SamplesToRun.Contains(sample.Name)) //assumes method/sample names are unique
                    {
                        runSample = false;
                    }
                }

                if (runSample)
                {
                    Console.WriteLine("----- Running sample {0} -----", sample.Name);
                    sample.Invoke(null, new object[] { });
                }
            }

        }


        //simple commandline arg processor
        private static void ProcessArugments(string[] args)
        {
            Console.WriteLine("Arguments to SparkCLRSamples are {0}", string.Join(",", args));
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
