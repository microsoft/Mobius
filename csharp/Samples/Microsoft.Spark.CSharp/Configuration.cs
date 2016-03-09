// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Samples
{
    internal class Configuration
    {
        private string samplesCategory = SampleAttribute.CATEGORY_DEFAULT;

        public string SparkLocalDirectoryOverride
        {
            get;
            set;
        }

        public string SampleDataLocation
        {
            get;
            set;
        }

        public string SamplesToRun
        {
            get;
            set;
        }

        public string SamplesCategory
        {
            get
            {
                return samplesCategory;
            }

            set
            {
                samplesCategory = value;
            }
        }

        public bool IsValidationEnabled
        {
            get;
            set;
        }

        public bool IsDryrun
        {
            get;
            set;
        }

        public string CheckpointDir
        {
            get; 
            set;
        }

        public string GetInputDataPath(string fileName)
        {
            if (SampleDataLocation.StartsWith("hdfs://"))
            {
                var clusterPath = SampleDataLocation + "/" + fileName;
                SparkCLRSamples.Logger.LogInfo("Cluster path " + clusterPath);
                return clusterPath;
            }
            else
            {
                return new Uri(Path.Combine(SampleDataLocation, fileName)).ToString();
            }
        }
    }
}
