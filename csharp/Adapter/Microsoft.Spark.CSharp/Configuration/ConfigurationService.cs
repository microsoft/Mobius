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
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Configuration
{
    /// <summary>
    /// Implementation of configuration service that helps getting config settings
    /// to be used in SparkCLR runtime
    /// </summary>
    internal class ConfigurationService : IConfigurationService
    {
        private ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(ConfigurationService));
        private SparkCLRConfiguration configuration;
        private RunMode runMode = RunMode.UNKNOWN; //not used anywhere for now but may come handy in the future

        public int BackendPortNumber
        {
            get
            {
                return configuration.GetPortNumber();
            }
        }

        internal ConfigurationService()
        {
            var appConfig = ConfigurationManager.OpenExeConfiguration(Assembly.GetEntryAssembly().Location);
            var sparkMaster = Environment.GetEnvironmentVariable("spark.master"); //set by CSharpRunner when launching driver process
            if (sparkMaster == null)
            {
                configuration = new SparkCLRDebugConfiguration(appConfig);
                runMode = RunMode.DEBUG;
            }
            else if (sparkMaster.StartsWith("local"))
            {
                configuration = new SparkCLRLocalConfiguration(appConfig);
                runMode = RunMode.LOCAL;
            }
            else if (sparkMaster.StartsWith("spark://"))
            {
                configuration = new SparkCLRConfiguration(appConfig);
                runMode = RunMode.CLUSTER;
            }
            else if (sparkMaster.StartsWith("yarn"))
            {
                runMode = RunMode.YARN;
                throw new NotSupportedException("YARN is not currently supported");
            }
            else
            {
                throw new NotSupportedException(string.Format("Spark master value {0} not recognized", sparkMaster));
            }

            logger.LogInfo(string.Format("ConfigurationService runMode is {0}", runMode));
        }

        public string GetCSharpRDDExternalProcessName()
        {
            return configuration.GetCSharpRDDExternalProcessName();
        }

        public string GetCSharpWorkerPath()
        {
            return configuration.GetCSharpWorkerPath();
        }

        public IEnumerable<string> GetDriverFiles()
        {
            return configuration.GetDriverFiles();
        }

        /// <summary>
        /// Default configuration for SparkCLR jobs.
        /// Works with Standalone cluster mode
        /// May work with YARN or Mesos - needs validation when adding support for YARN/Mesos
        /// </summary>
        private class SparkCLRConfiguration
        {
            protected AppSettingsSection appSettings;
            private string sparkCLRHome = Environment.GetEnvironmentVariable("SPARKCLR_HOME"); //set by sparkclr-submit.cmd
            protected ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkCLRConfiguration));
            internal SparkCLRConfiguration(System.Configuration.Configuration configuration)
            {
                appSettings = configuration.AppSettings;
            }

            internal virtual  int GetPortNumber()
            {
                int portNo;
                if (!int.TryParse(Environment.GetEnvironmentVariable("CSHARPBACKEND_PORT"), out portNo))
                {
                    throw new Exception("Environment variable CSHARPBACKEND_PORT not set");
                }

                logger.LogInfo("CSharpBackend successfully read from environment variable CSHARPBACKEND_PORT");
                return portNo;
            }

            internal virtual string GetCSharpRDDExternalProcessName()
            {
                //SparkCLR jar and driver, worker & dependencies are shipped using Spark file server. These files available in spark executing directory at executor
                return "CSharpWorker.exe";
            }

            internal virtual string GetCSharpWorkerPath()
            {
                return new Uri(GetSparkCLRArtifactsPath("bin", "CSharpWorker.exe")).ToString();
            }

            //this works for Standlone cluster //TODO fix for YARN support
            internal virtual IEnumerable<string> GetDriverFiles()
            {
                var driverFolder = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location); 
                var files = Directory.EnumerateFiles(driverFolder);
                return files.Select(s => new Uri(s).ToString());
            }

            private string GetSparkCLRArtifactsPath(string sparkCLRSubFolderName, string fileName)
            {
                var filePath = Path.Combine(sparkCLRHome, sparkCLRSubFolderName, fileName);
                if (!File.Exists(filePath))
                {
                    throw new Exception(string.Format("Path {0} not exists", filePath));
                }
                return filePath;
            }
        }

        /// <summary>
        /// Configuration for SparkCLR jobs in ** Local ** mode
        /// Needs some investigation to find out why Local mode behaves
        /// different than standalone cluster mode for the configuration values
        /// overridden here
        /// </summary>
        private class SparkCLRLocalConfiguration : SparkCLRConfiguration
        {
            internal SparkCLRLocalConfiguration(System.Configuration.Configuration configuration)
                : base(configuration)
            {}

            internal override string GetCSharpRDDExternalProcessName()
            {
                return appSettings.Settings["CSharpWorkerPath"].Value;
            }
            
            internal override string GetCSharpWorkerPath()
            {
                return new Uri(appSettings.Settings["CSharpWorkerPath"].Value).ToString();
            }

        }

        /// <summary>
        /// Configuration mode for debug mode
        /// This configuration exists only to make SparkCLR development & debugging easier
        /// </summary>
        private class SparkCLRDebugConfiguration : SparkCLRLocalConfiguration
        {
            internal SparkCLRDebugConfiguration(System.Configuration.Configuration configuration)
                : base(configuration)
            {}

            internal override int GetPortNumber()
            {
                var cSharpBackendPortNumber = int.Parse(appSettings.Settings["CSharpBackendPortNumber"].Value);
                logger.LogInfo(string.Format("CSharpBackend port number read from app config {0}", cSharpBackendPortNumber));
                return cSharpBackendPortNumber;
            }
        }
    }

    public enum RunMode
    {
        UNKNOWN,
        DEBUG, //not a Spark mode but exists for dev debugging purpose
        LOCAL,
        CLUSTER,
        //following are not currently supported
        YARN,
        MESOS
    }
}
