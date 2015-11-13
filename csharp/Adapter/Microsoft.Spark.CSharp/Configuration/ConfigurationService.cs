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
            else if (sparkMaster.Equals("yarn-client", StringComparison.OrdinalIgnoreCase))
            {
                configuration = new SparkCLRConfiguration(appConfig);
                runMode = RunMode.YARN;
            }
            else if (sparkMaster.Equals("yarn-cluster", StringComparison.OrdinalIgnoreCase))
            {
                configuration = new SparkCLRYarnClusterConfiguration(appConfig);
                runMode = RunMode.YARN;
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
            protected readonly AppSettingsSection appSettings;
            private readonly string sparkCLRHome = Environment.GetEnvironmentVariable("SPARKCLR_HOME"); //set by sparkclr-submit.cmd
            protected readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkCLRConfiguration));

            internal SparkCLRConfiguration(System.Configuration.Configuration configuration)
            {
                appSettings = configuration.AppSettings;
            }

            /// <summary>
            /// The port number used for communicating with the CSharp external backend worker process.
            /// </summary>
            internal virtual int GetPortNumber()
            {
                int portNo;
                if (!int.TryParse(Environment.GetEnvironmentVariable("CSHARPBACKEND_PORT"), out portNo))
                {
                    throw new Exception("Environment variable CSHARPBACKEND_PORT not set");
                }

                logger.LogInfo("CSharpBackend successfully read from environment variable CSHARPBACKEND_PORT");
                return portNo;
            }

            /// <summary>
            /// The short-name (filename part) of the CSharp external backend worker process.
            /// </summary>
            internal virtual string GetCSharpRDDExternalProcessName()
            {
                //SparkCLR jar and driver, worker & dependencies are shipped using Spark file server. These files available in spark executing directory at executor
                return "CSharpWorker.exe";
            }

            /// <summary>
            /// The full path of the CSharp external backend worker process.
            /// </summary>
            internal virtual string GetCSharpWorkerPath()
            {
                return new Uri(GetSparkCLRArtifactsPath("bin", "CSharpWorker.exe")).ToString();
            }

            /// <summary>
            /// List of the files required for the CSharp external backend worker process.
            /// </summary>
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

            private string workerPath;
            internal override string GetCSharpRDDExternalProcessName()
            {
                // SparkCLR jar and driver, worker & dependencies are shipped using Spark file server. 
                // These files are available in the Spark executing directory at executor node.

                if (workerPath != null) return workerPath; // Return cached value

                KeyValueConfigurationElement workerPathConfig = appSettings.Settings["CSharpWorkerPath"];
                if (workerPathConfig == null)
                {
                    // Path for the CSharpWorker.exe was not specified in App.config
                    // Try to work out where location relative to this class.
                    // Construct path based on well-known file name + directory this class was loaded from.
                    string procFileName = base.GetCSharpRDDExternalProcessName();
                    string procDir = Path.GetDirectoryName(GetType().Assembly.Location);
                    workerPath = Path.Combine(procDir, procFileName);
                    logger.LogDebug("Using synthesized value for CSharpWorkerPath : " + workerPath);
                }
                else
                {
                    // Explicit path for the CSharpWorker.exe was listed in App.config
                    workerPath = workerPathConfig.Value;
                    logger.LogDebug("Using CSharpWorkerPath value from App.config : " + workerPath);
                }
                return workerPath;
            }

            internal override string GetCSharpWorkerPath()
            {
                string workerPath = GetCSharpRDDExternalProcessName();
                return new Uri(workerPath).ToString();
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
                KeyValueConfigurationElement portConfig = appSettings.Settings["CSharpBackendPortNumber"];
                if (portConfig == null)
                {
                    throw new ConfigurationErrorsException("Need to set CSharpBackendPortNumber value in App.config for running in DEBUG mode.");
                }
                int cSharpBackendPortNumber = int.Parse(portConfig.Value);
                logger.LogInfo(string.Format("CSharpBackend port number read from app config {0}", cSharpBackendPortNumber));
                return cSharpBackendPortNumber;
            }
        }

        /// <summary>
        /// Configuration for SparkCLR jobs in ** Yarn-Cluster ** mode
        /// </summary>
        private class SparkCLRYarnClusterConfiguration : SparkCLRConfiguration
        {
            internal SparkCLRYarnClusterConfiguration(System.Configuration.Configuration configuration)
                : base(configuration)
            { }

            internal override string GetCSharpWorkerPath()
            {
                return "CSharpSparkWorker.exe";
            }
        }
    }

    public enum RunMode
    {
        UNKNOWN,
        DEBUG, //not a Spark mode but exists for dev debugging purpose
        LOCAL,
        CLUSTER,
        YARN,
        //following are not currently supported
        MESOS
    }
}
