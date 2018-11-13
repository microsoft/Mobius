// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Moq;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validates implementation of configuration service that helps getting config settings
    /// to be used in SparkCLR runtime
    /// </summary>
    [TestFixture]
    class ConfigurationServiceTest
    {
        // Set environment variable with new value, and return old value for enviroment reset after each test finishes
        internal string SetEnviromentVariable(string name, string value)
        {
            string oldValue = Environment.GetEnvironmentVariable(name);
            Environment.SetEnvironmentVariable(name, value);
            return oldValue;
        }

        [Test]
        public void TestDebugConfiguration()
        {
            string oldSparkClrHome = SetEnviromentVariable(ConfigurationService.SPARKCLR_HOME, Path.GetTempPath());
            try
            {
                ConfigurationService debugConfiguration = new ConfigurationService();
                Assert.AreEqual(ConfigurationService.CSHARPBACKEND_DEBUG_PORT, debugConfiguration.BackendPortNumber);
                Assert.Throws<Exception>(() => Console.WriteLine(debugConfiguration.GetCSharpWorkerExePath()));
            }
            finally
            {
                SetEnviromentVariable(ConfigurationService.SPARKCLR_HOME, oldSparkClrHome);
            }
        }

        [Test]
        public void TestLocalConfiguration()
        {
            string oldSparkClrHome = SetEnviromentVariable(ConfigurationService.SPARKCLR_HOME, Path.GetTempPath());
            string oldSparkMaster = SetEnviromentVariable(ConfigurationService.SPARK_MASTER, "local[3]");
            try
            {
                ConfigurationService localConfiguration = new ConfigurationService();
                Assert.IsNotNull(localConfiguration.GetCSharpWorkerExePath());
            }
            finally
            {
                SetEnviromentVariable(ConfigurationService.SPARKCLR_HOME, oldSparkClrHome);
                SetEnviromentVariable(ConfigurationService.SPARK_MASTER, oldSparkMaster);
            }
        }

        [Test]
        public void TestConfigurationForStandaloneCluster()
        {
            TestConfiguration("spark://hostname:port");
        }

        [Test]
        public void TestConfigurationForYarnCluster()
        {
            TestConfiguration("yarn-client");
            TestConfiguration("yarn-cluster");
        }

        [Test]
        public void TestConfigurationWithInvalidSparkMaster()
        {
            Assert.Throws<NotSupportedException>(() => TestConfiguration("invalid-master"));
        }

        internal void TestConfiguration(string sparkMaster)
        {
            string oldSparkClrHome = SetEnviromentVariable(ConfigurationService.SPARKCLR_HOME, Path.GetTempPath());
            string oldSparkMaster = SetEnviromentVariable(ConfigurationService.SPARK_MASTER, sparkMaster);
            string oldPort = "NOT_SET";
            try
            {
                ConfigurationService localConfiguration = new ConfigurationService();
                Assert.IsNotNull(localConfiguration.GetCSharpWorkerExePath());
                Assert.Throws<Exception>(() => Console.WriteLine(localConfiguration.BackendPortNumber));
                const int backendPort = 1108;

                oldPort = SetEnviromentVariable(ConfigurationService.CSHARPBACKEND_PORT, Convert.ToString(backendPort));

                Assert.AreEqual(backendPort, localConfiguration.BackendPortNumber);
                Assert.AreEqual(ConfigurationService.ProcFileName, localConfiguration.GetCSharpWorkerExePath());
            }
            finally
            {
                SetEnviromentVariable(ConfigurationService.SPARKCLR_HOME, oldSparkClrHome);
                SetEnviromentVariable(ConfigurationService.SPARK_MASTER, oldSparkMaster);
                if (oldPort != "NOT_SET")
                {
                    SetEnviromentVariable(ConfigurationService.CSHARPBACKEND_PORT, oldPort);
                }
            }
        }
    }
}
