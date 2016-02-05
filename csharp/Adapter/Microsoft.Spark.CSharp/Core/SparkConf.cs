// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
    ///
    /// Note that once a SparkConf object is passed to Spark, it is cloned and can no longer be modified
    /// by the user. Spark does not support modifying the configuration at runtime.
    /// 
    /// See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkConf
    /// </summary>
    public class SparkConf
    {
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(SparkConf));
        private readonly ISparkConfProxy sparkConfProxy;
        internal ISparkConfProxy SparkConfProxy { get { return sparkConfProxy; } }

        /// <summary>
        /// when created from checkpoint
        /// </summary>
        /// <param name="sparkConfProxy"></param>
        internal SparkConf(ISparkConfProxy sparkConfProxy)
        {
            this.sparkConfProxy = sparkConfProxy;
        }

        /// <summary>
        /// Create SparkConf
        /// </summary>
        /// <param name="loadDefaults">indicates whether to also load values from Java system properties</param>
        public SparkConf(bool loadDefaults = true)
        {
            sparkConfProxy = SparkCLREnvironment.SparkCLRProxy.CreateSparkConf(loadDefaults);

            //special handling for debug mode because
            //spark.master and spark.app.name will not be set in debug mode
            //driver code may override these values if SetMaster or SetAppName methods are used
            if (string.IsNullOrWhiteSpace(Get("spark.master", "")))
            {
                logger.LogInfo("spark.master not set. Assuming debug mode.");
                SetMaster("local");
            }
            if (string.IsNullOrWhiteSpace(Get("spark.app.name", "")))
            {
                logger.LogInfo("spark.app.name not set. Assuming debug mode");
                SetAppName("debug app");
            }
        }

        /// <summary>
        /// The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to 
        /// run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
        /// </summary>
        /// <param name="master">Spark master</param>
        public SparkConf SetMaster(string master)
        {
            sparkConfProxy.SetMaster(master);
            logger.LogInfo("Spark master set to {0}", master);
            return this;
        }

        /// <summary>
        /// Set a name for your application. Shown in the Spark web UI.
        /// </summary>
        /// <param name="appName">Name of the app</param>
        public SparkConf SetAppName(string appName)
        {
            sparkConfProxy.SetAppName(appName);
            logger.LogInfo("Spark app name set to {0}", appName);
            return this;
        }

        /// <summary>
        /// Set the location where Spark is installed on worker nodes.
        /// </summary>
        /// <param name="sparkHome"></param>
        /// <returns></returns>
        public SparkConf SetSparkHome(string sparkHome)
        {
            sparkConfProxy.SetSparkHome(sparkHome);
            logger.LogInfo("Spark home set to {0}", sparkHome);
            return this;
        }

        /// <summary>
        /// Set the value of a string config
        /// </summary>
        /// <param name="key">Config name</param>
        /// <param name="value">Config value</param>
        public SparkConf Set(string key, string value)
        {
            sparkConfProxy.Set(key, value);
            logger.LogInfo("Spark configuration key-value set to {0}={1}", key, value);
            return this;
        }

        /// <summary>
        /// Get a int parameter value, falling back to a default if not set
        /// </summary>
        /// <param name="key">Key to use</param>
        /// <param name="defaultValue">Default value to use</param>
        public int GetInt(string key, int defaultValue)
        {
            return sparkConfProxy.GetInt(key, defaultValue);
        }


        /// <summary>
        /// Get a string parameter value, falling back to a default if not set
        /// </summary>
        /// <param name="key">Key to use</param>
        /// <param name="defaultValue">Default value to use</param>
        public string Get(string key, string defaultValue)
        {
            return sparkConfProxy.Get(key, defaultValue);
        }
    }
    


}
