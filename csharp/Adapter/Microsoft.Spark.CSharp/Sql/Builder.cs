// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// The entry point to programming Spark with the Dataset and DataFrame API.
    /// </summary>
    public class Builder
    {
        internal Dictionary<string, string> options = new Dictionary<string, string>();

        internal Builder() { }

        /// <summary>
        /// Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to
        /// run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
        /// </summary>
        /// <param name="master">Master URL</param>
        public Builder Master(string master)
        {
            Config("spark.master", master);
            return this;
        }

        /// <summary>
        /// Sets a name for the application, which will be shown in the Spark web UI.
        /// If no application name is set, a randomly generated name will be used.
        /// </summary>
        /// <param name="appName">Name of the app</param>
        public Builder AppName(string appName)
        {
            Config("spark.app.name", appName);
            return this;
        }

        /// <summary>
        /// Sets a config option. Options set using this method are automatically propagated to
        /// both SparkConf and SparkSession's own configuration.
        /// </summary>
        /// <param name="key">Key for the configuration</param>
        /// <param name="value">value of the configuration</param>
        public Builder Config(string key, string value)
        {
            options[key] = value;
            return this;
        }

        /// <summary>
        /// Sets a config option. Options set using this method are automatically propagated to
        /// both SparkConf and SparkSession's own configuration.
        /// </summary>
        /// <param name="key">Key for the configuration</param>
        /// <param name="value">value of the configuration</param>
        public Builder Config(string key, bool value)
        {
            options[key] = value.ToString();
            return this;
        }

        /// <summary>
        /// Sets a config option. Options set using this method are automatically propagated to
        /// both SparkConf and SparkSession's own configuration.
        /// </summary>
        /// <param name="key">Key for the configuration</param>
        /// <param name="value">value of the configuration</param>
        public Builder Config(string key, double value)
        {
            options[key] = value.ToString();
            return this;
        }

        /// <summary>
        /// Sets a config option. Options set using this method are automatically propagated to
        /// both SparkConf and SparkSession's own configuration.
        /// </summary>
        /// <param name="key">Key for the configuration</param>
        /// <param name="value">value of the configuration</param>
        public Builder Config(string key, long value)
        {
            options[key] = value.ToString();
            return this;
        }

        /// <summary>
        /// Sets a list of config options based on the given SparkConf
        /// </summary>
        public Builder Config(SparkConf conf)
        {
            foreach (var keyValuePair in conf.GetAll())
            {
                options[keyValuePair.Key] = keyValuePair.Value;
            }

            return this;
        }
        
        /// <summary>
        /// Enables Hive support, including connectivity to a persistent Hive metastore, support for
        /// Hive serdes, and Hive user-defined functions.
        /// </summary>
        public Builder EnableHiveSupport()
        {
            return Config("spark.sql.catalogImplementation", "hive");
        }

        /// <summary>
        /// Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
        /// one based on the options set in this builder.
        /// </summary>
        /// <returns></returns>
        public SparkSession GetOrCreate()
        {
            var sparkConf = new SparkConf();
            foreach (var option in options)
            {
                sparkConf.Set(option.Key, option.Value);
            }
            var sparkContext = SparkContext.GetOrCreate(sparkConf);
            return SqlContext.GetOrCreate(sparkContext).SparkSession;
        }
    }
}
