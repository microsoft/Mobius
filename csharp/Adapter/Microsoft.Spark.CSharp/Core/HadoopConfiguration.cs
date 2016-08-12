// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Configuration for Hadoop operations
    /// </summary>
    public class HadoopConfiguration
    {
        private readonly IHadoopConfigurationProxy hadoopConfigurationProxy;
        internal HadoopConfiguration(IHadoopConfigurationProxy hadoopConfProxy)
        {
            hadoopConfigurationProxy = hadoopConfProxy;
        }

        /// <summary>
        /// Sets a property value to HadoopConfiguration
        /// </summary>
        /// <param name="name">Name of the property</param>
        /// <param name="value">Value of the property</param>
        public void Set(string name, string value)
        {
            hadoopConfigurationProxy.Set(name, value);
        }

        /// <summary>
        /// Gets the value of a property from HadoopConfiguration
        /// </summary>
        /// <param name="name">Name of the property</param>
        /// <param name="defaultValue">Default value if the property is not available in the configuration</param>
        /// <returns></returns>
        public string Get(string name, string defaultValue)
        {
            return hadoopConfigurationProxy.Get(name, defaultValue);
        }
    }
}
