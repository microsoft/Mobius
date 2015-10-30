// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Runtime.CompilerServices;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;

[assembly: InternalsVisibleTo("AdapterTest")]
namespace Microsoft.Spark.CSharp.Interop
{
    /// <summary>
    /// Contains everything needed to setup an environment for using C# with Spark
    /// </summary>
    public class SparkCLREnvironment
    {
        private static ISparkCLRProxy sparkCLRProxy;
        internal static ISparkCLRProxy SparkCLRProxy
        {
            get
            {
                if (sparkCLRProxy == null)
                {
                    // TO DO: should get from app.config first, if not configured, then default to IPC
                    sparkCLRProxy = new SparkCLRIpcProxy();
                }
                return sparkCLRProxy;
            }
            set
            {
                sparkCLRProxy = value;   // for plugging test environment
            }
        }

        internal static IConfigurationService configurationService;

        internal static IConfigurationService ConfigurationService
        {
            get
            {
                if (configurationService == null)
                    configurationService = new ConfigurationService();
                return configurationService;
            }
            set
            {
                configurationService = value;
            }
        }
    }
}
