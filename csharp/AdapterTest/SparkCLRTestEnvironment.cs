// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using NUnit.Framework;

namespace AdapterTest
{
    [SetUpFixture]
    public class SparkCLRTestEnvironment 
    {
        [OneTimeSetUp]
        public static void Initialize()
        {
            SparkCLREnvironment.SparkCLRProxy = new MockSparkCLRProxy();
            SparkCLREnvironment.ConfigurationService = new MockConfigurationService();
        }
    }
}
