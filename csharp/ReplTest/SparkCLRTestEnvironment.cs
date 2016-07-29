// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using AdapterTest;
using AdapterTest.Mocks;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;
using NUnit.Framework;

namespace ReplTest
{
    [SetUpFixture]
    public class SparkCLRTestEnvironment
    {
        [OneTimeSetUp]
        public static void Initialize()
        {
            Console.WriteLine("Running SparkCLRTestEnvironment Initialize()");
            SparkCLREnvironment.SparkCLRProxy = new MockSparkCLRProxy();
            SparkCLREnvironment.ConfigurationService = new MockConfigurationService();
            SparkCLREnvironment.WeakObjectManager = new WeakObjectManagerImpl
            {
                ObjectReleaser = new MockObjectReleaser(),
            };
        }
    }
}
