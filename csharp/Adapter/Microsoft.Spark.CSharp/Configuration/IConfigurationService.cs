// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Spark.CSharp.Configuration
{
    /// <summary>
    /// Helps getting config settings to be used in SparkCLR runtime
    /// </summary>
    internal interface IConfigurationService
    {
        /// <summary>
        /// The port number used for communicating with the CSharp external backend worker process.
        /// </summary>
        int BackendPortNumber { get; }
        /// <summary>
        /// The full path of the CSharp external backend worker process executable.
        /// </summary>
        string GetCSharpWorkerExePath();
    }
}
