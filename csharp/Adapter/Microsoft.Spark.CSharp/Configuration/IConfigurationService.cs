// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        /// The short-name (filename part) of the CSharp external backend worker process.
        /// </summary>
        string GetCSharpRDDExternalProcessName();
        /// <summary>
        /// The full path of the CSharp external backend worker process.
        /// </summary>
        string GetCSharpWorkerPath();
        /// <summary>
        /// List of the files required for the CSharp external backend worker process.
        /// </summary>
        IEnumerable<string> GetDriverFiles();
    }
}
