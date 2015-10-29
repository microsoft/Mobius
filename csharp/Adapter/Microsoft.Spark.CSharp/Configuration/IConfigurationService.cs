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
        int BackendPortNumber { get; }
        string GetCSharpRDDExternalProcessName();
        string GetCSharpWorkerPath();
        IEnumerable<string> GetDriverFiles();
    }
}
