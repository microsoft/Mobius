// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Microsoft.Spark.CSharp.Proxy
{
    internal interface ISparkConfProxy
    {
        void SetMaster(string master);
        void SetAppName(string appName);
        void SetSparkHome(string sparkHome);
        void Set(string key, string value);
        int GetInt(string key, int defaultValue);
        string Get(string key, string defaultValue);
        string GetSparkConfAsString();
    }
}
