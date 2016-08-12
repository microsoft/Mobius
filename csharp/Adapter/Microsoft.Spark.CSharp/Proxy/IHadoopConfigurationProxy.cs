// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Spark.CSharp.Proxy
{
    interface IHadoopConfigurationProxy
    {
        void Set(string name, string value);
        string Get(string name, string defaultValue);
    }
}
