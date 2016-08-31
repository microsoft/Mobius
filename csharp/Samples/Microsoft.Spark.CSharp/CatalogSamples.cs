// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Microsoft.Spark.CSharp.Samples
{
    class CatalogSamples
    {
        [Sample]
        internal static void CatalogSample()
        {
            var catalog = SparkSessionSamples.GetSparkSession().Catalog;
            var currentDatabase = catalog.CurrentDatabase;
            var databasesList = SparkSessionSamples.GetSparkSession().Catalog.ListDatabases().Collect();
            
            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var defaultDatabase = databasesList.First(row => row.Get("name").Equals("default")); //throws exception if First() is missing
            }
        }
    }
}
