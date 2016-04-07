// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using Microsoft.Spark.CSharp.Sql;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Microsoft.Spark.CSharp.Samples
{
    class HiveContextSamples
    {
        private const string PeopleJson = @"people.json";
        private const string RequestsLog = @"requestslog.txt";

        private static HiveContext hiveContext;

        private static SqlContext GetHiveContext()
        {
            return hiveContext ?? (hiveContext = new HiveContext(SparkCLRSamples.SparkContext));
        }

        /// <summary>
        /// Sample to SaveAsTable and Tables
        /// </summary>
        [Sample]
        internal static void HiveContextSaveAsTableAndTablesSample()
        {
            var hiveContext = GetHiveContext();
            var peopleDataFrame = hiveContext.Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));

            const string dbName = "SampleDataBase";
            const string tableName = "people";

            // create database and delete table 'people' if exists
            hiveContext.Sql(string.Format("CREATE DATABASE IF NOT EXISTS {0}", dbName));
            hiveContext.Sql(string.Format("DROP TABLE {0}", tableName));

            // SaveAsTable
            peopleDataFrame.Write().SaveAsTable("people");
            var tablesDataFrame = hiveContext.Tables(dbName);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                var collected = tablesDataFrame.Collect().ToArray();
                Assert.IsTrue(collected.Any(row => "people" == row.GetAs<string>("tableName")));
            }
        }
    }
}
