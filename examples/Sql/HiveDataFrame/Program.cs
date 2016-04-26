// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql;
using System.IO;

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// This example shows how to use Hive in SparkCLR's C# API for Apache Spark DataFrame to
    /// load data from Hive.
    /// </summary>
    class HiveDataFrameExample
    {
        static void Main(string[] args)
        {
            LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance); //this is optional - DefaultLoggerService will be used if not set
            var logger = LoggerServiceFactory.GetLogger(typeof(HiveDataFrameExample));

            var sparkConf = new SparkConf();
            var sparkContext = new SparkContext(sparkConf);
            var hiveContext = new HiveContext(sparkContext);
            var peopleDataFrame = hiveContext.Read().Json(Path.Combine(Environment.CurrentDirectory, @"data\people.json"));

            const string dbName = "SampleHiveDataBaseForMobius";
            const string tableName = "people";
            
            hiveContext.Sql(string.Format("CREATE DATABASE IF NOT EXISTS {0}", dbName)); // create database if not exists
            hiveContext.Sql(string.Format("USE {0}", dbName));
            hiveContext.Sql(string.Format("DROP TABLE {0}", tableName)); // drop table if exists

            peopleDataFrame.Write().Mode(SaveMode.Overwrite).SaveAsTable(tableName); // create table
            var tablesDataFrame = hiveContext.Tables(dbName); // get all tables in database
            logger.LogInfo(string.Format("table count in database {0}: {1}", dbName, tablesDataFrame.Count()));
            tablesDataFrame.Show();

            hiveContext.Sql(string.Format("SELECT * FROM {0}", tableName)).Show(); // select from table
        }
    }
}
