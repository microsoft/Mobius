// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql;

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// This example shows how to use JDBC in SparkCLR's C# API for Apache Spark DataFrame to
    /// load data from SQL Server. The connection url need to be updated for a different JDBC source.
    /// </summary>
    class JdbcDataFrameExample
    {
        static void Main(string[] args)
        {
            LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance); //this is optional - DefaultLoggerService will be used if not set
            var logger = LoggerServiceFactory.GetLogger(typeof(JdbcDataFrameExample));

            //For SQL Server use the connection string formats below
            //"jdbc:sqlserver://localhost:1433;databaseName=Temp;integratedSecurity=true;" or
            //"jdbc:sqlserver://localhost;databaseName=Temp;user=MyUserName;password=myPassword;"
            var connectionString = args[0];
            var tableName = args[1];

            var sparkConf = new SparkConf();
            var sparkContext = new SparkContext(sparkConf);
            var sqlContext = new SqlContext(sparkContext);
            var df = sqlContext
                        .Read()
                        .Jdbc(connectionString, tableName, new Dictionary<string, string>());
            df.ShowSchema();
            var rowCount = df.Count();
            logger.LogInfo("Row count is " + rowCount);
            sparkContext.Stop();
        }
    }
}
