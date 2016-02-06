// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Services;
using Microsoft.Spark.CSharp.Sql;

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// This example shows shows how to use C# to process XML as Spark DataFrame. 
    /// This sample implements the same example available at https://github.com/databricks/spark-xml#scala-api. 
    /// </summary>
    class SparkXmlExample
    {
        static void Main(string[] args)
        {
            LoggerServiceFactory.SetLoggerService(Log4NetLoggerService.Instance); //this is optional - DefaultLoggerService will be used if not set
            var logger = LoggerServiceFactory.GetLogger(typeof(SparkXmlExample));

            var inputXmlFilePath = args[0];
            var outputXmlFilePath = args[1];

            var sparkConf = new SparkConf();
            sparkConf.SetAppName("myapp");
            var sparkContext = new SparkContext(sparkConf);
            var sqlContext = new SqlContext(sparkContext);
            var df = sqlContext.Read()
                                .Format("com.databricks.spark.xml")
                                .Option("rowTag", "book")
                                .Load(inputXmlFilePath); //"D:\temp\books.xml", "file:/D:/temp/books.xml" or "hdfs://temp/books.xml"
            df.ShowSchema();
            var rowCount = df.Count();
            logger.LogInfo("Row count is " + rowCount);

            var selectedData = df.Select("author", "@id");

            selectedData.Write()
                        .Format("com.databricks.spark.xml")
                        .Option("rootTag", "books")
                        .Option("rowTag", "book")
                        .Save(outputXmlFilePath); //"D:\temp\booksUpdated.xml", "file:/D:/temp/booksUpdated.xml" or "hdfs://temp/booksUpdated.xml"

            sparkContext.Stop();
        }
    }
}
