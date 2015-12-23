// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using Microsoft.Spark.CSharp.Sql;
using NUnit.Framework;

namespace Microsoft.Spark.CSharp.Samples
{
    class SqlContextSamples
    {
        private const string PeopleJson = @"people.json";

        private static SqlContext sqlContext;

        private static SqlContext GetSqlContext()
        {
            return sqlContext ?? (sqlContext = new SqlContext(SparkCLRSamples.SparkContext));
        }

        /// <summary>
        /// DataFrameReader read json sample
        /// </summary>
        [Sample]
        internal static void SqlContextReadJsonSample()
        {
            // load json
            var peopleDataFrame = GetSqlContext().Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.ShowSchema();
            peopleDataFrame.Show();

            var count1 = peopleDataFrame.Count();
            Console.WriteLine("Line count in people.json: {0}", count1);

            // load json with schema sample
            var peopleDataFrameWitSchema = GetSqlContext().Read().Schema(peopleDataFrame.Schema).Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrameWitSchema.ShowSchema();
            peopleDataFrameWitSchema.Show();

            var count2 = peopleDataFrameWitSchema.Count();
            Console.WriteLine("Line count in people.json: {0}", count2);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(count1, count2);
                Assert.AreEqual(4, count1);
            }
        }

        /// <summary>
        /// DataFrameReader read json with option sample
        /// </summary>
        [Sample]
        internal static void SqlContextReadJsonWithOptionSample()
        {
            // load json with sampling ration option sample
            // set sampling ration to a very little value so that schema probably won't be infered.
            var peopleDataFrame = GetSqlContext().Read().Option("samplingRatio", "0.001").Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.ShowSchema();
            peopleDataFrame.Show();

            var count1 = peopleDataFrame.Count();
            Console.WriteLine("Line count in people.json: {0}", count1);
        }

        /// <summary>
        /// DataFrameReader read parquet sample
        /// </summary>
        [Sample]
        internal static void SqlContextReadParquetSample()
        {
            // save json file to parquet file first
            var peopleDataFrame = GetSqlContext().Read().Json(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var expectedCount = peopleDataFrame.Count();

            var parquetPath = Path.GetTempPath() + "DF_Parquet_Samples_" + (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0)).TotalSeconds;
            peopleDataFrame.Coalesce(1).Write().Parquet(parquetPath);

            Console.WriteLine("Save dataframe to parquet: {0}", parquetPath);

            var peopleDataFrame2 = GetSqlContext().Read().Parquet(parquetPath);
            Console.WriteLine("Schema:");
            peopleDataFrame2.ShowSchema();
            peopleDataFrame2.Show();

            var count = peopleDataFrame2.Count();
            Console.WriteLine("Rows in peopleDataFrame2: {0}", count);

            if (SparkCLRSamples.Configuration.IsValidationEnabled)
            {
                Assert.AreEqual(expectedCount, count);
            }

            Directory.Delete(parquetPath, true);
            Console.WriteLine("Remove parquet directory: {0}", parquetPath);
        }
    }
}
