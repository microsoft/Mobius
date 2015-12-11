// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.PerfBenchmark
{
    /// <summary>
    /// Perf benchmark that users Freebase deletions data
    /// This data is licensed under CC-BY license (http://creativecommons.org/licenses/by/2.5)
    /// Data is available for download at https://developers.google.com/freebase/data)
    /// Data format - CSV, size - 8 GB uncompressed
    /// Columns in the dataset are
    ///     1. creation_timestamp (Unix epoch time in milliseconds)
    ///     2. creator
    ///     3. deletion_timestamp (Unix epoch time in milliseconds)
    ///     4. deletor
    ///     5. subject (MID)
    ///     6. predicate (MID)
    ///     7. object (MID/Literal)
    ///     8. language_code
    /// </summary>
    class FreebaseDeletionsBenchmark
    {
        private static readonly Stopwatch stopwatch = new Stopwatch();

        [PerfSuite]
        internal static void RunRDDLineCount(string[] args)
        {
            string filePath = args[2].StartsWith(@"hdfs://") ? args[2] : new Uri(args[2]).ToString();
            stopwatch.Restart();

            var lines = PerfBenchmark.SparkContext.TextFile(filePath);
            var count = lines.Count();

            stopwatch.Stop();
            PerfBenchmark.ExecutionTimeList.Add(stopwatch.Elapsed);

            Console.WriteLine("Count of lines {0}. Time elapsed {1}", count, stopwatch.Elapsed);
        }

        [PerfSuite]
        internal static void RunRDDMaxDeletionsByUser(string[] args)
        {
            string filePath = args[2].StartsWith(@"hdfs://") ? args[2] : new Uri(args[2]).ToString();
            stopwatch.Restart();

            var lines = PerfBenchmark.SparkContext.TextFile(filePath);
            var parsedRows = lines.Map(s =>
            {
                var columns = s.Split(new[] {','});

                //data has some bad records - use bool flag to indicate corrupt rows
                if (columns.Length > 4)
                    return new Tuple<bool, string, string, string, string>(true, columns[0], columns[1], columns[2], columns[3]);
                else
                    return new Tuple<bool, string, string, string, string>(false, "X", "X", "X", "X"); //invalid row placeholder
            });

            var flaggedRows = parsedRows.Filter(s => s.Item1); //select good rows
            var selectedDeletions = flaggedRows.Filter(s => s.Item3.Equals(s.Item5)); //select deletions made by same creators
            var userDeletions = selectedDeletions.Map(s => new KeyValuePair<string, int>(s.Item3, 1));
            var userDeletionCount = userDeletions.ReduceByKey((x, y) => x + y);
            var userWithMaxDeletions = userDeletionCount.Fold(new KeyValuePair<string, int>("zerovalue", 0), (kvp1, kvp2) =>
            {
                if (kvp1.Value > kvp2.Value)
                    return kvp1;
                else
                    return kvp2;
            });
            
            stopwatch.Stop();
            PerfBenchmark.ExecutionTimeList.Add(stopwatch.Elapsed);

            Console.WriteLine("User with max deletions is {0}, count of deletions={1}. Elapsed time={2}", userWithMaxDeletions.Key, userWithMaxDeletions.Value, stopwatch.Elapsed);
        }

        [PerfSuite]
        internal static void RunDFLineCount(string[] args)
        {
            stopwatch.Restart();

            var rows = PerfBenchmark.SqlContext.TextFile(args[2]);
            var rowCount = rows.Count();

            stopwatch.Stop();
            PerfBenchmark.ExecutionTimeList.Add(stopwatch.Elapsed);
            Console.WriteLine("Count of rows {0}. Time elapsed {1}", rowCount, stopwatch.Elapsed);
        }

        [PerfSuite]
        internal static void RunDFMaxDeletionsByUser(string[] args)
        {
            stopwatch.Restart();

            var rows = PerfBenchmark.SqlContext.TextFile(args[2]);
            var filtered = rows.Filter("C1 = C3");
            var aggregated = filtered.GroupBy("C1").Agg(new Dictionary<string, string> { { "C1", "count" } });
            aggregated.RegisterTempTable("freebasedeletions");
            var max = PerfBenchmark.SqlContext.Sql("select max(`COUNT(C1)`) from freebasedeletions");
            var maxArray = max.Collect();
            var maxValue = maxArray.First();
            var maxDeletions = PerfBenchmark.SqlContext.Sql("select * from freebasedeletions where `COUNT(C1)` = " + maxValue.Get(0));
            maxDeletions.Show();
            //TODO - add perf suite for subquery

            stopwatch.Stop();
            PerfBenchmark.ExecutionTimeList.Add(stopwatch.Elapsed);
            Console.WriteLine("User with max deletions & count of deletions is listed above. Time elapsed {0}", stopwatch.Elapsed);
            
        }
        
       
    }
}
