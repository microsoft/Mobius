// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;

namespace Microsoft.Spark.CSharp.Samples
{
    class DataFrameSamples
    {
        private const string PeopleJson = @"people.json";
        private const string OrderJson = @"order.json";
        private const string RequestsLog = @"requestslog.txt";
        private const string MetricsLog = @"metricslog.txt";

        private static SqlContext sqlContext;

        private static SqlContext GetSqlContext()
        {
            return sqlContext ?? (sqlContext = new SqlContext(SparkCLRSamples.SparkContext));
        }

        /// <summary>
        /// Sample to show schema of DataFrame
        /// </summary>
        [Sample]
        internal static void DFShowSchemaSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.Explain(true);
            peopleDataFrame.ShowSchema();
        }

        /// <summary>
        /// Sample to register a DataFrame as temptable and run queries
        /// </summary>
        [Sample]
        internal static void DFRegisterTableSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            peopleDataFrame.RegisterTempTable("people");
            var nameFilteredDataFrame = GetSqlContext().Sql("SELECT name, address.city, address.state FROM people where name='Bill'");
            var countDataFrame = GetSqlContext().Sql("SELECT count(name) FROM people where name='Bill'");
            var maxAgeDataFrame = GetSqlContext().Sql("SELECT max(age) FROM people where name='Bill'");
            long maxAgeDataFrameRowsCount = maxAgeDataFrame.Count();
            long nameFilteredDataFrameRowsCount = nameFilteredDataFrame.Count();
            long countDataFrameRowsCount = countDataFrame.Count();
            Console.WriteLine("nameFilteredDataFrameRowsCount={0}, maxAgeDataFrameRowsCount={1}, countDataFrameRowsCount={2}", nameFilteredDataFrameRowsCount, maxAgeDataFrameRowsCount, countDataFrameRowsCount);
        }

        /// <summary>
        /// Sample to load a text file as dataframe
        /// </summary>
        [Sample]
        internal static void DFTextFileLoadDataFrameSample()
        {
            var requestsSchema = StructType.CreateStructType(
                new List<StructField>
                {
                    StructField.CreateStructField("guid", "string", false),
                    StructField.CreateStructField("datacenter", "string", false),
                    StructField.CreateStructField("abtestid", "string", false),
                    StructField.CreateStructField("traffictype", "string", false),
                }
                );

            var requestsDateFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(RequestsLog), requestsSchema);
            requestsDateFrame.RegisterTempTable("requests");
            var guidFilteredDataFrame = GetSqlContext().Sql("SELECT guid, datacenter FROM requests where guid = '4628deca-139d-4121-b540-8341b9c05c2a'");
            guidFilteredDataFrame.Show();

            requestsDateFrame.ShowSchema();
            requestsDateFrame.Show();
            //var count = requestsDateFrame.Count();

            guidFilteredDataFrame.ShowSchema();
            guidFilteredDataFrame.Show();
            //var filteredCount = guidFilteredDataFrame.Count();
        }

        private static DataFrame GetMetricsDataFrame()
        {
            var metricsSchema = StructType.CreateStructType(
                new List<StructField>
                {
                    StructField.CreateStructField("unknown", "string", false),
                    StructField.CreateStructField("date", "string", false),
                    StructField.CreateStructField("time", "string", false),
                    StructField.CreateStructField("guid", "string", false),
                    StructField.CreateStructField("lang", "string", false),
                    StructField.CreateStructField("country", "string", false),
                    StructField.CreateStructField("latency", "integer", false)
                }
                );

            return
                GetSqlContext()
                    .TextFile(SparkCLRSamples.Configuration.GetInputDataPath(MetricsLog), metricsSchema);
        }

        /// <summary>
        /// Sample to load two text files and join them using temptable constructs
        /// </summary>
        [Sample]
        internal static void DFTextFileJoinTempTableSample()
        {
            var requestsDataFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(RequestsLog));
            var metricsDateFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(MetricsLog));
            metricsDateFrame.ShowSchema();

            requestsDataFrame.RegisterTempTable("requests");
            metricsDateFrame.RegisterTempTable("metrics");

            //C0 - guid in requests DF, C3 - guid in metrics DF
            var join = GetSqlContext().Sql(
                        "SELECT joinedtable.datacenter, max(joinedtable.latency) maxlatency, avg(joinedtable.latency) avglatency " +
                        "FROM (SELECT a.C1 as datacenter, b.C6 as latency from requests a JOIN metrics b ON a.C0  = b.C3) joinedtable " +
                        "GROUP BY datacenter");
            
            join.ShowSchema();
            join.Show();
            //var count = join.Count();    
        }

        /// <summary>
        /// Sample to load two text files and join them using DataFrame DSL
        /// </summary>
        [Sample]
        internal static void DFTextFileJoinTableDSLSample()
        {
            //C0 - guid, C1 - datacenter
            var requestsDataFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(RequestsLog)).Select("C0", "C1");
            //C3 - guid, C6 - latency
            var metricsDateFrame = GetSqlContext().TextFile(SparkCLRSamples.Configuration.GetInputDataPath(MetricsLog), ",", false, true).Select("C3", "C6"); //override delimiter, hasHeader & inferSchema

            var joinDataFrame = requestsDataFrame.Join(metricsDateFrame, requestsDataFrame["C0"] == metricsDateFrame["C3"]).GroupBy("C1");
            var maxLatencyByDcDataFrame = joinDataFrame.Agg(new Dictionary<string, string> { { "C6", "max" } });
            var avgLatencyByDcDataFrame = joinDataFrame.Agg(new Dictionary<string, string> { { "C6", "avg" } });

            maxLatencyByDcDataFrame.ShowSchema();
            maxLatencyByDcDataFrame.Show();
            
            avgLatencyByDcDataFrame.ShowSchema();
            avgLatencyByDcDataFrame.Show();
        }

        /// <summary>
        /// Sample to iterate on the schema of DataFrame
        /// </summary>
        [Sample]
        internal static void DFSchemaSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var peopleDataFrameSchema = peopleDataFrame.Schema;
            var peopleDataFrameSchemaFields = peopleDataFrameSchema.Fields;
            foreach (var peopleDataFrameSchemaField in peopleDataFrameSchemaFields)
            {
                var name = peopleDataFrameSchemaField.Name;
                var dataType = peopleDataFrameSchemaField.DataType;
                var stringVal = dataType.ToString();
                var simpleStringVal = dataType.SimpleString();
                var isNullable = peopleDataFrameSchemaField.IsNullable;
                Console.WriteLine("Name={0}, DT.string={1}, DT.simplestring={2}, DT.isNullable={3}", name, stringVal, simpleStringVal, isNullable);
            }
        }

        /// <summary>
        /// Sample to convert DataFrames to RDD
        /// </summary>
        [Sample]
        internal static void DFConversionSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var stringRddCreatedFromDataFrame = peopleDataFrame.ToJSON();
            var stringRddCreatedFromDataFrameRowCount = stringRddCreatedFromDataFrame.Count();

            var byteArrayRddCreatedFromDataFrame = peopleDataFrame.ToRDD();
            var byteArrayRddCreatedFromDataFrameRowCount = byteArrayRddCreatedFromDataFrame.Count();

            Console.WriteLine("stringRddCreatedFromDataFrameRowCount={0}, byteArrayRddCreatedFromDataFrameRowCount={1}", stringRddCreatedFromDataFrameRowCount, byteArrayRddCreatedFromDataFrameRowCount);
        }

        /// <summary>
        /// Sample to perform simple select and filter on DataFrame using DSL
        /// </summary>
        [Sample]
        internal static void DFProjectionFilterDSLSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson)); 
            var projectedFilteredDataFrame = peopleDataFrame.Select("name", "address.state")
                                                    .Where("name = 'Bill' or state = 'California'");
            projectedFilteredDataFrame.ShowSchema();
            projectedFilteredDataFrame.Show();
            var projectedFilteredDataFrameCount = projectedFilteredDataFrame.Count();
            projectedFilteredDataFrame.RegisterTempTable("selectedcolumns");
            var sqlCountDataFrame = GetSqlContext().Sql("SELECT count(name) FROM selectedcolumns where name='Bill'");
            var sqlCountDataFrameRowCount = sqlCountDataFrame.Count();
            Console.WriteLine("projectedFilteredDataFrameCount={0}, sqlCountDataFrameRowCount={1}", projectedFilteredDataFrameCount, sqlCountDataFrameRowCount);
        }

        /// <summary>
        /// Sample to join DataFrame using DSL
        /// </summary>
        [Sample]
        internal static void DFJoinSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var orderDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(OrderJson));
            
            //Following example does not work in Spark 1.4.1 - uncomment in 1.5
            //var peopleDataFrame2 = GetSqlContext().JsonFile(Samples.GetInputDataPath(PeopleJson));
            //var columnNameJoin = peopleDataFrame.Join(peopleDataFrame2, new string[] {"id"});
            //columnNameJoin.Show();
            //columnNameJoin.ShowDF();

            var expressionJoin = peopleDataFrame.Join(orderDataFrame, peopleDataFrame["id"] == orderDataFrame["personid"]);
            expressionJoin.ShowSchema();
            expressionJoin.Show();
        }

        /// <summary>
        /// Sample to perform aggregatoin on DataFrame using DSL
        /// </summary>
        [Sample]
        internal static void DFAggregateDSLSample()
        {
            var peopleDataFrame = GetSqlContext().JsonFile(SparkCLRSamples.Configuration.GetInputDataPath(PeopleJson));
            var countAggDataFrame = peopleDataFrame.Where("name = 'Bill'").Agg(new Dictionary<string, string> {{"name", "count"}});
            var countAggDataFrameCount = countAggDataFrame.Count();
            var maxAggDataFrame = peopleDataFrame.GroupBy("name").Agg(new Dictionary<string, string> {{"age", "max"}});
            var maxAggDataFrameCount = maxAggDataFrame.Count();
            Console.WriteLine("countAggDataFrameCount: {0}, maxAggDataFrameCount: {1}.", countAggDataFrameCount, maxAggDataFrameCount);
        }
    }
}
