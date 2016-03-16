// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    [ExcludeFromCodeCoverage] //IPC calls to JVM validated using validation-enabled samples - unit test coverage not reqiured
    internal class DataFrameIpcProxy : IDataFrameProxy
    {
        private readonly JvmObjectReference jvmDataFrameReference;

        internal JvmObjectReference JvmDataFrameReference
        {
            get { return jvmDataFrameReference; }
        }

        private readonly ISqlContextProxy sqlContextProxy;

        private readonly DataFrameNaFunctions na;
        private readonly DataFrameStatFunctions stat;

        internal DataFrameIpcProxy(JvmObjectReference jvmDataFrameReference, ISqlContextProxy sqlProxy)
        {
            this.jvmDataFrameReference = jvmDataFrameReference;
            sqlContextProxy = sqlProxy;
            na = new DataFrameNaFunctions(jvmDataFrameReference);
            stat = new DataFrameStatFunctions(jvmDataFrameReference);
        }

        public void RegisterTempTable(string tableName)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameReference,
                "registerTempTable", new object[] { tableName });
        }

        public long Count()
        {
            return
                long.Parse(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "count").ToString());
        }

        public IRDDProxy JavaToCSharp()
        {
            var javaRDDReference = new JvmObjectReference(
                (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameReference, "javaToPython"));

            return new RDDIpcProxy(javaRDDReference);
        }

        public string GetQueryExecution()
        {
            var queryExecutionReference = GetQueryExecutionReference();
            return SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(queryExecutionReference, "toString").ToString();
        }

        private JvmObjectReference GetQueryExecutionReference()
        {
            return
                new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "queryExecution").ToString());
        }

        public string GetExecutedPlan()
        {
            var queryExecutionReference = GetQueryExecutionReference();
            var executedPlanReference =
                new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(queryExecutionReference, "executedPlan")
                        .ToString());
            return SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(executedPlanReference, "toString", new object[] { }).ToString();
        }

        public string GetShowString(int numberOfRows, bool truncate)
        {
            return
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                    jvmDataFrameReference, "showString",
                    new object[] { numberOfRows, truncate }).ToString(); 
        }

        public bool IsLocal()
        {
            return
                bool.Parse(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                    jvmDataFrameReference, "isLocal").ToString());
        }

        public IStructTypeProxy GetSchema()
        {
            return
                new StructTypeIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "schema").ToString()));
        }

        public IRDDProxy ToJSON()
        {
            return new RDDIpcProxy(
                new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameReference, "toJSON")), 
                        "toJavaRDD")));
        }

        public IRDDProxy ToRDD()
        {
            return new RDDIpcProxy(
                new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "dfToRowRDD", new object[] {jvmDataFrameReference})),
                        "toJavaRDD")));
        }

        public IColumnProxy GetColumn(string columnName)
        {
            return
                new ColumnIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "col", new object[] { columnName }).ToString()));
        }

        public IDataFrameProxy Select(string columnName, string[] columnNames)
        {
            var parameters = columnNames.Length > 0 ?
                new object[] 
                { 
                    columnName,
                    new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", new object[] { columnNames })) 
                } :
                new object[] { columnName, new string[0] }; // when columnNames is empty, pass an empty array to JVM instead calling SQLUtils.toSeq

            return new DataFrameIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameReference, 
                "select",
                parameters)),
                sqlContextProxy);
        }

        public IDataFrameProxy Select(IEnumerable<IColumnProxy> columns)
        {
            var columnsSeq = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils",
                        "toSeq", new object[] { columns.Select(c => (c as ColumnIpcProxy).ScalaColumnReference).ToArray() }));

            return new DataFrameIpcProxy(new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "select", columnsSeq).ToString()), sqlContextProxy);
        
        }

        public IDataFrameProxy SelectExpr(string[] columnExpressions)
        {
            return new DataFrameIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameReference,
                "selectExpr",
                new object[] 
                { 
                    new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", new object[] { columnExpressions })) 
                })),
                sqlContextProxy);
        }

        public IDataFrameProxy Filter(string condition)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "filter", new object[] { condition }).ToString()), sqlContextProxy);
        }


        public IGroupedDataProxy GroupBy(string firstColumnName, string[] otherColumnNames)
        {
            return
                new GroupedDataIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, 
                        "groupBy",
                        new object[] 
                        { 
                            firstColumnName, 
                            new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", new object[] { otherColumnNames })) 
                        })), sqlContextProxy);
        }

        public IGroupedDataProxy GroupBy()
        {
            return
                new GroupedDataIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference,
                        "groupBy",
                        new object[] 
                        { 
                            new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils","toSeq", 
                                new object[] {new JvmObjectReference[0]}))
                        })), sqlContextProxy);
        }

        public IGroupedDataProxy Rollup(string firstColumnName, string[] otherColumnNames)
        {
            return
                new GroupedDataIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference,
                        "rollup",
                        new object[] 
                        { 
                            firstColumnName, 
                            new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", new object[] { otherColumnNames })) 
                        })), sqlContextProxy);
        }

        public IGroupedDataProxy Cube(string firstColumnName, string[] otherColumnNames)
        {
            return
                new GroupedDataIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference,
                        "cube",
                        new object[] 
                        { 
                            firstColumnName, 
                            new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", new object[] { otherColumnNames })) 
                        })), sqlContextProxy);
        }

        public IDataFrameProxy Agg(IGroupedDataProxy scalaGroupedDataReference, Dictionary<string, string> columnNameAggFunctionDictionary)
        {
            var mapReference = new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallConstructor("java.util.HashMap").ToString());
            foreach (var key in columnNameAggFunctionDictionary.Keys)
            {
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(mapReference, "put", new object[] { key, columnNameAggFunctionDictionary[key] });
            }
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        (scalaGroupedDataReference as GroupedDataIpcProxy).ScalaGroupedDataReference, "agg", new object[] { mapReference }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string joinColumnName)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                        SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameReference, "join", new object[]
                        {
                            (otherScalaDataFrameReference as DataFrameIpcProxy).jvmDataFrameReference,
                            joinColumnName
                        }).ToString()
                    ), sqlContextProxy);
        }

        public IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string[] joinColumnNames)
        {
            var stringSequenceReference = new JvmObjectReference(
                     SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", new object[] { joinColumnNames }).ToString());

            return
                new DataFrameIpcProxy(new JvmObjectReference(
                        SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameReference, "join", new object[]
                        {
                            (otherScalaDataFrameReference as DataFrameIpcProxy).jvmDataFrameReference,
                            stringSequenceReference
                        }).ToString()
                    ), sqlContextProxy);
        }

        public IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, IColumnProxy scalaColumnReference, string joinType)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "join",
                        new object[]
                        {
                            (otherScalaDataFrameReference as DataFrameIpcProxy).jvmDataFrameReference,
                            (scalaColumnReference as ColumnIpcProxy).ScalaColumnReference,
                            joinType
                        }).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, intersect(other: DataFrame): DataFrame
        /// </summary>
        /// <param name="otherScalaDataFrameReference"></param>
        /// <returns></returns>
        public IDataFrameProxy Intersect(IDataFrameProxy otherScalaDataFrameReference)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "intersect",
                        new object[] { (otherScalaDataFrameReference as DataFrameIpcProxy).jvmDataFrameReference }).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, unionAll(other: DataFrame): DataFrame
        /// </summary>
        /// <param name="otherScalaDataFrameReference"></param>
        /// <returns></returns>
        public IDataFrameProxy UnionAll(IDataFrameProxy otherScalaDataFrameReference)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "unionAll",
                        new object[] { (otherScalaDataFrameReference as DataFrameIpcProxy).jvmDataFrameReference }).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, except(other: DataFrame): DataFrame
        /// </summary>
        /// <param name="otherScalaDataFrameReference"></param>
        /// <returns></returns>
        public IDataFrameProxy Subtract(IDataFrameProxy otherScalaDataFrameReference)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "except",
                        new object[] { (otherScalaDataFrameReference as DataFrameIpcProxy).jvmDataFrameReference }).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, drop(colName: String): DataFrame
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public IDataFrameProxy Drop(string columnName)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "drop",
                        new object[] { columnName }).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, na(): DataFrame
        /// </summary>
        public IDataFrameNaFunctionsProxy Na()
        {
            return new DataFrameNaFunctionsIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                    jvmDataFrameReference, "na")), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, dropDuplicates(): DataFrame
        /// </summary>
        /// <returns></returns>
        public IDataFrameProxy DropDuplicates()
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "dropDuplicates").ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, dropDuplicates(colNames: Seq[String]): DataFrame
        /// </summary>
        /// <param name="subset"></param>
        /// <returns></returns>
        public IDataFrameProxy DropDuplicates(string[] subset)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "dropDuplicates", new object[] { subset }).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrameNaFunctions.scala, replace[T](col: String, replacement: Map[T, T]): DataFrame
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="subset"></param>
        /// <param name="toReplaceAndValueDict"></param>
        /// <returns></returns>
        public IDataFrameProxy Replace<T>(object subset, Dictionary<T, T> toReplaceAndValueDict)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        na.JvmReference, "replace",
                        new object[] { subset, toReplaceAndValueDict }).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, randomSplit(weights: Array[Double], seed: Long): Array[DataFrame]
        /// </summary>
        /// <param name="weights"></param>
        /// <param name="seed"></param>
        /// <returns></returns>
        public IEnumerable<IDataFrameProxy> RandomSplit(IEnumerable<double> weights, long? seed)
        {
            return ((List<JvmObjectReference>)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "randomSplit",
                        new object[] { weights.ToArray(), seed })).Select(jRef => new DataFrameIpcProxy(jRef, sqlContextProxy));
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, sort(sortExprs: Column*): DataFrame
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public IDataFrameProxy Sort(IColumnProxy[] columns)
        {
            var columnsSeq = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils",
                        "toSeq", new object[] { columns.Select(c => (c as ColumnIpcProxy).ScalaColumnReference).ToArray() }));
            
            return new DataFrameIpcProxy(new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "sort", columnsSeq).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.6/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, sortWithinPartitions(sortExprs: Column*): DataFrame
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public IDataFrameProxy SortWithinPartitions(IColumnProxy[] columns)
        {
            var columnsSeq = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils",
                        "toSeq", new object[] { columns.Select(c => (c as ColumnIpcProxy).ScalaColumnReference).ToArray() }));

            return new DataFrameIpcProxy(new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "sortWithinPartitions", columnsSeq).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, as(alias: String): DataFrame
        /// </summary>
        /// <param name="alias"></param>
        /// <returns></returns>
        public IDataFrameProxy Alias(string alias)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "as", alias).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala, corr(col1: String, col2: String, method: String): Double
        /// </summary>
        /// <param name="column1"></param>
        /// <param name="column2"></param>
        /// <param name="method"></param>
        /// <returns></returns>
        public double Corr(string column1, string column2, string method)
        {
            return (double)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(stat.JvmReference, "corr", column1, column2, method);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala, cov(col1: String, col2: String): Double
        /// </summary>
        /// <param name="column1"></param>
        /// <param name="column2"></param>
        /// <returns></returns>
        public double Cov(string column1, string column2)
        {
            return (double)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(stat.JvmReference, "cov", column1, column2);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala, freqItems(cols: Array[String], support: Double)
        /// </summary>
        /// <param name="columns"></param>
        /// <param name="support"></param>
        /// <returns></returns>
        public IDataFrameProxy FreqItems(IEnumerable<string> columns, double support)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                stat.JvmReference, "freqItems", new object[] {columns.ToArray(), support}).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala, crosstab(col1: String, col2: String): DataFrame
        /// </summary>
        /// <param name="column1"></param>
        /// <param name="column2"></param>
        /// <returns></returns>
        public IDataFrameProxy Crosstab(string column1, string column2)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                stat.JvmReference, "crosstab", column1, column2).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Describe(string[] columns)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameReference, "describe", new object[] { columns }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Limit(int num)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "limit",
                        new object[] { num }).ToString()), sqlContextProxy);
        }    

        public IDataFrameProxy Distinct()
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "distinct").ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Coalesce(int numPartitions)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "coalesce",
                        new object[] { numPartitions }).ToString()), sqlContextProxy);
        }

        public void Persist(StorageLevelType storageLevelType)
        {
            var jstorageLevel = SparkContextIpcProxy.GetJavaStorageLevel(storageLevelType);
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameReference, "persist", new object[] { jstorageLevel });
        }

        public void Unpersist(bool blocking)
        {
            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                jvmDataFrameReference, "unpersist", new object[] { blocking });
        }

        public IDataFrameProxy Repartition(int numPartitions)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "repartition",
                        new object[] { numPartitions }).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.6/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, repartition(numPartitions: Int, partitionExprs: Column*): DataFrame
        /// </summary>
        /// <param name="numPartitions"></param>
        /// <param name="columns"></param>
        /// <returns></returns>
        public IDataFrameProxy Repartition(int numPartitions, IColumnProxy[] columns)
        {
            var columnsSeq = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils",
                        "toSeq", new object[] { columns.Select(c => (c as ColumnIpcProxy).ScalaColumnReference).ToArray() }));

            return new DataFrameIpcProxy(new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "repartition", new object[] { numPartitions, columnsSeq }).ToString()), sqlContextProxy);
        }

        /// <summary>
        /// Call https://github.com/apache/spark/blob/branch-1.6/sql/core/src/main/scala/org/apache/spark/sql/DataFrame.scala, repartition(partitionExprs: Column*): DataFrame
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public IDataFrameProxy Repartition(IColumnProxy[] columns)
        {
            var columnsSeq = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils",
                        "toSeq", new object[] { columns.Select(c => (c as ColumnIpcProxy).ScalaColumnReference).ToArray() }));

            return new DataFrameIpcProxy(new JvmObjectReference(SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "repartition", new object[] { columnsSeq }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Sample(bool withReplacement, double fraction, long seed)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "sample",
                        new object[] { withReplacement, fraction, seed }).ToString()), sqlContextProxy);
        }

        public IDataFrameWriterProxy Write()
        {
            return new DataFrameWriterIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameReference, "write").ToString()));
        }
    }

    internal class UDFIpcProxy : IUDFProxy
    {
        private readonly JvmObjectReference jvmUDFReference;

        internal UDFIpcProxy(JvmObjectReference jvmUDFReference)
        {
            this.jvmUDFReference = jvmUDFReference;
        }

        public IColumnProxy Apply(IColumnProxy[] columns)
        {
            var seq = new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils",
                        "toSeq", new object[] { columns.Select(c => (c as ColumnIpcProxy).ScalaColumnReference).ToArray() }));
            return new ColumnIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmUDFReference, "apply", seq)));
        }
    }

    internal class ColumnIpcProxy : IColumnProxy
    {
        private readonly JvmObjectReference scalaColumnReference;

        internal JvmObjectReference ScalaColumnReference { get { return scalaColumnReference; } }

        internal ColumnIpcProxy(JvmObjectReference colReference)
        {
            scalaColumnReference = colReference;
        }

        public IColumnProxy EqualsOperator(IColumnProxy secondColumn)
        {
            return
                new ColumnIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        scalaColumnReference, "equalTo",
                        new object[] { (secondColumn as ColumnIpcProxy).scalaColumnReference }).ToString()));
        }

        public IColumnProxy UnaryOp(string name)
        {
            return new ColumnIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(scalaColumnReference, name)));
        }

        public IColumnProxy FuncOp(string name)
        {
            return new ColumnIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.functions", name, scalaColumnReference)));
        }

        public IColumnProxy BinOp(string name, object other)
        {
            if (other is ColumnIpcProxy)
                other = (other as ColumnIpcProxy).scalaColumnReference;
            return new ColumnIpcProxy(new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(scalaColumnReference, name, other)));
        }

        public IColumnProxy InvokeMethod(string methodName, params object[] parameters)
        {
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(scalaColumnReference, methodName, parameters.ToArray())));
        }
    }

    internal class GroupedDataIpcProxy : IGroupedDataProxy
    {
        private readonly JvmObjectReference scalaGroupedDataReference;
        internal JvmObjectReference ScalaGroupedDataReference { get { return scalaGroupedDataReference; } }
        private readonly ISqlContextProxy scalaSqlContextReference;
        internal GroupedDataIpcProxy(JvmObjectReference gdRef, ISqlContextProxy sccProxy)
        {
            scalaGroupedDataReference = gdRef;
            scalaSqlContextReference = sccProxy;
        }
        public IDataFrameProxy Count()
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(scalaGroupedDataReference, "count").ToString()), scalaSqlContextReference);
        }

        public IDataFrameProxy Mean(params string[] columns)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(scalaGroupedDataReference, "mean", new object[] { columns }).ToString()), scalaSqlContextReference);
        }

        public IDataFrameProxy Max(params string[] columns)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(scalaGroupedDataReference, "max", new object[] { columns }).ToString()), scalaSqlContextReference);
        }

        public IDataFrameProxy Min(params string[] columns)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(scalaGroupedDataReference, "min", new object[] { columns }).ToString()), scalaSqlContextReference);
        }

        public IDataFrameProxy Avg(params string[] columns)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(scalaGroupedDataReference, "avg", new object[] { columns }).ToString()), scalaSqlContextReference);
        }

        public IDataFrameProxy Sum(params string[] columns)
        {
            return new DataFrameIpcProxy(new JvmObjectReference(
                SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(scalaGroupedDataReference, "sum", new object[] { columns }).ToString()), scalaSqlContextReference);
        }
    }

    internal class DataFrameNaFunctions
    {
        private readonly JvmObjectReference dataFrameProxy;
        public DataFrameNaFunctions(JvmObjectReference dataFrameProxy)
        {
            this.dataFrameProxy = dataFrameProxy;
        }
        public JvmObjectReference JvmReference
        {
            get
            {
                return new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                    dataFrameProxy, "na"));
            }
        }
    }

    internal class DataFrameStatFunctions
    {
        private readonly JvmObjectReference dataFrameProxy;
        public DataFrameStatFunctions(JvmObjectReference dataFrameProxy)
        {
            this.dataFrameProxy = dataFrameProxy;
        }
        public JvmObjectReference JvmReference
        {
            get
            {
                return new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                    dataFrameProxy, "stat"));
            }
        }
    }
}
