// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    internal class DataFrameIpcProxy : IDataFrameProxy
    {
        private readonly JvmObjectReference jvmDataFrameReference;
        private readonly ISqlContextProxy sqlContextProxy;

        private readonly DataFrameNaFunctions na;

        internal DataFrameIpcProxy(JvmObjectReference jvmDataFrameReference, ISqlContextProxy sqlProxy)
        {
            this.jvmDataFrameReference = jvmDataFrameReference;
            sqlContextProxy = sqlProxy;
            na = new DataFrameNaFunctions(jvmDataFrameReference);
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
                    new object[] { numberOfRows /*,  truncate*/ }).ToString(); //1.4.1 does not support second param
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
                        })));
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
                        })));
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
            throw new NotSupportedException("Not supported in 1.4.1");

            //TODO - uncomment this in 1.5
            //var stringSequenceReference = new JvmObjectReference(
            //         SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", new object[] { joinColumnNames }).ToString());

            //return
            //    new JvmObjectReference(
            //            SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(scalaDataFrameReference, "join", new object[]
            //            {
            //                otherScalaDataFrameReference,
            //                stringSequenceReference
            //            }).ToString()
            //        );
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
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrameNaFunctions.scala, drop(how: String, cols: Seq[String])
        /// </summary>
        /// <param name="thresh"></param>
        /// <param name="subset"></param>
        /// <returns></returns>
        public IDataFrameProxy DropNa(int? thresh, string[] subset)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        na.DataFrameNaReference, "drop",
                        new object[] { thresh, subset }).ToString()), sqlContextProxy);
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
        /// Call https://github.com/apache/spark/blob/branch-1.4/sql/core/src/main/scala/org/apache/spark/sql/DataFrameNaFunctions.scala, def replace[T](col: String, replacement: Map[T, T]): DataFrame
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
                        na.DataFrameNaReference, "replace",
                        new object[] { subset, toReplaceAndValueDict }).ToString()), sqlContextProxy);
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

        public IDataFrameProxy Sample(bool withReplacement, double fraction, long seed)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "sample",
                        new object[] { withReplacement, fraction, seed }).ToString()), sqlContextProxy);
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
    }

    internal class GroupedDataIpcProxy : IGroupedDataProxy
    {
        private readonly JvmObjectReference scalaGroupedDataReference;
        internal JvmObjectReference ScalaGroupedDataReference { get { return scalaGroupedDataReference; } }

        internal GroupedDataIpcProxy(JvmObjectReference gdRef)
        {
            scalaGroupedDataReference = gdRef;
        }
    }

    internal class DataFrameNaFunctions
    {
        private readonly JvmObjectReference dataFrameProxy;
        public DataFrameNaFunctions(JvmObjectReference dataFrameProxy)
        {
            this.dataFrameProxy = dataFrameProxy;
        }
        public JvmObjectReference DataFrameNaReference
        {
            get
            {
                return new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                    this.dataFrameProxy, "na"));
            }
        }
    }
}
