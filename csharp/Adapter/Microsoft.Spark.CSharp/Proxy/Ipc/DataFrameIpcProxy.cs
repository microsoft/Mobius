// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Proxy.Ipc
{
    internal class DataFrameIpcProxy : IDataFrameProxy
    {
        private readonly JvmObjectReference jvmDataFrameReference;
        private readonly ISqlContextProxy sqlContextProxy;

        internal DataFrameIpcProxy(JvmObjectReference jvmDataFrameReference, ISqlContextProxy sqlProxy)
        {
            this.jvmDataFrameReference = jvmDataFrameReference;
            sqlContextProxy = sqlProxy;
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

        /// <summary>
        /// Call CollectAndServe() in Java side, it will collect an RDD as an iterator, then serve it via socket
        /// </summary>
        /// <returns>the port number of a local socket which serves the data collected</returns>
        public int CollectAndServe()
        {
            var javaRDDReference = new JvmObjectReference(
                (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameReference, "javaToPython"));
            var rddReference = new JvmObjectReference(
                (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(javaRDDReference, "rdd"));
            return int.Parse(SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod(
                "org.apache.spark.api.python.PythonRDD",
                "collectAndServe",
                new object[] { rddReference }).ToString());
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

        public IDataFrameProxy Intersect(IDataFrameProxy otherScalaDataFrameReference)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "intersect",
                        new object[] { (otherScalaDataFrameReference as DataFrameIpcProxy).jvmDataFrameReference }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy UnionAll(IDataFrameProxy otherScalaDataFrameReference)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "unionAll",
                        new object[] { (otherScalaDataFrameReference as DataFrameIpcProxy).jvmDataFrameReference }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Subtract(IDataFrameProxy otherScalaDataFrameReference)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "except",
                        new object[] { (otherScalaDataFrameReference as DataFrameIpcProxy).jvmDataFrameReference }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Drop(string columnName)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "drop",
                        new object[] { columnName }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy DropNa(string how, int? thresh, string[] subset)
        {
            if (how != "any" && how != "all")
                throw new ArgumentException(string.Format(@"how ({0}) should be 'any' or 'all'.", how));

            string[] columnNames = null;
            if (subset == null || subset.Length == 0)
                columnNames = GetSchema().GetStructTypeFields().Select(f => f.GetStructFieldName().ToString()).ToArray();

            if (thresh == null)
                thresh = how == "any" ? (subset == null ? columnNames.Length : subset.Length) : 1;

            var dataFrameNaRef = new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "na"));

            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        dataFrameNaRef, "drop",
                        new object[] { thresh, subset ?? columnNames }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy DropDuplicates(string[] subset)
        {
            return (subset == null || subset.Length == 0) ?
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "dropDuplicates").ToString()), sqlContextProxy) :
            new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "dropDuplicates",
                        new object[] { subset }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Replace<T>(T toReplace, T value, string[] subset)
        {
            return ReplaceCore(new Dictionary<T, T> { { toReplace, value } }, subset);
        }

        public IDataFrameProxy ReplaceAll<T>(IEnumerable<T> toReplace, IEnumerable<T> value, string[] subset)
        {
            var toReplaceArray = toReplace.ToArray();
            var valueArray = value.ToArray();
            if (toReplaceArray.Length != valueArray.Length)
                throw new ArgumentException("toReplace and value lists should be of the same length");

            var toReplaceAndValueDict = toReplaceArray.Zip(valueArray, (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v);

            return ReplaceCore(toReplaceAndValueDict, subset);
        }

        public IDataFrameProxy ReplaceAll<T>(IEnumerable<T> toReplace, T value, string[] subset)
        {
            var toReplaceArray = toReplace.ToArray();
            var toReplaceAndValueDict = toReplaceArray.Zip(Enumerable.Repeat(value, toReplaceArray.Length).ToList(), (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v);

            return ReplaceCore(toReplaceAndValueDict, subset);
        }

        private IDataFrameProxy ReplaceCore<T>(Dictionary<T, T> toReplaceAndValueDict, string[] subset)
        {
            var validTypes = new[] { typeof(int), typeof(short), typeof(long), typeof(double), typeof(float), typeof(string) };
            if (!validTypes.Any(t => t == typeof(T)))
                throw new ArgumentException("toReplace and value should be a float, double, short, int, long or string");

            var dataFrameNaRef = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "na"));

            object subsetRepresentation;
            if (subset == null || subset.Length == 0)
            {
                subsetRepresentation = "*";
            }
            else
            {
                subsetRepresentation = subset;
            }
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(
                        dataFrameNaRef, "replace",
                        new object[] { subsetRepresentation, toReplaceAndValueDict }).ToString()), sqlContextProxy);
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

}
