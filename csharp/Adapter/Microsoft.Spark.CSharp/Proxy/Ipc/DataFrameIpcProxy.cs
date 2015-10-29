// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop;
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
            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameReference,
                "registerTempTable", new object[] {tableName});
        }

        public long Count()
        {
            return
                long.Parse(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "count").ToString());
        }

        public string GetQueryExecution()
        {
            var queryExecutionReference = GetQueryExecutionReference();
            return SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(queryExecutionReference, "toString").ToString();
        }

        private JvmObjectReference GetQueryExecutionReference()
        {
            return
                new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "queryExecution").ToString());
        }

        public string GetExecutedPlan()
        {
            var queryExecutionReference = GetQueryExecutionReference();
            var executedPlanReference =
                new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(queryExecutionReference, "executedPlan")
                        .ToString());
            return SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(executedPlanReference, "toString", new object[] { }).ToString();
        }

        public string GetShowString(int numberOfRows, bool truncate)
        {
            return
                SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                    jvmDataFrameReference, "showString",
                    new object[] {numberOfRows /*,  truncate*/ }).ToString(); //1.4.1 does not support second param
        }

        public IStructTypeProxy GetSchema()
        {
            return
                new StructTypeIpcProxy(new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "schema").ToString()));
        }

        public IRDDProxy ToJSON()
        {
            return new RDDIpcProxy(
                new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameReference, "toJSON")), 
                    "toJavaRDD")));
        }

        public IRDDProxy ToRDD()
        {
            return new RDDIpcProxy(
                new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "dfToRowRDD", new object[] {jvmDataFrameReference})),
                    "toJavaRDD")));
        }

        public IColumnProxy GetColumn(string columnName)
        {
            return
                new ColumnIpcProxy(new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "col", new object[] {columnName}).ToString()));
        }

        public object ToObjectSeq(List<object> objectList)
        {
            var javaObjectReferenceList = objectList.Cast<JvmObjectReference>().ToList();
            return
                new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils",
                        "toSeq", new object[] {javaObjectReferenceList}).ToString());
        }

        public IColumnProxy ToColumnSeq(List<IColumnProxy> columnRefList)
        {
            var javaObjectReferenceList = columnRefList.Select(s => (s as ColumnIpcProxy).ScalaColumnReference).ToList().Cast<JvmObjectReference>();
            return
                new ColumnIpcProxy(new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils",
                        "toSeq", new object[] { javaObjectReferenceList }).ToString()));
        }

        public IDataFrameProxy Select(IColumnProxy columnSequenceReference)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "select",
                        new object[] { (columnSequenceReference as ColumnIpcProxy).ScalaColumnReference }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Filter(string condition)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "filter", new object[] { condition }).ToString()), sqlContextProxy);
        }


        public IGroupedDataProxy GroupBy(string firstColumnName, IColumnProxy otherColumnSequenceReference)
        {
            return
                new GroupedDataIpcProxy(new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "groupBy",
                        new object[] { firstColumnName, (otherColumnSequenceReference as ColumnIpcProxy).ScalaColumnReference }).ToString()));
        }

        public IGroupedDataProxy GroupBy(IColumnProxy columnSequenceReference)
        {
            return
                new GroupedDataIpcProxy(new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "groupBy",
                        new object[] { (columnSequenceReference as ColumnIpcProxy).ScalaColumnReference}).ToString()));
        }

        public IGroupedDataProxy GroupBy(object columnSequenceReference)
        {
            return
                new GroupedDataIpcProxy(new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "groupBy",
                        new object[] { columnSequenceReference as JvmObjectReference }).ToString()));
        }

        public IDataFrameProxy Agg(IGroupedDataProxy scalaGroupedDataReference, Dictionary<string, string> columnNameAggFunctionDictionary)
        {
            var mapReference = new JvmObjectReference(SparkCLREnvironment.JvmBridge.CallConstructor("java.util.HashMap").ToString());
            foreach (var key in columnNameAggFunctionDictionary.Keys)
            {
                SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(mapReference, "put", new object[] { key, columnNameAggFunctionDictionary[key]});
            }
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        (scalaGroupedDataReference as GroupedDataIpcProxy).ScalaGroupedDataReference, "agg", new object[] { mapReference }).ToString()), sqlContextProxy);
        }

        public IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string joinColumnName)
        {
            return
                new DataFrameIpcProxy(new JvmObjectReference(
                        SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(jvmDataFrameReference, "join", new object[]
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
            //         SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.api.csharp.SQLUtils", "toSeq", new object[] { joinColumnNames }).ToString());

            //return
            //    new JvmObjectReference(
            //            SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(scalaDataFrameReference, "join", new object[]
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
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        jvmDataFrameReference, "join",
                        new object[]
                        {
                            (otherScalaDataFrameReference as DataFrameIpcProxy).jvmDataFrameReference,
                            (scalaColumnReference as ColumnIpcProxy).ScalaColumnReference,
                            joinType
                        }).ToString()), sqlContextProxy);
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
                    SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(
                        scalaColumnReference, "equalTo",
                        new object[] { (secondColumn as ColumnIpcProxy).scalaColumnReference }).ToString()));
        }

        public IColumnProxy UnaryOp(string name)
        {
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(scalaColumnReference, name)));
        }

        public IColumnProxy FuncOp(string name)
        {
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallStaticJavaMethod("org.apache.spark.sql.functions", name, scalaColumnReference)));
        }

        public IColumnProxy BinOp(string name, object other)
        {
            if (other is ColumnIpcProxy)
                other = (other as ColumnIpcProxy).scalaColumnReference;
            return new ColumnIpcProxy(new JvmObjectReference((string)SparkCLREnvironment.JvmBridge.CallNonStaticJavaMethod(scalaColumnReference, name, other)));
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
