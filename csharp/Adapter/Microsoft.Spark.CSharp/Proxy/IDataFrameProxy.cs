// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.Proxy
{
    internal interface IDataFrameProxy
    {
        void RegisterTempTable(string tableName);
        long Count();
        IRDDProxy JavaToCSharp();
        string GetQueryExecution();
        string GetExecutedPlan();
        string GetShowString(int numberOfRows, bool truncate);
        bool IsLocal();
        IStructTypeProxy GetSchema();
        IRDDProxy ToJSON();
        IRDDProxy ToRDD();
        IColumnProxy GetColumn(string columnName);
        IDataFrameProxy Select(string columnName, string[] columnNames);
        IDataFrameProxy Select(IEnumerable<IColumnProxy> columns);
        IDataFrameProxy SelectExpr(string[] columnExpressions);
        IDataFrameProxy Filter(string condition);
        IGroupedDataProxy GroupBy(string firstColumnName, string[] otherColumnNames);
        IGroupedDataProxy GroupBy();
        IGroupedDataProxy Rollup(string firstColumnName, string[] otherColumnNames);
        IGroupedDataProxy Cube(string firstColumnName, string[] otherColumnNames);

        IDataFrameProxy Agg(IGroupedDataProxy scalaGroupedDataReference, Dictionary<string, string> columnNameAggFunctionDictionary);
        IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string joinColumnName);
        IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string[] joinColumnNames);
        IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, IColumnProxy scalaColumnReference, string joinType);
        IDataFrameProxy Intersect(IDataFrameProxy otherScalaDataFrameReference);
        IDataFrameProxy UnionAll(IDataFrameProxy otherScalaDataFrameReference);
        IDataFrameProxy Subtract(IDataFrameProxy otherScalaDataFrameReference);
        IDataFrameProxy Drop(string columnName);
        IDataFrameNaFunctionsProxy Na();
        IDataFrameProxy DropDuplicates();
        IDataFrameProxy DropDuplicates(string[] subset);
        IDataFrameProxy Replace<T>(object subset, Dictionary<T, T> toReplaceAndValueDict);
        IEnumerable<IDataFrameProxy> RandomSplit(IEnumerable<double> weights, long? seed);
        IDataFrameProxy Sort(IColumnProxy[] columns);
        IDataFrameProxy SortWithinPartitions(IColumnProxy[] columns);
        IDataFrameProxy Alias(string alias);
        double Corr(string column1, string column2, string method);
        double Cov(string column1, string column2);
        IDataFrameProxy FreqItems(IEnumerable<string> columns, double support);
        IDataFrameProxy Crosstab(string column1, string column2);
        IDataFrameProxy Describe(string[] columns);

        IDataFrameProxy Limit(int num);
        IDataFrameProxy Distinct();
        IDataFrameProxy Coalesce(int numPartitions);
        void Persist(StorageLevelType storageLevelType);
        void Unpersist(bool blocking = true);
        IDataFrameProxy Repartition(int numPartitions);
        IDataFrameProxy Repartition(int numPartitions, IColumnProxy[] columns);
        IDataFrameProxy Repartition(IColumnProxy[] columns);
        IDataFrameProxy Sample(bool withReplacement, double fraction, long seed);
        IDataFrameWriterProxy Write();
    }

    internal interface IUDFProxy
    {
        IColumnProxy Apply(IColumnProxy[] columns);
    }

    internal interface IColumnProxy
    {
        IColumnProxy EqualsOperator(IColumnProxy secondColumn);
        IColumnProxy UnaryOp(string name);
        IColumnProxy FuncOp(string name);
        IColumnProxy BinOp(string name, object other);
        IColumnProxy InvokeMethod(string methodName, params object[] parameters);
    }

    internal interface IGroupedDataProxy
    {
        IDataFrameProxy Count();
        IDataFrameProxy Mean(params string[] columns);
        IDataFrameProxy Max(params string[] columns);
        IDataFrameProxy Min(params string[] columns);
        IDataFrameProxy Avg(params string[] columns);
        IDataFrameProxy Sum(params string[] columns);
    }
}
