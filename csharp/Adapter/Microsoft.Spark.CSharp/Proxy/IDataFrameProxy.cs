// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;
using Microsoft.Spark.CSharp.Sql;

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
        IStructTypeProxy GetSchema();
        IRDDProxy ToJSON();
        IRDDProxy ToRDD();
        IColumnProxy GetColumn(string columnName);
        IDataFrameProxy Select(string columnName, string[] columnNames);
        IDataFrameProxy SelectExpr(string[] columnExpressions);
        IDataFrameProxy Filter(string condition);
        IGroupedDataProxy GroupBy(string firstColumnName, string[] otherColumnNames);
        IGroupedDataProxy GroupBy();
        IDataFrameProxy Agg(IGroupedDataProxy scalaGroupedDataReference, Dictionary<string, string> columnNameAggFunctionDictionary);
        IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string joinColumnName);
        IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string[] joinColumnNames);
        IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, IColumnProxy scalaColumnReference, string joinType);
        IDataFrameProxy Intersect(IDataFrameProxy otherScalaDataFrameReference);
        IDataFrameProxy UnionAll(IDataFrameProxy otherScalaDataFrameReference);
        IDataFrameProxy Subtract(IDataFrameProxy otherScalaDataFrameReference);
        IDataFrameProxy Drop(string columnName);
        IDataFrameProxy DropNa(int? thresh, string[] subset);
        IDataFrameProxy DropDuplicates();
        IDataFrameProxy DropDuplicates(string[] subset);
        IDataFrameProxy Replace<T>(object subset, Dictionary<T, T> toReplaceAndValueDict);
        IEnumerable<IDataFrameProxy> RandomSplit(IEnumerable<double> weights, long? seed);
        IDataFrameProxy Sort(IColumnProxy[] columns);
        IDataFrameProxy Alias(string alias);
        IDataFrameProxy Limit(int num);
        IDataFrameProxy Distinct();
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
    }

    internal interface IGroupedDataProxy
    {
    }
}
