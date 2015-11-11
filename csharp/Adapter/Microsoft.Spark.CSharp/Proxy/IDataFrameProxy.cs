// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Collections.Generic;

namespace Microsoft.Spark.CSharp.Proxy
{
    internal interface IDataFrameProxy
    {
        void RegisterTempTable(string tableName);
        long Count();
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
        IDataFrameProxy DropNa(string how, int? thresh, string[] subset);
        IDataFrameProxy Replace<T>(T toReplace, T value, string[] subset);
        IDataFrameProxy ReplaceAll<T>(IEnumerable<T> toReplace, IEnumerable<T> value, string[] subset);
        IDataFrameProxy ReplaceAll<T>(IEnumerable<T> toReplace, T value, string[] subset);
        IDataFrameProxy DropDuplicates(string[] subset);
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
