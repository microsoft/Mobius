// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Sql;

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
        object ToObjectSeq(List<object> objectList);
        IColumnProxy ToColumnSeq(List<IColumnProxy> columnRefList);
        IDataFrameProxy Select(IColumnProxy columnSequenceReference);
        IDataFrameProxy Filter(string condition);
        IGroupedDataProxy GroupBy(string firstColumnName, IColumnProxy otherColumnSequenceReference);
        IGroupedDataProxy GroupBy(IColumnProxy columnSequenceReference);
        IGroupedDataProxy GroupBy(object columnSequenceReference);
        IDataFrameProxy Agg(IGroupedDataProxy scalaGroupedDataReference, Dictionary<string, string> columnNameAggFunctionDictionary);
        IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string joinColumnName);
        IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, string[] joinColumnNames);
        IDataFrameProxy Join(IDataFrameProxy otherScalaDataFrameReference, IColumnProxy scalaColumnReference, string joinType);
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
