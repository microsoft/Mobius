// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    ///  A distributed collection of data organized into named columns.
    /// 
    /// See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
    /// </summary>
    public class DataFrame
    {
        private IDataFrameProxy dataFrameProxy;
        private readonly SparkContext sparkContext;
        private StructType schema;
        
        internal SparkContext SparkContext
        {
            get
            {
                return sparkContext;
            }
        }

        internal IDataFrameProxy DataFrameProxy
        {
            get { return dataFrameProxy;  }
        }

        public StructType Schema
        {
            get { return schema ?? (schema = new StructType(dataFrameProxy.GetSchema())); }
        }

        public Column this[string columnName]
        {
            get
            {
                return new Column(dataFrameProxy.GetColumn(columnName));
            }
        }

        internal DataFrame(IDataFrameProxy dataFrameProxy, SparkContext sparkContext)
        {
            this.dataFrameProxy = dataFrameProxy;
            this.sparkContext = sparkContext;
        }

        /// <summary>
        /// Registers this DataFrame as a temporary table using the given name.  The lifetime of this 
        /// temporary table is tied to the SqlContext that was used to create this DataFrame.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        public void RegisterTempTable(string tableName)
        {
            dataFrameProxy.RegisterTempTable(tableName);
        }

        /// <summary>
        /// Number of rows in the DataFrame
        /// </summary>
        /// <returns>row count</returns>
        public long Count()
        {
            return dataFrameProxy.Count();
        }

        /// <summary>
        /// Displays rows of the DataFrame in tabular form
        /// </summary>
        /// <param name="numberOfRows">Number of rows to display - default 20</param>
        /// <param name="truncate">Indicates if strings more than 20 characters long will be truncated</param>
        public void Show(int numberOfRows = 20, bool truncate = true)
        {
            Console.WriteLine(dataFrameProxy.GetShowString(numberOfRows, truncate));
        }

        /// <summary>
        /// Prints the schema information of the DataFrame
        /// </summary>
        public void ShowSchema()
        {
            List<string> nameTypeList = Schema.Fields.Select(structField => string.Format("{0}:{1}", structField.Name, structField.DataType.SimpleString())).ToList();
            Console.WriteLine(string.Join(", ", nameTypeList));
        }

        public IEnumerable<Row> Collect()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Converts the DataFrame to RDD of byte[]
        /// </summary>
        /// <returns>resulting RDD</returns>
        public RDD<byte[]> ToRDD() //RDD created using byte representation of GenericRow objects
        {
            return new RDD<byte[]>(dataFrameProxy.ToRDD(), sparkContext);
        }

        /// <summary>
        /// Returns the content of the DataFrame as RDD of JSON strings
        /// </summary>
        /// <returns>resulting RDD</returns>
        public RDD<string> ToJSON()
        {
            var stringRddReference = dataFrameProxy.ToJSON();
            return new RDD<string>(stringRddReference, sparkContext);
        }

        /// <summary>
        /// Prints the plans (logical and physical) to the console for debugging purposes
        /// </summary>
        /// <param name="extended">if true prints both query plan and execution plan; otherwise just prints query plan</param>
        public void Explain(bool extended = false) //TODO - GetQueryExecution is called in JVM twice if extendd = true - fix that
        {
            Console.WriteLine(dataFrameProxy.GetQueryExecution());
            if (extended)
            {
                Console.WriteLine(dataFrameProxy.GetExecutedPlan());
            }
        }

        /// <summary>
        /// Select a list of columns
        /// </summary>
        /// <param name="columnNames">name of the columns</param>
        /// <returns>DataFrame with selected columns</returns>
        public DataFrame Select(params string[] columnNames)
        {
            List<IColumnProxy> columnReferenceList = columnNames.Select(columnName => dataFrameProxy.GetColumn(columnName)).ToList();
            IColumnProxy columnReferenceSeq = dataFrameProxy.ToColumnSeq(columnReferenceList);
            return new DataFrame(dataFrameProxy.Select(columnReferenceSeq), sparkContext);
        }

        /// <summary>
        /// Select a list of columns
        /// </summary>
        /// <param name="columnNames"></param>
        /// <returns></returns>
        public DataFrame Select(params Column[] columns)
        {
            List<IColumnProxy> columnReferenceList = columns.Select(column => column.ColumnProxy).ToList();
            IColumnProxy columnReferenceSeq = dataFrameProxy.ToColumnSeq(columnReferenceList);
            return new DataFrame(dataFrameProxy.Select(columnReferenceSeq), sparkContext);
        }

        /// <summary>
        /// Filters rows using the given condition
        /// </summary>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DataFrame Where(string condition)
        {
            return Filter(condition);
        }

        /// <summary>
        /// Filters rows using the given condition
        /// </summary>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DataFrame Filter(string condition)
        {
            return new DataFrame(dataFrameProxy.Filter(condition), sparkContext);
        }

        /// <summary>
        /// Groups the DataFrame using the specified columns, so we can run aggregation on them.
        /// </summary>
        /// <param name="columnNames"></param>
        /// <returns></returns>
        public GroupedData GroupBy(params string[] columnNames)
        {
            if (columnNames.Length == 0)
            {
                throw new NotSupportedException("Invalid number of columns");
            }

            string firstColumnName = columnNames[0];
            string[] otherColumnNames = columnNames.Skip(1).ToArray();

            List<IColumnProxy> otherColumnReferenceList = otherColumnNames.Select(columnName => dataFrameProxy.GetColumn(columnName)).ToList();
            IColumnProxy otherColumnReferenceSeq = dataFrameProxy.ToColumnSeq(otherColumnReferenceList);
            var scalaGroupedDataReference = dataFrameProxy.GroupBy(firstColumnName, otherColumnReferenceSeq);
            return new GroupedData(scalaGroupedDataReference, this);
        }

        private GroupedData GroupBy()
        {
            object otherColumnReferenceSeq = dataFrameProxy.ToObjectSeq(new List<object>());
            var scalaGroupedDataReference = dataFrameProxy.GroupBy(otherColumnReferenceSeq);
            return new GroupedData(scalaGroupedDataReference, this);
        }

        /// <summary>
        /// Aggregates on the DataFrame for the given column-aggregate function mapping
        /// </summary>
        /// <param name="columnNameAggFunctionDictionary"></param>
        /// <returns></returns>
        public DataFrame Agg(Dictionary<string, string> columnNameAggFunctionDictionary)
        {
            return GroupBy().Agg(columnNameAggFunctionDictionary);
        }

        /// <summary>
        /// Join with another DataFrame
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame) //cartesian join
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Join with another DataFrame
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame, string joinColumnName) //inner equi join using given column name //need aliasing for self join
        {
            return new DataFrame(
                dataFrameProxy.Join(otherDataFrame.dataFrameProxy, joinColumnName),
                sparkContext);
        }

        /// <summary>
        /// Join with another DataFrame
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame, string[] joinColumnNames) //inner equi join using given column name //need aliasing for self join
        {
            return new DataFrame(
                dataFrameProxy.Join(otherDataFrame.dataFrameProxy, joinColumnNames),
                sparkContext);
        }

        /// <summary>
        /// Join with another DataFrame
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame, Column joinExpression, JoinType joinType = null) 
        {
            if (joinType == null)
            {
                joinType = JoinType.Inner;
            }

            return
                new DataFrame(dataFrameProxy.Join(otherDataFrame.dataFrameProxy, joinExpression.ColumnProxy, joinType.Value), sparkContext);
        }

        /// <summary>
        /// Intersect with another DataFrame
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to intersect with</param>
        /// <returns>Intersected DataFrame</returns>
        public DataFrame Intersect(DataFrame otherDataFrame)
        {
            return
                new DataFrame(dataFrameProxy.Intersect(otherDataFrame.dataFrameProxy), sparkContext);
        }
    }

    //TODO - complete impl
    public class Row
    {
        
    }

    public class JoinType
    {
        public string Value { get; private set; }
        private JoinType(string value)
        {
            Value = value;
        }

        private static readonly JoinType InnerJoinType = new JoinType("inner");
        private static readonly JoinType OuterJoinType = new JoinType("outer");
        private static readonly JoinType LeftOuterJoinType = new JoinType("left_outer");
        private static readonly JoinType RightOuterJoinType = new JoinType("right_outer");
        private static readonly JoinType LeftSemiJoinType = new JoinType("leftsemi");

        public static JoinType Inner
        {
            get
            {
                return InnerJoinType;
            }
        }

        public static JoinType Outer
        {
            get
            {
                return OuterJoinType;
            }
        }

        public static JoinType LeftOuter
        {
            get
            {
                return LeftOuterJoinType;
            }
        }

        public static JoinType RightOuter
        {
            get
            {
                return RightOuterJoinType;
            }
        }

        public static JoinType LeftSemi
        {
            get
            {
                return LeftSemiJoinType;
            }
        }
    }

    public class Column
    {
        private IColumnProxy columnProxy;

        internal IColumnProxy ColumnProxy
        {
            get
            {
                return columnProxy;
            }
        }

        internal Column(IColumnProxy columnProxy)
        {
            this.columnProxy = columnProxy;
        }

        public static Column operator ==(Column firstColumn, Column secondColumn)
        {
            return new Column(firstColumn.columnProxy.EqualsOperator(secondColumn.columnProxy));
        }

        public static Column operator !=(Column firstColumn, Column secondColumn)
        {
            throw new NotImplementedException();
        }
    }

    public class GroupedData
    {
        private IGroupedDataProxy groupedDataProxy;
        private DataFrame dataFrame;

        internal GroupedData(IGroupedDataProxy groupedDataProxy, DataFrame dataFrame)
        {
            this.groupedDataProxy = groupedDataProxy;
            this.dataFrame = dataFrame;
        }

        public DataFrame Agg(Dictionary<string, string> columnNameAggFunctionDictionary)
        {
            return new DataFrame(dataFrame.DataFrameProxy.Agg(groupedDataProxy, columnNameAggFunctionDictionary), dataFrame.SparkContext);
        }
    }
}
