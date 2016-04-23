// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    ///  A distributed collection of data organized into named columns.
    /// 
    /// See also http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
    /// </summary>
    [Serializable]
    public class DataFrame
    {
        [NonSerialized]
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(DataFrame));

        [NonSerialized]
        private readonly IDataFrameProxy dataFrameProxy;
        [NonSerialized]
        private readonly SparkContext sparkContext;

        private StructType schema;
        [NonSerialized]
        private RDD<Row> rdd;
        [NonSerialized]
        private IRDDProxy rddProxy;
        [NonSerialized]
        private bool? isLocal;
        [NonSerialized]
        private readonly Random random = new Random();

        /// <summary>
        /// Represents the content of the DataFrame as an RDD of Rows.
        /// </summary>
        public RDD<Row> Rdd
        {
            get
            {
                if (rdd == null)
                {
                    rddProxy = dataFrameProxy.JavaToCSharp();
                    rdd = new RDD<Row>(rddProxy, sparkContext, SerializedMode.Row);
                }
                return rdd;
            }
        }

        private IRDDProxy RddProxy
        {
            get
            {
                if (rddProxy == null)
                {
                    rddProxy = dataFrameProxy.JavaToCSharp();
                    rdd = new RDD<Row>(rddProxy, sparkContext, SerializedMode.Row);
                }
                return rddProxy;
            }
        }

        /// <summary>
        /// Returns true if the collect and take methods can be run locally (without any Spark executors).
        /// </summary>
        public bool IsLocal
        {
            get
            {
                if (!isLocal.HasValue)
                {
                    isLocal = dataFrameProxy.IsLocal();
                }
                return isLocal.Value;
            }
        }

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

        /// <summary>
        /// Returns the schema of this DataFrame.
        /// </summary>
        public StructType Schema
        {
            get { return schema ?? (schema = new StructType(dataFrameProxy.GetSchema())); }
        }

        /// <summary>
        /// Returns a column for a given column name.
        /// </summary>
        /// <param name="columnName">The name of column</param>
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
            logger.LogInfo("Calculating the number of rows in the dataframe");
            return dataFrameProxy.Count();
        }

        /// <summary>
        /// Displays rows of the DataFrame in tabular form
        /// </summary>
        /// <param name="numberOfRows">Number of rows to display - default 20</param>
        /// <param name="truncate">Indicates if strings more than 20 characters long will be truncated</param>
        public void Show(int numberOfRows = 20, bool truncate = true)
        {
            logger.LogInfo("Writing {0} rows in the DataFrame to Console output", numberOfRows);
            Console.WriteLine(dataFrameProxy.GetShowString(numberOfRows, truncate));
        }

        /// <summary>
        /// Prints the schema information of the DataFrame
        /// </summary>
        public void ShowSchema()
        {
            var nameTypeList = Schema.Fields.Select(structField => structField.SimpleString);
            logger.LogInfo("Writing Schema to Console output");
            Console.WriteLine(string.Join(", ", nameTypeList));
        }

        /// <summary>
        /// Returns all of Rows in this DataFrame
        /// </summary>
        public IEnumerable<Row> Collect()
        {
            int port = RddProxy.CollectAndServe();
            return Rdd.Collect(port).Cast<Row>();
        }

        /// <summary>
        /// Converts the DataFrame to RDD of Row
        /// </summary>
        /// <returns>resulting RDD</returns>
        public RDD<Row> ToRDD() //RDD created using byte representation of Row objects
        {
            return Rdd;
        }

        /// <summary>
        /// Returns the content of the DataFrame as RDD of JSON strings
        /// </summary>
        /// <returns>resulting RDD</returns>
        public RDD<string> ToJSON()
        {
            var stringRddReference = dataFrameProxy.ToJSON();
            return new RDD<string>(stringRddReference, sparkContext, SerializedMode.String);
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
        /// Selects a set of columns specified by column name or Column.
        /// 
        /// df.Select("colA", df["colB"])
        /// df.Select("*", df["colB"] + 10)
        /// 
        /// </summary>
        /// <param name="firstColumn">first column - required, must be of type string or Column</param>
        /// <param name="otherColumns">other column - optional, must be of type string or Column</param>
        /// <returns></returns>
        public DataFrame Select(object firstColumn, params object[] otherColumns)
        {
            var originalColumns = new List<object> { firstColumn };
            originalColumns.AddRange(otherColumns);
            var invalidColumn = originalColumns.FirstOrDefault(c => !(c is string || c is Column));
            if (invalidColumn != null)
            {
                throw new ArgumentException(string.Format("one or more parameter is not string or Column: {0}", invalidColumn));
            }

            var columns = originalColumns.Select(oc => oc is string ? Functions.Col((string)oc) : (Column)oc);

            return new DataFrame(dataFrameProxy.Select(columns.Select(c => c.ColumnProxy)), sparkContext);
        }

        /// <summary>
        /// Selects a set of columns. This is a variant of `select` that can only select
        /// existing columns using column names (i.e. cannot construct expressions).
        /// 
        /// df.Select("colA", "colB")
        /// 
        /// </summary>
        /// <param name="firstColumnName">first column name - required</param>
        /// <param name="otherColumnNames">other column names - optional</param>
        /// <returns></returns>
        public DataFrame Select(string firstColumnName, params string[] otherColumnNames)
        {
            return new DataFrame(dataFrameProxy.Select(firstColumnName, otherColumnNames), sparkContext);
        }

        /// <summary>
        /// Selects a set of SQL expressions. This is a variant of `select` that accepts SQL expressions.
        ///
        ///   df.SelectExpr("colA", "colB as newName", "abs(colC)")
        ///   
        /// </summary>
        /// <param name="columnExpressions"></param>
        /// <returns></returns>
        public DataFrame SelectExpr(params string[] columnExpressions)
        {
            return new DataFrame(dataFrameProxy.SelectExpr(columnExpressions), sparkContext);
        }

        // /// <summary>
        // /// TO DO:  to be decided whether to expose this API
        // /// 
        // ///     1. has alternative - sql("<SQL scripts>")
        // ///     2. perf impact comapred to sql() - 1 more java call per each Column in select list
        // ///     
        // /// Select a list of columns
        // /// </summary>
        // /// <param name="columns"></param>
        // /// <returns></returns>
        //public DataFrame Select(params Column[] columns)
        //{
        //    List<IColumnProxy> columnReferenceList = columns.Select(column => column.ColumnProxy).ToList();
        //    IColumnProxy columnReferenceSeq = dataFrameProxy.ToColumnSeq(columnReferenceList);
        //    return new DataFrame(dataFrameProxy.Select(columnReferenceSeq), sparkContext);
        //}

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
        /// <param name="firstColumnName">first column name - required</param>
        /// <param name="otherColumnNames">other column names - optional</param>
        /// <returns></returns>
        public GroupedData GroupBy(string firstColumnName, params string[] otherColumnNames)
        {
            var scalaGroupedDataReference = dataFrameProxy.GroupBy(firstColumnName, otherColumnNames);
            return new GroupedData(scalaGroupedDataReference, this);
        }

        /// <summary>
        /// Create a multi-dimensional rollup for the current DataFrame using the specified columns, so we can run aggregation on them.
        /// </summary>
        /// <param name="firstColumnName">first column name - required</param>
        /// <param name="otherColumnNames">other column names - optional</param>
        /// <returns></returns>
        public GroupedData Rollup(string firstColumnName, params string[] otherColumnNames)
        {
            return new GroupedData(dataFrameProxy.Rollup(firstColumnName, otherColumnNames), this);
        }

        /// <summary>
        /// Create a multi-dimensional cube for the current DataFrame using the specified columns, so we can run aggregation on them.
        /// </summary>
        /// <param name="firstColumnName">first column name - required</param>
        /// <param name="otherColumnNames">other column names - optional</param>
        /// <returns></returns>
        public GroupedData Cube(string firstColumnName, params string[] otherColumnNames)
        {
            return new GroupedData(dataFrameProxy.Cube(firstColumnName, otherColumnNames), this);
        }

        private GroupedData GroupBy()
        {
            var scalaGroupedDataReference = dataFrameProxy.GroupBy();
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
        /// Join with another DataFrame - Cartesian join
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Join with another DataFrame - Inner equi-join using given column name
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <param name="joinColumnName">Column to join with.</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame, string joinColumnName) // TODO: need aliasing for self join
        {
            return new DataFrame(
                dataFrameProxy.Join(otherDataFrame.dataFrameProxy, joinColumnName),
                sparkContext);
        }

        /// <summary>
        /// Join with another DataFrame - Inner equi-join using given column name 
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <param name="joinColumnNames">Columns to join with.</param>
        /// <returns>Joined DataFrame</returns>
        public DataFrame Join(DataFrame otherDataFrame, string[] joinColumnNames) // TODO: need aliasing for self join
        {
            return new DataFrame(
                dataFrameProxy.Join(otherDataFrame.dataFrameProxy, joinColumnNames),
                sparkContext);
        }

        /// <summary>
        /// Join with another DataFrame, using the specified JoinType
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to join with</param>
        /// <param name="joinExpression">Column to join with.</param>
        /// <param name="joinType">Type of join to perform (default null value means <c>JoinType.Inner</c>)</param>
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
        /// Intersect with another DataFrame.
        /// This is equivalent to `INTERSECT` in SQL.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, intersect(self, other)
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to intersect with.</param>
        /// <returns>Intersected DataFrame.</returns>
        public DataFrame Intersect(DataFrame otherDataFrame)
        {
            return
                new DataFrame(dataFrameProxy.Intersect(otherDataFrame.dataFrameProxy), sparkContext);
        }

        /// <summary>
        /// Union with another DataFrame WITHOUT removing duplicated rows.
        /// This is equivalent to `UNION ALL` in SQL.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, unionAll(self, other)
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to union all with.</param>
        /// <returns>Unioned DataFrame.</returns>
        public DataFrame UnionAll(DataFrame otherDataFrame)
        {
            return
                new DataFrame(dataFrameProxy.UnionAll(otherDataFrame.dataFrameProxy), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame containing rows in this frame but not in another frame.
        /// This is equivalent to `EXCEPT` in SQL.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, subtract(self, other)
        /// </summary>
        /// <param name="otherDataFrame">DataFrame to subtract from this frame.</param>
        /// <returns>A new DataFrame containing rows in this frame but not in another frame.</returns>
        public DataFrame Subtract(DataFrame otherDataFrame)
        {
            return
                new DataFrame(dataFrameProxy.Subtract(otherDataFrame.dataFrameProxy), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame with a column dropped.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, drop(self, col)
        /// </summary>
        /// <param name="columnName"> a string name of the column to drop</param>
        /// <returns>A new new DataFrame that drops the specified column.</returns>
        public DataFrame Drop(string columnName)
        {
            return
                new DataFrame(dataFrameProxy.Drop(columnName), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame omitting rows with null values.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, dropna(self, how='any', thresh=None, subset=None)
        /// </summary>
        /// <param name="how">'any' or 'all'. 
        /// If 'any', drop a row if it contains any nulls.
        /// If 'all', drop a row only if all its values are null.</param>
        /// <param name="thresh">thresh: int, default null.
        /// If specified, drop rows that have less than `thresh` non-null values.
        /// This overwrites the `how` parameter.</param>
        /// <param name="subset">optional list of column names to consider.</param>
        /// <returns>A new DataFrame omitting rows with null values</returns>
        public DataFrame DropNa(string how = "any", int? thresh = null, string[] subset = null)
        {
            if (how != "any" && how != "all")
                throw new ArgumentException(string.Format(@"how ({0}) should be 'any' or 'all'.", how));

            if (subset == null || subset.Length == 0)
                subset = dataFrameProxy.GetSchema().GetStructTypeFields().Select(f => f.GetStructFieldName().ToString(CultureInfo.InvariantCulture)).ToArray();

            if (!thresh.HasValue)
                thresh = how == "any" ? subset.Length : 1;

            return Na().Drop(thresh.Value, subset);
        }

        /// <summary>
        /// Returns a DataFrameNaFunctions for working with missing data.
        /// </summary>
        /// Reference: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, na(self)
        public DataFrameNaFunctions Na()
        {
            return new DataFrameNaFunctions(dataFrameProxy.Na(), this, sparkContext);
        }

        /// <summary>
        /// Replace null values, alias for ``na.fill()`
        /// </summary>
        /// <param name="value">
        /// Value to replace null values with.
        /// Value type should be float, double, short, int, long, string or Dictionary.
        /// If the value is a dict, then `subset` is ignored and `value` must be a mapping
        /// from column name (string) to replacement value. The replacement value must be
        /// an int, long, float, or string.
        /// </param>
        /// <param name="subset">
        /// optional list of column names to consider.
        /// Columns specified in subset that do not have matching data type are ignored.
        /// For example, if `value` is a string, and subset contains a non-string column,
        /// then the non-string column is simply ignored.
        /// </param>
        // Reference: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, fillna(self, value, subset=None)
        public DataFrame FillNa(dynamic value, string[] subset = null)
        {
            var validTypes = new[] { typeof(int), typeof(short), typeof(long), typeof(double), typeof(float), typeof(string), typeof(Dictionary<string,dynamic>) };
            if (validTypes.All(t => t != value.GetType()))
                throw new ArgumentException("value type should be float, double, short, int, long, string or Dictionary.");

            var dict = value as Dictionary<string, dynamic>;
            if (dict != null)
            {
                return Na().Fill(dict);
            }

            return subset == null ? Na().Fill(value) : Na().Fill(value, subset);
        }

        /// <summary>
        /// Returns a new DataFrame with duplicate rows removed, considering only the subset of columns.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, dropDuplicates(self, subset=None)
        /// </summary>
        /// <param name="subset">drop duplicated rows on these columns.</param>
        /// <returns>A new DataFrame with duplicate rows removed.</returns>
        public DataFrame DropDuplicates(string[] subset = null)
        {
            return (subset == null || subset.Length == 0) ?
                new DataFrame(dataFrameProxy.DropDuplicates(), sparkContext) :
                new DataFrame(dataFrameProxy.DropDuplicates(subset), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame replacing a value with another value.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, replace(self, to_replace, value, subset=None)
        /// </summary>
        /// <typeparam name="T">Data type of value to replace.</typeparam>
        /// <param name="toReplace">Value to be replaced. The value to be replaced must be an int, long, float, or string and must be the same type as <paramref name="value"/>.</param>
        /// <param name="value">Value to use to replace holes. The replacement value must be an int, long, float, or string and must be the same type as <paramref name="toReplace"/>.</param>
        /// <param name="subset">Optional list of column names to consider.</param>
        /// <returns>A new DataFrame replacing a value with another value</returns>
        public DataFrame Replace<T>(T toReplace, T value, string[] subset = null)
        {
            var toReplaceAndValueDict = new Dictionary<T, T> { { toReplace, value } };
            return ReplaceCore(toReplaceAndValueDict, subset);
        }

        /// <summary>
        /// Returns a new DataFrame replacing values with other values.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, replace(self, to_replace, value, subset=None)
        /// </summary>
        /// <typeparam name="T">Data type of values to replace.</typeparam>
        /// <param name="toReplace">List of values to be replaced. The value to be replaced must be an int, long, float, or string and must be the same type as <paramref name="value"/>. 
        /// This list should be of the same length with <paramref name="value"/>.</param>
        /// <param name="value">List of values to replace holes. The replacement must be an int, long, float, or string and must be the same type as <paramref name="toReplace"/>.
        /// This list should be of the same length with <paramref name="toReplace"/>.</param>
        /// <param name="subset">Optional list of column names to consider.</param>
        /// <returns>A new DataFrame replacing values with other values</returns>
        public DataFrame ReplaceAll<T>(IEnumerable<T> toReplace, IEnumerable<T> value, string[] subset = null)
        {
            var toReplaceArray = toReplace.ToArray();
            var valueArray = value.ToArray();
            if (toReplaceArray.Length != valueArray.Length)
                throw new ArgumentException("toReplace and value lists should be of the same length");

            var toReplaceAndValueDict = toReplaceArray.Zip(valueArray, (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v);

            return ReplaceCore(toReplaceAndValueDict, subset);
        }

        /// <summary>
        /// Returns a new DataFrame replacing values with another value.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, replace(self, to_replace, value, subset=None)
        /// </summary>
        /// <typeparam name="T">Data type of values to replace.</typeparam>
        /// <param name="toReplace">List of values to be replaced. The value to be replaced must be an int, long, float, or string and must be the same type as <paramref name="value"/>.</param>
        /// <param name="value">Value to use to replace holes. The replacement value must be an int, long, float, or string and must be the same type as <paramref name="toReplace"/>.</param>
        /// <param name="subset">Optional list of column names to consider.</param>
        /// <returns>A new DataFrame replacing values with another value</returns>
        public DataFrame ReplaceAll<T>(IEnumerable<T> toReplace, T value, string[] subset = null)
        {
            var toReplaceArray = toReplace.ToArray();
            var toReplaceAndValueDict = toReplaceArray.Zip(Enumerable.Repeat(value, toReplaceArray.Length).ToList(), (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v);

            return ReplaceCore(toReplaceAndValueDict, subset);
        }

        /// <summary>
        /// Randomly splits this DataFrame with the provided weights.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, randomSplit(self, weights, seed=None)
        /// </summary>
        /// <param name="weights">list of weights with which to split the DataFrame. Weights will be normalized if they don't sum up to 1.0</param>
        /// <param name="seed">The seed for sampling</param>
        /// <returns></returns>
        public IEnumerable<DataFrame> RandomSplit(IEnumerable<double> weights, int? seed = null)
        {
            foreach (var weight in weights)
            {
                if (weight < 0.0)
                {
                    throw new ArgumentException(string.Format("Weights must be positive. Found weight value: {0}", weight));
                }
            }

            if (seed == null) 
                seed = new Random().Next();

            return dataFrameProxy.RandomSplit(weights, seed.Value).Select(dfProxy => new DataFrame(dfProxy, sparkContext));
        }

        /// <summary>
        /// Returns all column names as a list.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, columns(self)
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> Columns()
        {
            return dataFrameProxy.GetSchema().GetStructTypeFields().Select(f => f.GetStructFieldName());
        }

        /// <summary>
        /// Returns all column names and their data types.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, dtypes(self)
        /// </summary>
        /// <returns>Column names and their data types.</returns>
        public IEnumerable<Tuple<string, string>> DTypes()
        {
            return dataFrameProxy.GetSchema().GetStructTypeFields().Select(f => new Tuple<string, string>(f.GetStructFieldName(), f.GetStructFieldDataType().GetDataTypeSimpleString()));
        }

        /// <summary>
        /// Returns a new DataFrame sorted by the specified column(s).
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, sort(self, *cols, **kwargs)
        /// </summary>
        /// <param name="columns">List of column names to sort by</param>
        /// <param name="ascending">List of boolean to specify multiple sort orders for <paramref name="columns"/>, TRUE for ascending, FALSE for descending</param>
        /// <returns>A new DataFrame sorted by the specified column(s)</returns>
        public DataFrame Sort(string[] columns, bool[] ascending)
        {
            if (columns == null || columns.Length == 0)
            {
                throw new ArgumentException("should sort by at least one column.");
            }
            if (columns.Length != ascending.Length)
            {
                throw new ArgumentException("ascending should have the same length with columns");
            }
            return Sort(columns.Select(c => this[c]).ToArray(), ascending);
        }

        /// <summary>
        /// Returns a new DataFrame sorted by the specified column(s).
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, sort(self, *cols, **kwargs)
        /// </summary>
        /// <param name="columns">List of Columns to sort by</param>
        /// <param name="ascending">List of boolean to specify multiple sort orders for <paramref name="columns"/>, TRUE for ascending, FALSE for descending.
        /// if not null, it will overwrite the order specified by Column.Asc() or Column Desc() in <paramref name="columns"/>, </param>
        /// <returns>A new DataFrame sorted by the specified column(s)</returns>
        public DataFrame Sort(Column[] columns, bool[] ascending = null)
        {
            if (columns == null || columns.Length == 0)
            {
                throw new ArgumentException("should sort by at least one column.");
            }
            if (ascending != null)
            {
                var sortedColumns = SortColumns(columns, ascending);
                return new DataFrame(dataFrameProxy.Sort(sortedColumns.Select(c => c.ColumnProxy).ToArray()), sparkContext);
            }
            return new DataFrame(dataFrameProxy.Sort(columns.Select(c => c.ColumnProxy).ToArray()), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame sorted by the specified column(s).
        /// Reference to https://github.com/apache/spark/blob/branch-1.6/python/pyspark/sql/dataframe.py, sortWithinPartitions(self, *cols, **kwargs)
        /// </summary>
        /// <param name="columns">List of Columns to sort by</param>
        /// <param name="ascending">List of boolean to specify multiple sort orders for <paramref name="columns"/>, TRUE for ascending, FALSE for descending.
        /// if not null, it will overwrite the order specified by Column.Asc() or Column Desc() in <paramref name="columns"/>, </param>
        /// <returns>A new DataFrame sorted by the specified column(s)</returns>
        public DataFrame SortWithinPartitions(string[] columns, bool[] ascending = null)
        {
            if (columns == null || columns.Length == 0)
            {
                throw new ArgumentException("should sort by at least one column.");
            }
            if (ascending != null)
            {
                var sortedColumns = SortColumns(columns.Select(c => this[c]).ToArray(), ascending);
                return new DataFrame(dataFrameProxy.SortWithinPartitions(sortedColumns.Select(c => c.ColumnProxy).ToArray()), sparkContext);
            }
            return new DataFrame(dataFrameProxy.SortWithinPartitions(columns.Select(c => this[c].ColumnProxy).ToArray()), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame sorted by the specified column(s).
        /// Reference to https://github.com/apache/spark/blob/branch-1.6/python/pyspark/sql/dataframe.py, sortWithinPartitions(self, *cols, **kwargs)
        /// </summary>
        /// <param name="columns">List of Columns to sort by</param>
        /// <param name="ascending">List of boolean to specify multiple sort orders for <paramref name="columns"/>, TRUE for ascending, FALSE for descending.
        /// if not null, it will overwrite the order specified by Column.Asc() or Column Desc() in <paramref name="columns"/>, </param>
        /// <returns>A new DataFrame sorted by the specified column(s)</returns>
        public DataFrame SortWithinPartition(Column[] columns, bool[] ascending = null)
        {
            if (columns == null || columns.Length == 0)
            {
                throw new ArgumentException("should sort by at least one column.");
            }
            if (ascending != null)
            {
                var sortedColumns = SortColumns(columns, ascending);
                return new DataFrame(dataFrameProxy.SortWithinPartitions(sortedColumns.Select(c => c.ColumnProxy).ToArray()), sparkContext);
            }
            return new DataFrame(dataFrameProxy.SortWithinPartitions(columns.Select(c => c.ColumnProxy).ToArray()), sparkContext);
        }

        private Column[] SortColumns(Column[] columns, bool[] ascending)
        {
            if (columns.Length != ascending.Length)
                throw new ArgumentException("ascending should have the same length with columns");

            var columnsWithOrder = new Column[columns.Length];
            for (var i = 0; i < columns.Length; i++)
            {
                columnsWithOrder[i] = ascending[i] ? columns[i].Asc() : columns[i].Desc();
            }
            return columnsWithOrder;
        }

        /// <summary>
        /// Returns a new DataFrame with an alias set.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, alias(self, alias) 
        /// </summary>
        /// <param name="alias">The alias of the DataFrame</param>
        /// <returns>A new DataFrame with an alias set</returns>
        public DataFrame Alias(string alias)
        {
            return new DataFrame(dataFrameProxy.Alias(alias), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame by adding a column.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, withColumn(self, colName, col)
        /// </summary>
        /// <param name="newColName">name of the new column</param>
        /// <param name="column">a Column expression for the new column</param>
        /// <returns>A new DataFrame with the added column</returns>
        public DataFrame WithColumn(string newColName, Column column)
        {
            return Select("*", column.Alias(newColName));
        }

        /// <summary>
        /// Returns a new DataFrame by renaming an existing column.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, withColumnRenamed(self, existing, new)
        /// </summary>
        /// <param name="existingName">name of an existing column</param>
        /// <param name="newName">new name</param>
        /// <returns>A new DataFrame with renamed column</returns>
        public DataFrame WithColumnRenamed(string existingName, string newName)
        {
            var columns = new List<Column>();
            foreach (var col in Columns())
            {
                columns.Add(col == existingName ? this[existingName].Alias(newName) : this[col]);
            }
            // select columns including the column renamed
            return Select(columns[0], columns.Skip(1).ToArray());
        }

        /// <summary>
        /// Calculates the correlation of two columns of a DataFrame as a double value.
        /// Currently only supports the Pearson Correlation Coefficient.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, corr(self, col1, col2, method=None)
        /// </summary>
        /// <param name="column1">The name of the first column</param>
        /// <param name="column2">The name of the second column</param>
        /// <param name="method">The correlation method. Currently only supports "pearson"</param>
        /// <returns>The correlation of two columns</returns>
        public double Corr(string column1, string column2, string method = "pearson")
        {
            if (!method.Equals("pearson", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Currently only the calculation of the Pearson Correlation coefficient is supported.");

            return dataFrameProxy.Corr(column1, column2, method);
        }

        /// <summary>
        /// Calculate the sample covariance of two columns as a double value.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, cov(self, col1, col2)
        /// </summary>
        /// <param name="column1">The name of the first column</param>
        /// <param name="column2">The name of the second column</param>
        /// <returns>The sample covariance of two columns</returns>
        public double Cov(string column1, string column2)
        {
            return dataFrameProxy.Cov(column1, column2);
        }

        /// <summary>
        /// Finding frequent items for columns, possibly with false positives. Using the frequent element count algorithm described in 
        /// "http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou".
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, freqItems(self, cols, support=None)
        /// 
        /// <alert class="note">Note: This function is meant for exploratory data analysis, as we make no guarantee about the backward compatibility 
        /// of the schema of the resulting DataFrame. </alert>
        /// </summary>
        /// <param name="columns"></param>
        /// <param name="support"></param>
        /// <returns></returns>
        public DataFrame FreqItems(IEnumerable<string> columns, double support = 0.01)
        {
            return new DataFrame(dataFrameProxy.FreqItems(columns, support), sparkContext);
        }

        /// <summary>
        /// Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
        /// The number of distinct values for each column should be less than 1e4. At most 1e6 non-zero pair frequencies will be returned.
        /// Reference to https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py, crosstab(self, col1, col2)
        /// </summary>
        /// <param name="column1">The name of the first column. Distinct items will make the first item of each row</param>
        /// <param name="column2">The name of the second column. Distinct items will make the column names of the DataFrame</param>
        /// <returns>A pair-wise frequency table of the given columns</returns>
        public DataFrame Crosstab(string column1, string column2)
        {
            return new DataFrame(dataFrameProxy.Crosstab(column1, column2), sparkContext);
        }

        /// <summary>
        /// Computes statistics for numeric columns.
        /// This include count, mean, stddev, min, and max. If no columns are given, this function computes statistics for all numerical columns.
        /// </summary>
        /// <param name="columns">Column names to compute statistics.</param>
        /// <returns></returns>
        public DataFrame Describe(params string[] columns)
        {
            return new DataFrame(dataFrameProxy.Describe(columns), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame by taking the first `n` rows.
        /// The difference between this function and `head` is that `head` returns an array while `limit` returns a new DataFrame.
        /// </summary>
        /// <param name="num">Number of rows to take from current DataFrame</param>
        /// <returns>A new DataFrame containing the first `n` rows</returns>
        public DataFrame Limit(int num)
        {
            return
                new DataFrame(dataFrameProxy.Limit(num), sparkContext);
        }

        /// <summary>
        /// Returns the first `n` rows.
        /// </summary>
        public IEnumerable<Row> Head(int num)
        {
            return Limit(num).Collect();
        }

        /// <summary>
        /// Returns the first row.
        /// </summary>
        public Row First()
        {
            return Head(1).First();
        }

        /// <summary>
        /// Returns the first `n` rows in the DataFrame.
        /// </summary>
        public IEnumerable<Row> Take(int num)
        {
            return Head(num);
        }

        /// <summary>
        /// Returns a new DataFrame that contains only the unique rows from this DataFrame.
        /// </summary>
        public DataFrame Distinct()
        {
            return new DataFrame(dataFrameProxy.Distinct(), sparkContext);
        }

        private DataFrame ReplaceCore<T>(Dictionary<T, T> toReplaceAndValue, string[] subset)
        {
            var validTypes = new[] { typeof(int), typeof(short), typeof(long), typeof(double), typeof(float), typeof(string) };
            if (!validTypes.Any(t => t == typeof(T)))
                throw new ArgumentException("toReplace and value should be a float, double, short, int, long or string");
            
            object subsetObj;
            if (subset == null || subset.Length == 0)
            {
                subsetObj = "*";
            }
            else
            {
                subsetObj = subset;
            }
            return new DataFrame(dataFrameProxy.Replace(subsetObj, toReplaceAndValue), sparkContext);
        }
		
        /// <summary>
        /// Returns a new DataFrame that has exactly `numPartitions` partitions.
        /// Similar to coalesce defined on an RDD, this operation results in a narrow dependency, 
        /// e.g. if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
        /// the 100 new partitions will claim 10 of the current partitions.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py coalesce(self, numPartitions)
        public DataFrame Coalesce(int numPartitions)
        {
            return new DataFrame(dataFrameProxy.Coalesce(numPartitions), sparkContext);
        }

        /// <summary>
        /// Persist this DataFrame with the default storage level (`MEMORY_AND_DISK`)
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py persist(self, storageLevel)
        public DataFrame Persist()
        {
            dataFrameProxy.Persist(StorageLevelType.MEMORY_AND_DISK);
            return this;
        }

        /// <summary>
        /// Mark the DataFrame as non-persistent, and remove all blocks for it from memory and disk.
        /// </summary>
        /// <param name="blocking">Whether to block until all blocks are deleted.</param>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py unpersist(self, blocking=True)
        public DataFrame Unpersist(bool blocking = true)
        {
            dataFrameProxy.Unpersist(blocking);
            return this;
        }

        /// <summary>
        /// Persist this DataFrame with the default storage level (`MEMORY_AND_DISK`)
        /// </summary>
        // https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py cache(self)
        public DataFrame Cache()
        {
            return Persist();
        }

        /// <summary>
        /// Returns a new DataFrame that has exactly `numPartitions` partitions.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py repartition(self, numPartitions)
        public DataFrame Repartition(int numPartitions)
        {
            return new DataFrame(dataFrameProxy.Repartition(numPartitions), sparkContext);
        }

        /// <summary>
        /// Returns a new [[DataFrame]] partitioned by the given partitioning columns into <paramref name="numPartitions"/>. The resulting DataFrame is hash partitioned.
        /// <param name="columns"></param>
        /// <param name="numPartitions">optional. If not specified, keep current partitions.</param>
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.6/python/pyspark/sql/dataframe.py repartition(self, numPartitions)
        public DataFrame Repartition(string[] columns, int numPartitions = 0)
        {
            return numPartitions == 0 ?
                new DataFrame(dataFrameProxy.Repartition(columns.Select(c => this[c].ColumnProxy).ToArray()), sparkContext) :
                new DataFrame(dataFrameProxy.Repartition(numPartitions, columns.Select(c => this[c].ColumnProxy).ToArray()), sparkContext);
        }

        /// <summary>
        /// Returns a new [[DataFrame]] partitioned by the given partitioning columns into <paramref name="numPartitions"/>. The resulting DataFrame is hash partitioned.
        /// <param name="columns"></param>
        /// <param name="numPartitions">optional. If not specified, keep current partitions.</param>
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.6/python/pyspark/sql/dataframe.py repartition(self, numPartitions)
        public DataFrame Repartition(Column[] columns, int numPartitions = 0)
        {
            return numPartitions == 0 ?
                new DataFrame(dataFrameProxy.Repartition(columns.Select(c => c.ColumnProxy).ToArray()), sparkContext) :
                new DataFrame(dataFrameProxy.Repartition(numPartitions, columns.Select(c => c.ColumnProxy).ToArray()), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame by sampling a fraction of rows.
        /// </summary>
        /// <param name="withReplacement"> Sample with replacement or not. </param>
        /// <param name="fraction"> Fraction of rows to generate. </param>
        /// <param name="seed"> Seed for sampling. If it is not present, a randome long value will be assigned. </param>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py
        // sample(self, withReplacement, fraction, seed=None)
        public DataFrame Sample(bool withReplacement, double fraction, long? seed)
        {
            long v;
            if (seed.HasValue)
            {
                v = seed.Value;
            }
            else
            {
                v = ((long)random.Next()) << 32 + random.Next();
            }
            return new DataFrame(dataFrameProxy.Sample(withReplacement, fraction, v), sparkContext);
        }

        /// <summary>
        /// Returns a new RDD by first applying a function to all rows of this DataFrame, and then flattening the results.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py flatMap(self, f)
        public RDD<U> FlatMap<U>(Func<Row, IEnumerable<U>> f, bool preservesPartitioning = false)
        {
            return Rdd.FlatMap(f, preservesPartitioning);
        }

        /// <summary>
        /// Returns a new RDD by applying a function to all rows of this DataFrame.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py map(self, f)
        public RDD<U> Map<U>(Func<Row, U> f)
        {
            return Rdd.Map(f);
        }

        /// <summary>
        /// Returns a new RDD by applying a function to each partition of this DataFrame.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py
        // mapPartitions(self, f, preservesPartitioning=False)
        public RDD<U> MapPartitions<U>(Func<IEnumerable<Row>, IEnumerable<U>> f, bool preservesPartitioning = false)
        {
            return Rdd.MapPartitions(f, preservesPartitioning);
        }

        /// <summary>
        /// Applies a function f to each partition of this DataFrame.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py
        // foreachPartition(self, f)
        public void ForeachPartition(Action<IEnumerable<Row>> f)
        {
            Rdd.ForeachPartition(f);
        }

        /// <summary>
        /// Applies a function f to all rows.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py
        // foreach(self, f)
        public void Foreach(Action<Row> f)
        {
            Rdd.Foreach(f);
        }

        /// <summary>
        /// Interface for saving the content of the DataFrame out into external storage.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py
        // write(self)
        public DataFrameWriter Write()
        {
            logger.LogInfo("Using DataFrameWriter to write output data to external data storage");
            return new DataFrameWriter(dataFrameProxy.Write());
        }

        /// <summary>
        /// Saves the contents of this DataFrame as a parquet file, preserving the schema.
        /// Files that are written out using this method can be read back in as a DataFrame
        /// using the `parquetFile` function in SQLContext.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py
        // saveAsParquetFile(self, path)
        [Obsolete(" Deprecated. As of 1.4.0, replaced by write().parquet()")]
        public void SaveAsParquetFile(String path)
        {
            Write().Parquet(path);
        }

        /// <summary>
        /// Adds the rows from this RDD to the specified table, optionally overwriting the existing data.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py
        // insertInto(self, tableName, overwrite=False)
        [Obsolete("Deprecated. As of 1.4.0, replaced by write().mode(SaveMode.Append|SaveMode.Overwrite).saveAsTable(tableName)")]
        public void InsertInto(string tableName, bool overwrite = false)
        {
            var mode = overwrite ? SaveMode.Overwrite : SaveMode.Append;
            Write().Mode(mode).InsertInto(tableName);
        }
     
        /// <summary>
        /// Creates a table from the the contents of this DataFrame based on a given data source, 
        /// SaveMode specified by mode, and a set of options.
        /// 
        /// Note that this currently only works with DataFrames that are created from a HiveContext as
        /// there is no notion of a persisted catalog in a standard SQL context.  Instead you can write
        /// an RDD out to a parquet file, and then register that file as a table.  This "table" can then
        /// be the target of an `insertInto`.
        /// 
        /// Also note that while this function can persist the table metadata into Hive's metastore,
        /// the table will NOT be accessible from Hive, until SPARK-7550 is resolved.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py
        // saveAsTable(self, tableName, source=None, mode="error", **options)
        [Obsolete("Deprecated. As of 1.4.0, replaced by write().format(source).mode(mode).options(options).saveAsTable(tableName)")]
        public void SaveAsTable(string tableName, string source = null, SaveMode mode = SaveMode.ErrorIfExists, params string[] options)
        {
            var dataFrameWriter = Write().Mode(mode);
            if (source != null)
            {
                dataFrameWriter = dataFrameWriter.Format(source);
            }
            if (options.Length > 0)
            {
                dataFrameWriter = dataFrameWriter.Options(ParamsToDict(options));
            }
            dataFrameWriter.SaveAsTable(tableName);
        }

        /// <summary>
        /// Saves the contents of this DataFrame based on the given data source, 
        /// SaveMode specified by mode, and a set of options.
        /// </summary>
        // Python API: https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/dataframe.py
        // save(self, path=None, source=None, mode="error", **options)
        [Obsolete("Deprecated. As of 1.4.0, replaced by write().format(source).mode(mode).options(options).save(path)")]
        public void Save(string path = null, string source = null, SaveMode mode = SaveMode.ErrorIfExists, params string[] options)
        {
            var dataFrameWriter = Write().Mode(mode);
            if (source != null)
            {
                dataFrameWriter = dataFrameWriter.Format(source);
            }
            if (options.Length > 0)
            {
                dataFrameWriter = dataFrameWriter.Options(ParamsToDict(options));
            }
            if (path != null)
            {
                dataFrameWriter.Save(path);
            }
            else
            {
                dataFrameWriter.Save();
            }
        }

        // Helper method to convert variable number of arguments to a dictionary.
        private Dictionary<string, string> ParamsToDict(params string[] options)
        {
            var optionDict = new Dictionary<string, string>();
            if (options.Length <= 0)
            {
                return optionDict;
            }
            if (options.Length % 2 != 0)
            {
                throw new ArgumentException("options length must be even.");
            }
            for (var i = 0; i < options.Length; i++)
            {
                optionDict[options[i]] = options[++i];
            }
            return optionDict;
        } 
    }

    /// <summary>
    /// The type of join operation for DataFrame
    /// </summary>
    public class JoinType
    {
        /// <summary>
        /// Get the string that represents a join type
        /// </summary>
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

        /// <summary>
        /// Inner join
        /// </summary>
        public static JoinType Inner
        {
            get
            {
                return InnerJoinType;
            }
        }

        /// <summary>
        /// Outer join
        /// </summary>
        public static JoinType Outer
        {
            get
            {
                return OuterJoinType;
            }
        }

        /// <summary>
        /// Left outer join
        /// </summary>
        public static JoinType LeftOuter
        {
            get
            {
                return LeftOuterJoinType;
            }
        }

        /// <summary>
        /// Right outer join
        /// </summary>
        public static JoinType RightOuter
        {
            get
            {
                return RightOuterJoinType;
            }
        }

        /// <summary>
        /// Left semi join
        /// </summary>
        public static JoinType LeftSemi
        {
            get
            {
                return LeftSemiJoinType;
            }
        }
    }

    /// <summary>
    /// A set of methods for aggregations on a DataFrame, created by DataFrame.groupBy.
    /// </summary>
    public class GroupedData
    {
        internal IGroupedDataProxy GroupedDataProxy
        {
            get { return groupedDataProxy; }
        }

        private readonly IGroupedDataProxy groupedDataProxy;
        private readonly DataFrame dataFrame;

        internal GroupedData(IGroupedDataProxy groupedDataProxy, DataFrame dataFrame)
        {
            this.groupedDataProxy = groupedDataProxy;
            this.dataFrame = dataFrame;
        }

        /// <summary>
        /// Compute aggregates by specifying a dictionary from column name to aggregate methods.
        /// The available aggregate methods are avg, max, min, sum, count.
        /// </summary>
        /// <param name="columnNameAggFunctionDictionary">The dictionary of column name to aggregate method</param>
        /// <returns>The DataFrame object that contains the grouping columns.</returns>
        public DataFrame Agg(Dictionary<string, string> columnNameAggFunctionDictionary)
        {
            return new DataFrame(dataFrame.DataFrameProxy.Agg(groupedDataProxy, columnNameAggFunctionDictionary), dataFrame.SparkContext);
        }

        /// <summary>
        /// Count the number of rows for each group.
        /// </summary>
        /// <returns>The DataFrame object that contains the grouping columns.</returns>
        public DataFrame Count()
        {
            return new DataFrame(groupedDataProxy.Count(), dataFrame.SparkContext);
        }

        /// <summary>
        /// Compute the average value for each numeric columns for each group.
        /// This is an alias for avg.
        /// When specified columns are given, only compute the average values for them. 
        /// </summary>
        /// <param name="columns">The name of columns to be computed.</param>
        /// <returns>The DataFrame object that contains the grouping columns.</returns>
        public DataFrame Mean(params string[] columns)
        {
            return new DataFrame(groupedDataProxy.Mean(columns), dataFrame.SparkContext);
        }

        /// <summary>
        /// Compute the max value for each numeric columns for each group.
        /// When specified columns are given, only compute the max values for them.
        /// </summary>
        /// <param name="columns"> The name of columns to be computed.</param>
        /// <returns>The DataFrame object that contains the grouping columns.</returns>
        public DataFrame Max(params string[] columns)
        {
            return new DataFrame(groupedDataProxy.Max(columns), dataFrame.SparkContext);
        }

        /// <summary>
        /// Compute the min value for each numeric column for each group.
        /// </summary>
        /// <param name="columns">
        /// The name of columns to be computed. When specified columns are
        /// given, only compute the min values for them. 
        /// </param>
        /// <returns>The DataFrame object that contains the grouping columns.</returns>
        public DataFrame Min(params string[] columns)
        {
            return new DataFrame(groupedDataProxy.Min(columns), dataFrame.SparkContext);
        }

        /// <summary>
        /// Compute the mean value for each numeric columns for each group.
        /// When specified columns are given, only compute the mean values for them. 
        /// </summary>
        /// <param name="columns">The name of columns to be computed</param>
        /// <returns>The DataFrame object that contains the grouping columns.</returns>
        public DataFrame Avg(params string[] columns)
        {
            return new DataFrame(groupedDataProxy.Avg(columns), dataFrame.SparkContext);
        }

        /// <summary>
        /// Compute the sum for each numeric columns for each group.
        /// When specified columns are given, only compute the sum for them.
        /// </summary>
        /// <param name="columns">The name of columns to be computed</param>
        /// <returns>The DataFrame object that contains the grouping columns.</returns>
        public DataFrame Sum(params string[] columns)
        {
            return new DataFrame(groupedDataProxy.Sum(columns), dataFrame.SparkContext);
        }
    }
}
