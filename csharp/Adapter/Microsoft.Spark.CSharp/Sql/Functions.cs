// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// DataFrame Built-in functions
    /// </summary>
    public static class Functions
    {
        #region functions
        /// <summary>
        /// Creates a Column of any literal value.
        /// </summary>
        /// <param name="column">The given literal value</param>
        /// <returns>A new Column is created to represent the literal value</returns>
        public static Column Lit(object column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("lit", column));
        }

        /// <summary>
        /// Returns a Column based on the given column name.
        /// </summary>
        /// <param name="colName">The name of column specified</param>
        /// <returns>The column for the given name</returns>
        public static Column Col(string colName)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("col", colName));
        }

        /// <summary>
        /// Returns a Column based on the given column name.
        /// </summary>
        /// <param name="colName">The name of column specified</param>
        /// <returns>The column for the given name</returns>
        public static Column Column(string colName)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("column", colName));
        }

        /// <summary>
        /// Returns a sort expression based on ascending order of the column.
        /// </summary>
        /// <param name="columnName">The name of column specified</param>
        /// <returns>The column with ascending order</returns>
        public static Column Asc(string columnName)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("asc", columnName));
        }

        /// <summary>
        /// Returns a sort expression based on the descending order of the column.
        /// </summary>
        /// <param name="columnName">The name of column specified</param>
        /// <returns>the column with descending order</returns>
        public static Column Desc(string columnName)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("desc", columnName));
        }

        /// <summary>
        /// Converts a string column to upper case.
        /// </summary>
        /// <param name="column">The string column specified</param>
        /// <returns>The string column in upper case</returns>
        public static Column Upper(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("upper", column.ColumnProxy));
        }

        /// <summary>
        /// Converts a string column to lower case.
        /// </summary>
        /// <param name="column">The string column specified</param>
        /// <returns>The string column in lower case</returns>
        public static Column Lower(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("lower", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the square root of the specified float column.
        /// </summary>
        /// <param name="column">The float column</param>
        /// <returns>The square root of the specified float column.</returns>
        public static Column Sqrt(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("sqrt", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the absolute value.
        /// </summary>
        /// <param name="column">The column to compute</param>
        /// <returns>The new column represents the absolute value of the given column</returns>
        public static Column Abs(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("abs", column.ColumnProxy));
        }

        /// <summary>
        /// Returns the maximum value of the expression in a group.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The new column reprents the maximum value</returns>
        public static Column Max(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("max", column.ColumnProxy));
        }

        /// <summary>
        /// Returns the minimum value of the expression in a group.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The new column represents the minimum value</returns>
        public static Column Min(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("min", column.ColumnProxy));
        }

        /// <summary>
        /// Returns the first value in a group. 
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The new column represents the first value</returns>
        public static Column First(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("first", column.ColumnProxy));
        }

        /// <summary>
        /// Returns the last value in a group.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The new column represents the last value</returns>
        public static Column Last(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("last", column.ColumnProxy));
        }

        /// <summary>
        /// Returns the number of items in a group.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The new column represents the count value</returns>
        public static Column Count(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("count", column.ColumnProxy));
        }

        /// <summary>
        /// Returns the sum of all values in the expression.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The new column represents the sum</returns>
        public static Column Sum(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("sum", column.ColumnProxy));
        }

        /// <summary>
        /// Returns the average of the values in a group.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The new column represents the average</returns>
        public static Column Avg(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("avg", column.ColumnProxy));
        }

        /// <summary>
        /// Returns the average of the values in a group.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The new column represents the average</returns>
        public static Column Mean(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("mean", column.ColumnProxy));
        }

        /// <summary>
        /// Returns the sum of distinct values in the expression.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The new column represents the sum of distinct values </returns>
        public static Column SumDistinct(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("sumDistinct", column.ColumnProxy));
        }

        /// <summary>
        /// Creates a new array column. The input columns must all have the same data type. 
        /// </summary>
        /// <param name="columns">The given columns</param>
        /// <returns>The new array column</returns>
        public static Column Array(params Column[] columns)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("array", columns.Select(x => x.ColumnProxy)));
        }

        /// <summary>
        /// Returns the first column that is not null, or null if all inputs are null.
        /// </summary>
        /// <param name="columns">The given columns</param>
        /// <returns>The first column that is not null</returns>
        public static Column Coalesce(params Column[] columns)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("coalesce", columns.Select(x => x.ColumnProxy)));
        }

        /// <summary>
        ///  Returns the number of distinct items in a group.
        /// </summary>
        /// <param name="columns">The given columns</param>
        /// <returns>The new column represents the number of distinct items</returns>
        public static Column CountDistinct(params Column[] columns)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("countDistinct", columns.Select(x => x.ColumnProxy)));
        }

        /// <summary>
        /// Creates a new struct column.
        /// </summary>
        /// <param name="columns">The given columns</param>
        /// <returns>The new struct column</returns>
        public static Column Struct(params Column[] columns)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("struct", columns.Select(x => x.ColumnProxy)));
        }

        /// <summary>
        /// Returns the approximate number of distinct items in a group
        /// </summary>
        /// <param name="column">The given columns</param>
        /// <returns>The column represents the approximate number of distinct items</returns>
        public static Column ApproxCountDistinct(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("approxCountDistinct", column));
        }

        /// <summary>
        /// Creates a new row for each element in the given array or map column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The new column for each element in the given array or map column</returns>
        public static Column Explode(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("explode", column));
        }

        /// <summary>
        /// Generate a random column with i.i.d. samples from U[0.0, 1.0].
        /// </summary>
        /// <param name="seed">The long integer as seed</param>
        /// <returns>A random column with i.i.d. samples from U[0.0, 1.0]. </returns>
        public static Column Rand(long seed)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("rand", seed));
        }

        /// <summary>
        /// Generate a column with i.i.d. samples from the standard normal distribution. 
        /// </summary>
        /// <param name="seed">The long integer as seed</param>
        /// <returns>A column with i.i.d. samples from the standard normal distribution</returns>
        public static Column Randn(long seed)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("randn", seed));
        }

        /// <summary>
        /// Returns the ntile group id (from 1 to n inclusive) in an ordered window partition.
        /// This is equivalent to the NTILE function in SQL. 
        /// </summary>
        /// <param name="n">The given number</param>
        /// <returns>The ntile group id</returns>
        public static Column Ntile(int n)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("ntile", n));
        }
        #endregion

        #region unary math functions
        /// <summary>
        /// Computes the cosine inverse of the given column; the returned angle is in the range 0.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the cosine inverse</returns>
        public static Column Acos(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("acos", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the sine inverse of the given column; the returned angle is in the range -pi/2 through pi/2.
        /// </summary>
        /// <param name="column"></param>
        /// <returns>The column represents the sine inverse</returns>
        public static Column Asin(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("asin", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the tangent inverse of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the tangent inverse</returns>
        public static Column Atan(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("atan", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the cube-root of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the cube-root</returns>
        public static Column Cbrt(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("cbrt", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the ceiling of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the ceiling</returns>
        public static Column Ceil(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("ceil", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the cosine of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the cosine</returns>
        public static Column Cos(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("cos", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the hyperbolic cosine of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the hyperbolic cosine</returns>
        public static Column Cosh(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("cosh", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the exponential of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the exponential</returns>
        public static Column Exp(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("exp", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the exponential of the given value minus column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the exponential</returns>
        public static Column Expm1(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("expm1", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the floor of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the floor</returns>
        public static Column Floor(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("floor", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the natural logarithm of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the natural logarithm</returns>
        public static Column Log(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("log", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the logarithm of the given column in base 10.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the logarithm</returns>
        public static Column Log10(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("log10", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the natural logarithm of the given column plus one.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the logarithm</returns>
        public static Column Log1p(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("log1p", column.ColumnProxy));
        }

        /// <summary>
        /// Returns the double value that is closest in value to the argument and is equal to a mathematical integer.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the double value</returns>
        public static Column Rint(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("rint", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the signum of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the signum</returns>
        public static Column Signum(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("signum", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the sine of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the sine</returns>
        public static Column Sin(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("sin", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the hyperbolic sine of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the hyperbolic sine</returns>
        public static Column Sinh(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("sinh", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the tangent of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the tangent</returns>
        public static Column Tan(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("tan", column.ColumnProxy));
        }

        /// <summary>
        /// Computes the hyperbolic tangent of the given column.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the hyperbolic tangent</returns>
        public static Column Tanh(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("tanh", column.ColumnProxy));
        }

        /// <summary>
        /// Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the degrees</returns>
        public static Column ToDegrees(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("toDegrees", column.ColumnProxy));
        }

        /// <summary>
        /// Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column represents the radians</returns>
        public static Column ToRadians(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("toRadians", column.ColumnProxy));
        }

        /// <summary>
        /// Computes bitwise NOT.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <returns>The column of bitwise NOT result</returns>
        public static Column BitwiseNOT(Column column)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateFunction("bitwiseNOT", column.ColumnProxy));
        }
        #endregion

        #region binary math functions
        /// <summary>
        /// Returns the angle theta from the conversion of rectangular coordinates (x, y) to polar coordinates (r, theta).
        /// </summary>
        /// <param name="leftColumn">The left column</param>
        /// <param name="rightColumn">The right column</param>
        /// <returns>The column of the result</returns>
        public static Column Atan2(Column leftColumn, Column rightColumn)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("atan2", leftColumn.ColumnProxy, rightColumn.ColumnProxy));
        }

        /// <summary>
        /// Computes sqrt(a2 + b2) without intermediate overflow or underflow.
        /// </summary>
        /// <param name="leftColumn">The left column</param>
        /// <param name="rightColumn">The right column</param>
        /// <returns>The column of the result</returns>
        public static Column Hypot(Column leftColumn, Column rightColumn)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("hypot", leftColumn.ColumnProxy, rightColumn.ColumnProxy));
        }

        /// <summary>
        /// Computes sqrt(a2 + b2) without intermediate overflow or underflow.
        /// </summary>
        /// <param name="leftColumn">The left column</param>
        /// <param name="rightValue">The right column</param>
        /// <returns>The column of the result</returns>
        public static Column Hypot(Column leftColumn, double rightValue)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("hypot", leftColumn.ColumnProxy, rightValue));
        }

        /// <summary>
        /// Computes sqrt(a2 + b2) without intermediate overflow or underflow.
        /// </summary>
        /// <param name="leftValue">The left value</param>
        /// <param name="rightColumn">The right column</param>
        /// <returns>The column of the result</returns>
        public static Column Hypot(double leftValue, Column rightColumn)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("hypot", leftValue, rightColumn.ColumnProxy));
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="leftColumn">The left column</param>
        /// <param name="rightColumn">The right column</param>
        /// <returns>The column of the result</returns>
        public static Column Pow(Column leftColumn, Column rightColumn)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("pow", leftColumn.ColumnProxy, rightColumn.ColumnProxy));
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="leftColumn">The left column</param>
        /// <param name="rightValue">The right value</param>
        /// <returns>The column of the result</returns>
        public static Column Pow(Column leftColumn, double rightValue)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("pow", leftColumn.ColumnProxy, rightValue));
        }

        /// <summary>
        /// Returns the value of the first argument raised to the power of the second argument.
        /// </summary>
        /// <param name="leftValue">The left value</param>
        /// <param name="rightColumn">The right column</param>
        /// <returns>The column of the result</returns>
        public static Column Pow(double leftValue, Column rightColumn)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("pow", leftValue, rightColumn.ColumnProxy));
        }

        /// <summary>
        /// Returns the approximate number of distinct items in a group.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <param name="rsd">The rsd</param>
        /// <returns>The column of the result</returns>
        public static Column ApproxCountDistinct(Column column, double rsd)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("approxCountDistinct", column, rsd));
        }

        /// <summary>
        /// Evaluates a list of conditions and returns one of multiple possible result expressions. 
        /// </summary>
        /// <param name="condition">The given column of condition</param>
        /// <param name="value">The value of condition</param>
        /// <returns>The column of the result</returns>
        public static Column When(Column condition, object value)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("when", condition, value));
        }

        /// <summary>
        /// Returns the value that is offset rows before the current row, and null if there is less than offset rows before the current row.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <param name="offset">The offset of the given column</param>
        /// <returns>The column of the result</returns>
        public static Column Lag(Column column, int offset)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("lag", column, offset));
        }

        /// <summary>
        /// Returns the value that is offset rows after the current row, and null if there is less than offset rows after the current row.
        /// </summary>
        /// <param name="column">The given column</param>
        /// <param name="offset">The offset of the given column</param>
        /// <returns>The column of the result</returns>
        public static Column Lead(Column column, int offset)
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateBinaryMathFunction("lead", column, offset));
        }
        #endregion

        #region window functions
        /// <summary>
        /// Returns a sequential number starting at 1 within a window partition.
        /// </summary>
        /// <returns>The column of the result</returns>
        public static Column RowNumber()
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateWindowFunction("row_number"));
        }

        /// <summary>
        /// Returns the rank of rows within a window partition, without any gaps.
        /// </summary>
        /// <returns>The column of the result</returns>
        public static Column DenseRank()
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateWindowFunction("dense_rank"));
        }

        /// <summary>
        ///  Returns the rank of rows within a window partition.
        /// </summary>
        /// <returns>The column of the result</returns>
        public static Column Rank()
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateWindowFunction("rank"));
        }

        /// <summary>
        /// Returns the cumulative distribution of values within a window partition
        /// </summary>
        /// <returns>The column of the result</returns>
        public static Column CumeDist()
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateWindowFunction("cume_dist"));
        }

        /// <summary>
        /// Returns the relative rank (i.e. percentile) of rows within a window partition.
        /// </summary>
        /// <returns>The column of the result</returns>
        public static Column PercentRank()
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateWindowFunction("percent_rank"));
        }

        /// <summary>
        /// A column expression that generates monotonically increasing 64-bit integers.
        /// </summary>
        /// <returns>The column of the result</returns>
        public static Column MonotonicallyIncreasingId()
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateWindowFunction("monotonically_increasing_id"));
        }

        /// <summary>
        /// Partition ID of the Spark task.
        /// Note that this is indeterministic because it depends on data partitioning and task scheduling.
        /// </summary>
        /// <returns>The column of the result</returns>
        public static Column SparkPartitionId()
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateWindowFunction("spark_partition_id"));
        }

        /// <summary>
        /// Generate a random column with i.i.d. samples from U[0.0, 1.0]. 
        /// </summary>
        /// <returns>The column of the result</returns>
        public static Column Rand()
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateWindowFunction("rand"));
        }

        /// <summary>
        /// Generate a column with i.i.d. samples from the standard normal distribution. 
        /// </summary>
        /// <returns>The column of the result</returns>
        public static Column Randn()
        {
            return new Column(SparkCLREnvironment.SparkCLRProxy.SparkContextProxy.CreateWindowFunction("randn"));
        }
        #endregion

        #region udf
        /// <summary>
        /// Defines a user-defined function of 0 arguments as user-defined function (UDF).
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column> Udf<RT>(Func<RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT>(f).Execute).Execute0;
        }

        /// <summary>
        /// Defines a user-defined function of 1 arguments as user-defined function (UDF). 
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <typeparam name="A1">The 1st arguement of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column, Column> Udf<RT, A1>(Func<A1, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1>(f).Execute).Execute1;
        }

        /// <summary>
        /// Defines a user-defined function of 2 arguments as user-defined function (UDF).
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <typeparam name="A1">The 1st arguement of the given function</typeparam>
        /// <typeparam name="A2">The 2nd arguement of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column, Column, Column> Udf<RT, A1, A2>(Func<A1, A2, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2>(f).Execute).Execute2;
        }

        /// <summary>
        /// Defines a user-defined function of 3 arguments as user-defined function (UDF).
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <typeparam name="A1">The 1st arguement of the given function</typeparam>
        /// <typeparam name="A2">The 2nd arguement of the given function</typeparam>
        /// <typeparam name="A3">The 3rd arguement of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column, Column, Column, Column> Udf<RT, A1, A2, A3>(Func<A1, A2, A3, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3>(f).Execute).Execute3;
        }

        /// <summary>
        /// Defines a user-defined function of 4 arguments as user-defined function (UDF).
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <typeparam name="A1">The 1st arguement of the given function</typeparam>
        /// <typeparam name="A2">The 2nd arguement of the given function</typeparam>
        /// <typeparam name="A3">The 3rd arguement of the given function</typeparam>
        /// <typeparam name="A4">The 4th arguement of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column, Column, Column, Column, Column> Udf<RT, A1, A2, A3, A4>(Func<A1, A2, A3, A4, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4>(f).Execute).Execute4;
        }

        /// <summary>
        /// Defines a user-defined function of 5 arguments as user-defined function (UDF).
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <typeparam name="A1">The 1st arguement of the given function</typeparam>
        /// <typeparam name="A2">The 2nd arguement of the given function</typeparam>
        /// <typeparam name="A3">The 3rd arguement of the given function</typeparam>
        /// <typeparam name="A4">The 4th arguement of the given function</typeparam>
        /// <typeparam name="A5">The 5th arguement of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column, Column, Column, Column, Column, Column> Udf<RT, A1, A2, A3, A4, A5>(Func<A1, A2, A3, A4, A5, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5>(f).Execute).Execute5;
        }

        /// <summary>
        /// Defines a user-defined function of 6 arguments as user-defined function (UDF).
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <typeparam name="A1">The 1st arguement of the given function</typeparam>
        /// <typeparam name="A2">The 2nd arguement of the given function</typeparam>
        /// <typeparam name="A3">The 3rd arguement of the given function</typeparam>
        /// <typeparam name="A4">The 4th arguement of the given function</typeparam>
        /// <typeparam name="A5">The 5th arguement of the given function</typeparam>
        /// <typeparam name="A6">The 6th arguement of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column> Udf<RT, A1, A2, A3, A4, A5, A6>(Func<A1, A2, A3, A4, A5, A6, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5, A6>(f).Execute).Execute6;
        }

        /// <summary>
        /// Defines a user-defined function of 7 arguments as user-defined function (UDF).
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <typeparam name="A1">The 1st arguement of the given function</typeparam>
        /// <typeparam name="A2">The 2nd arguement of the given function</typeparam>
        /// <typeparam name="A3">The 3rd arguement of the given function</typeparam>
        /// <typeparam name="A4">The 4th arguement of the given function</typeparam>
        /// <typeparam name="A5">The 5th arguement of the given function</typeparam>
        /// <typeparam name="A6">The 6th arguement of the given function</typeparam>
        /// <typeparam name="A7">The 7th arguement of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column> Udf<RT, A1, A2, A3, A4, A5, A6, A7>(Func<A1, A2, A3, A4, A5, A6, A7, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7>(f).Execute).Execute7;
        }

        /// <summary>
        /// Defines a user-defined function of 8 arguments as user-defined function (UDF).
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <typeparam name="A1">The 1st arguement of the given function</typeparam>
        /// <typeparam name="A2">The 2nd arguement of the given function</typeparam>
        /// <typeparam name="A3">The 3rd arguement of the given function</typeparam>
        /// <typeparam name="A4">The 4th arguement of the given function</typeparam>
        /// <typeparam name="A5">The 5th arguement of the given function</typeparam>
        /// <typeparam name="A6">The 6th arguement of the given function</typeparam>
        /// <typeparam name="A7">The 7th arguement of the given function</typeparam>
        /// <typeparam name="A8">The 8th arguement of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column> Udf<RT, A1, A2, A3, A4, A5, A6, A7, A8>(Func<A1, A2, A3, A4, A5, A6, A7, A8, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8>(f).Execute).Execute8;
        }

        /// <summary>
        /// Defines a user-defined function of 9 arguments as user-defined function (UDF).
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <typeparam name="A1">The 1st arguement of the given function</typeparam>
        /// <typeparam name="A2">The 2nd arguement of the given function</typeparam>
        /// <typeparam name="A3">The 3rd arguement of the given function</typeparam>
        /// <typeparam name="A4">The 4th arguement of the given function</typeparam>
        /// <typeparam name="A5">The 5th arguement of the given function</typeparam>
        /// <typeparam name="A6">The 6th arguement of the given function</typeparam>
        /// <typeparam name="A7">The 7th arguement of the given function</typeparam>
        /// <typeparam name="A8">The 8th arguement of the given function</typeparam>
        /// <typeparam name="A9">The 9th arguement of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> Udf<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9>(Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9>(f).Execute).Execute9;
        }

        /// <summary>
        /// Defines a user-defined function of 10 arguments as user-defined function (UDF).
        /// The data types are automatically inferred based on the function's signature. 
        /// </summary>
        /// <param name="f">The given function</param>
        /// <typeparam name="RT">The return type of the given function</typeparam>
        /// <typeparam name="A1">The 1st arguement of the given function</typeparam>
        /// <typeparam name="A2">The 2nd arguement of the given function</typeparam>
        /// <typeparam name="A3">The 3rd arguement of the given function</typeparam>
        /// <typeparam name="A4">The 4th arguement of the given function</typeparam>
        /// <typeparam name="A5">The 5th arguement of the given function</typeparam>
        /// <typeparam name="A6">The 6th arguement of the given function</typeparam>
        /// <typeparam name="A7">The 7th arguement of the given function</typeparam>
        /// <typeparam name="A8">The 8th arguement of the given function</typeparam>
        /// <typeparam name="A9">The 9th arguement of the given function</typeparam>
        /// <typeparam name="A10">The 10th arguement of the given function</typeparam>
        /// <returns>The new user-defined function</returns>
        public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> Udf<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10>(Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10>(f).Execute).Execute10;
        }
        #endregion

        private static readonly Dictionary<Type, string> returnTypes = new Dictionary<Type, string>
        {
            {typeof(string), "string"},
            {typeof(byte[]), "binary"},
            {typeof(bool), "boolean"},
            {typeof(DateTime), "timestamp"},
            {typeof(decimal), "decimal(28,12)"},
            {typeof(double), "double"},
            {typeof(float), "float"},
            {typeof(byte), "tinyint"},
            {typeof(int), "int"},
            {typeof(long), "bigint"},
            {typeof(short), "smallint"}
        };

        internal static string GetReturnType(Type type)
        {
            Type[] types = new Type[] { type };
            string returnTypeFormat = "{0}";

            if (type.IsArray)
            {
                types[0] = type.GetElementType();
                returnTypeFormat = "array<{0}>";
            }
            else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
            {
                types = type.GenericTypeArguments;
                returnTypeFormat = "map<{0},{1}>";
            }

            if (types.Any(t => !returnTypes.ContainsKey(t)))
            {
                throw new ArgumentException(string.Format("{0} not supported. Supported types: {1}", type.Name, string.Join(",", returnTypes.Keys)));
            }

            return string.Format(returnTypeFormat, types.Select(t => returnTypes[t]).ToArray());
        }
    }

    #region udf helpers
    /// <summary>
    /// only used in SqlContext.RegisterFunction for now
    /// </summary>
    /// <typeparam name="RT"></typeparam>
    [Serializable]
    internal class UdfHelper<RT>
    {
        private readonly Func<RT> func;

        internal UdfHelper(Func<RT> f)
        {
            func = f;
        }
        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func()).Cast<dynamic>();
        }
    }

    [Serializable]
    internal class UdfHelper<RT, A1>
    {
        private readonly Func<A1, RT> func;

        internal UdfHelper(Func<A1, RT> f)
        {
            func = f;
        }
        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]))).Cast<dynamic>();
        }
    }

    [Serializable]
    internal class UdfHelper<RT, A1, A2>
    {
        private readonly Func<A1, A2, RT> func;

        internal UdfHelper(Func<A1, A2, RT> f)
        {
            func = f;
        }
        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]), (A2)(a[1]))).Cast<dynamic>();
        }
    }

    [Serializable]
    internal class UdfHelper<RT, A1, A2, A3>
    {
        private readonly Func<A1, A2, A3, RT> func;

        internal UdfHelper(Func<A1, A2, A3, RT> f)
        {
            func = f;
        }
        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]), (A2)(a[1]), (A3)(a[2]))).Cast<dynamic>();
        }
    }

    [Serializable]
    internal class UdfHelper<RT, A1, A2, A3, A4>
    {
        private readonly Func<A1, A2, A3, A4, RT> func;

        internal UdfHelper(Func<A1, A2, A3, A4, RT> f)
        {
            func = f;
        }
        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]), (A2)(a[1]), (A3)(a[2]), (A4)(a[3]))).Cast<dynamic>();
        }
    }

    [Serializable]
    internal class UdfHelper<RT, A1, A2, A3, A4, A5>
    {
        private readonly Func<A1, A2, A3, A4, A5, RT> func;

        internal UdfHelper(Func<A1, A2, A3, A4, A5, RT> f)
        {
            func = f;
        }
        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]), (A2)(a[1]), (A3)(a[2]), (A4)(a[3]), (A5)(a[4]))).Cast<dynamic>();
        }
    }

    [Serializable]
    internal class UdfHelper<RT, A1, A2, A3, A4, A5, A6>
    {
        private readonly Func<A1, A2, A3, A4, A5, A6, RT> func;

        internal UdfHelper(Func<A1, A2, A3, A4, A5, A6, RT> f)
        {
            func = f;
        }
        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]), (A2)(a[1]), (A3)(a[2]), (A4)(a[3]), (A5)(a[4]), (A6)(a[5]))).Cast<dynamic>();
        }
    }

    [Serializable]
    internal class UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7>
    {
        private readonly Func<A1, A2, A3, A4, A5, A6, A7, RT> func;

        internal UdfHelper(Func<A1, A2, A3, A4, A5, A6, A7, RT> f)
        {
            func = f;
        }
        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]), (A2)(a[1]), (A3)(a[2]), (A4)(a[3]), (A5)(a[4]), (A6)(a[5]), (A7)(a[6]))).Cast<dynamic>();
        }
    }

    [Serializable]
    internal class UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8>
    {
        private readonly Func<A1, A2, A3, A4, A5, A6, A7, A8, RT> func;

        internal UdfHelper(Func<A1, A2, A3, A4, A5, A6, A7, A8, RT> f)
        {
            func = f;
        }
        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]), (A2)(a[1]), (A3)(a[2]), (A4)(a[3]), (A5)(a[4]), (A6)(a[5]), (A7)(a[6]), (A8)(a[7]))).Cast<dynamic>();
        }
    }

    [Serializable]
    internal class UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9>
    {
        private readonly Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT> func;

        internal UdfHelper(Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT> f)
        {
            func = f;
        }
        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]), (A2)(a[1]), (A3)(a[2]), (A4)(a[3]), (A5)(a[4]), (A6)(a[5]), (A7)(a[6]), (A8)(a[7]), (A9)(a[8]))).Cast<dynamic>();
        }
    }

    [Serializable]
    internal class UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10>
    {
        private readonly Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT> func;

        internal UdfHelper(Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT> f)
        {
            func = f;
        }

        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]), (A2)(a[1]), (A3)(a[2]), (A4)(a[3]), (A5)(a[4]), (A6)(a[5]), (A7)(a[6]), (A8)(a[7]), (A9)(a[8]), (A10)(a[9]))).Cast<dynamic>();
        }
    }
    #endregion
}
