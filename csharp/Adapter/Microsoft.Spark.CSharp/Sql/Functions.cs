// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// not applicable yet - it is for UDF to be used in DataFrame
    /// </summary>
    public class Functions
    {
        #region functions
        public static Column Lit(object column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("lit", column));
        }
        public static Column Col(string colName)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("col", colName));
        }
        public static Column Column(string colName)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("column", colName));
        }
        public static Column Asc(string columnName)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("asc", columnName));
        }
        public static Column Desc(string columnName)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("desc", columnName));
        }
        public static Column Upper(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("upper", column.ColumnProxy));
        }
        public static Column Lower(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("lower", column.ColumnProxy));
        }
        public static Column Sqrt(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("sqrt", column.ColumnProxy));
        }
        public static Column Abs(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("abs", column.ColumnProxy));
        }
        public static Column Max(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("max", column.ColumnProxy));
        }
        public static Column Min(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("min", column.ColumnProxy));
        }
        public static Column First(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("first", column.ColumnProxy));
        }
        public static Column Last(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("last", column.ColumnProxy));
        }
        public static Column Count(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("count", column.ColumnProxy));
        }
        public static Column Sum(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("sum", column.ColumnProxy));
        }
        public static Column Avg(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("avg", column.ColumnProxy));
        }
        public static Column Mean(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("mean", column.ColumnProxy));
        }
        public static Column SumDistinct(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("sumDistinct", column.ColumnProxy));
        }
        public static Column Array(params Column[] columns)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("array", columns.Select(x => x.ColumnProxy)));
        }
        public static Column Coalesce(params Column[] columns)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("coalesce", columns.Select(x => x.ColumnProxy)));
        }
        public static Column CountDistinct(params Column[] columns)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("countDistinct", columns.Select(x => x.ColumnProxy)));
        }
        public static Column Struct(params Column[] columns)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("struct", columns.Select(x => x.ColumnProxy)));
        }
        public static Column ApproxCountDistinct(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("approxCountDistinct", column));
        }
        public static Column Explode(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("explode", column));
        }
        public static Column Rand(long seed)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("rand", seed));
        }
        public static Column Randn(long seed)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("randn", seed));
        }
        public static Column Ntile(int n)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("ntile", n));
        }
        #endregion

        #region unary math functions
        public static Column Acos(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("acos", column.ColumnProxy));
        }
        public static Column Asin(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("asin", column.ColumnProxy));
        }
        public static Column Atan(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("atan", column.ColumnProxy));
        }
        public static Column Cbrt(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("cbrt", column.ColumnProxy));
        }
        public static Column Ceil(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("ceil", column.ColumnProxy));
        }
        public static Column Cos(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("cos", column.ColumnProxy));
        }
        public static Column Cosh(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("cosh", column.ColumnProxy));
        }
        /// <summary>
        /// Computes the exponential of the given value.
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
        public static Column Exp(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("exp", column.ColumnProxy));
        }
        /// <summary>
        /// Computes the exponential of the given value minus one.
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
        public static Column Expm1(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("expm1", column.ColumnProxy));
        }
        public static Column Floor(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("floor", column.ColumnProxy));
        }
        public static Column Log(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("log", column.ColumnProxy));
        }
        public static Column Log10(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("log10", column.ColumnProxy));
        }
        public static Column Log1p(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("log1p", column.ColumnProxy));
        }
        public static Column Rint(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("rint", column.ColumnProxy));
        }
        public static Column Signum(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("signum", column.ColumnProxy));
        }
        public static Column Sin(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("sin", column.ColumnProxy));
        }
        public static Column Sinh(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("sinh", column.ColumnProxy));
        }
        public static Column Tan(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("tan", column.ColumnProxy));
        }
        public static Column Tanh(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("tanh", column.ColumnProxy));
        }
        public static Column ToDegrees(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("toDegrees", column.ColumnProxy));
        }
        public static Column ToRadians(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("toRadians", column.ColumnProxy));
        }
        public static Column BitwiseNOT(Column column)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateFunction("bitwiseNOT", column.ColumnProxy));
        }
        #endregion

        #region binary math functions
        public static Column Atan2(Column leftColumn, Column rightColumn)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("atan2", leftColumn.ColumnProxy, rightColumn.ColumnProxy));
        }
        public static Column Hypot(Column leftColumn, Column rightColumn)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("hypot", leftColumn.ColumnProxy, rightColumn.ColumnProxy));
        }
        public static Column Hypot(Column leftColumn, double rightValue)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("hypot", leftColumn.ColumnProxy, rightValue));
        }
        public static Column Hypot(double leftValue, Column rightColumn)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("hypot", leftValue, rightColumn.ColumnProxy));
        }
        public static Column Pow(Column leftColumn, Column rightColumn)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("pow", leftColumn.ColumnProxy, rightColumn.ColumnProxy));
        }
        public static Column Pow(Column leftColumn, double rightValue)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("pow", leftColumn.ColumnProxy, rightValue));
        }
        public static Column Pow(double leftValue, Column rightColumn)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("pow", leftValue, rightColumn.ColumnProxy));
        }
        public static Column ApproxCountDistinct(Column column, double rsd)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("approxCountDistinct", column, rsd));
        }
        public static Column When(Column condition, object value)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("when", condition, value));
        }
        public static Column Lag(Column column, int offset)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("lag", column, offset));
        }
        public static Column Lead(Column column, int offset)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateBinaryMathFunction("lead", column, offset));
        }
        #endregion

        #region window functions
        public static Column rowNumber()
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateWindowFunction("rowNumber"));
        }
        public static Column DenseRank()
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateWindowFunction("denseRank"));
        }
        public static Column Rank()
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateWindowFunction("rank"));
        }
        public static Column CumeDist()
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateWindowFunction("cumeDist"));
        }
        public static Column PercentRank()
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateWindowFunction("percentRank"));
        }
        public static Column MonotonicallyIncreasingId()
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateWindowFunction("monotonicallyIncreasingId"));
        }
        public static Column SparkPartitionId()
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateWindowFunction("sparkPartitionId"));
        }
        public static Column Rand()
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateWindowFunction("rand"));
        }
        public static Column Randn()
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateWindowFunction("randn"));
        }
        #endregion

        #region udf
        public static UserDefinedFunction<RT> Udf<RT>(Func<RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT>(f).Execute);
        }
        public static UserDefinedFunction<RT> Udf<RT, A1>(Func<A1, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1>(f).Execute);
        }
        public static UserDefinedFunction<RT> Udf<RT, A1, A2>(Func<A1, A2, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2>(f).Execute);
        }
        public static UserDefinedFunction<RT> Udf<RT, A1, A2, A3>(Func<A1, A2, A3, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3>(f).Execute);
        }
        public static UserDefinedFunction<RT> Udf<RT, A1, A2, A3, A4>(Func<A1, A2, A3, A4, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4>(f).Execute);
        }
        public static UserDefinedFunction<RT> Udf<RT, A1, A2, A3, A4, A5>(Func<A1, A2, A3, A4, A5, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5>(f).Execute);
        }
        public static UserDefinedFunction<RT> Udf<RT, A1, A2, A3, A4, A5, A6>(Func<A1, A2, A3, A4, A5, A6, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5, A6>(f).Execute);
        }
        public static UserDefinedFunction<RT> Udf<RT, A1, A2, A3, A4, A5, A6, A7>(Func<A1, A2, A3, A4, A5, A6, A7, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7>(f).Execute);
        }
        public static UserDefinedFunction<RT> Udf<RT, A1, A2, A3, A4, A5, A6, A7, A8>(Func<A1, A2, A3, A4, A5, A6, A7, A8, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8>(f).Execute);
        }
        public static UserDefinedFunction<RT> Udf<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9>(Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9>(f).Execute);
        }
        public static UserDefinedFunction<RT> Udf<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10>(Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT> f)
        {
            return new UserDefinedFunction<RT>(new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10>(f).Execute);
        }
        #endregion
    }

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
            this.func = f;
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
            this.func = f;
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
            this.func = f;
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
            this.func = f;
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
            this.func = f;
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
            this.func = f;
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
            this.func = f;
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
            this.func = f;
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
            this.func = f;
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
            this.func = f;
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
            this.func = f;
        }

        internal IEnumerable<dynamic> Execute(int pid, IEnumerable<dynamic> input)
        {
            return input.Select(a => func((A1)(a[0]), (A2)(a[1]), (A3)(a[2]), (A4)(a[3]), (A5)(a[4]), (A6)(a[5]), (A7)(a[6]), (A8)(a[7]), (A9)(a[8]), (A10)(a[9]))).Cast<dynamic>();
        }
    }

    public class UserDefinedFunction<RT>
    {
        private Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> func;
        private string name;

        internal UserDefinedFunction(Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> f, string name = null)
        {
            this.func = f;
            this.name = name;
        }

        private void CreateJavaUdf()
        {
            CSharpSparkEnvironment.SparkContextProxy.CreateUserDefinedCSharpFunction(func.GetType().Name, SparkContext.BuildCommand(func, SerializedMode.Row, SerializedMode.Row), "StringType");
        }
    }
}
