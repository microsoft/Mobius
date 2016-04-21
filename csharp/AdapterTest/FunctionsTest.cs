using NUnit.Framework;
using Microsoft.Spark.CSharp.Proxy;
using Moq;
using Microsoft.Spark.CSharp.Sql;
using Microsoft.Spark.CSharp.Interop;
using AdapterTest.Mocks;

namespace AdapterTest
{
    [TestFixture]
    public class FunctionsTest
    {
        private Mock<ISparkContextProxy> mockSparkContextProxy;

        [SetUp]
        public void Initialize()
        {
            Mock<ISparkCLRProxy> mockSparkCLRProxy = new Mock<ISparkCLRProxy>();
            mockSparkContextProxy = new Mock<ISparkContextProxy>();
            mockSparkCLRProxy.Setup(m => m.SparkContextProxy).Returns(mockSparkContextProxy.Object);
            SparkCLREnvironment.SparkCLRProxy = mockSparkCLRProxy.Object;
        }

        #region functions

        [TearDown]
        public void TestCleanUp()
        {
            // Revert to use Static mock class to prevent blocking other test methods which uses static mock class
            SparkCLREnvironment.SparkCLRProxy = new MockSparkCLRProxy();
        }

        [Test]
        public void TestFunctionLit()
        { 
            mockSparkContextProxy.Setup(m=>m.CreateFunction(It.IsAny<string>(), It.IsAny<object>()));
            Column column = GeneratorColum();
            Functions.Lit(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("lit", column), Times.Once);
        }

        [Test]
        public void TestFunctionCol()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<string>()));
            Functions.Col("AnyString");
            mockSparkContextProxy.Verify(m => m.CreateFunction("col", "AnyString"), Times.Once);
        }

        [Test]
        public void TestFunctionColumn()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<string>()));
            Functions.Column("AnyString");
            mockSparkContextProxy.Verify(m => m.CreateFunction("column", "AnyString"), Times.Once);
        }

        [Test]
        public void TestFunctionAsc()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<string>()));
            Functions.Asc("AnyString");
            mockSparkContextProxy.Verify(m => m.CreateFunction("asc", "AnyString"), Times.Once);
        }

        [Test]
        public void TestFunctionDesc()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<string>()));
            Functions.Desc("AnyString");
            mockSparkContextProxy.Verify(m => m.CreateFunction("desc", "AnyString"), Times.Once);
        }

        [Test]
        public void TestFunctionUpper()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Upper(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("upper", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionLower()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Lower(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("lower", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionSqrt()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Sqrt(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("sqrt", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionAbs()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Abs(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("abs", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionMax()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Max(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("max", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionMin()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Min(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("min", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionFirst()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.First(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("first", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionLast()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Last(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("last", column.ColumnProxy), Times.Once);
        }


        [Test]
        public void TestFunctionCount()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Count(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("count", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionSum()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Sum(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("sum", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionMean()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Mean(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("mean", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionAvg()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Avg(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("avg", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionSumDistinct()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.SumDistinct(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("sumDistinct", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionArray()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy[]>()));
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Functions.Array(column1, column2);
            mockSparkContextProxy.Verify(m => m.CreateFunction("array", new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy }), Times.Once);
        }

        [Test]
        public void TestFunctionCoalesce()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy[]>()));
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Functions.Coalesce(column1, column2);
            mockSparkContextProxy.Verify(m => m.CreateFunction("coalesce", new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy }), Times.Once);
        }

        [Test]
        public void TestFunctionCountDistinct()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy[]>()));
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Functions.CountDistinct(column1, column2);
            mockSparkContextProxy.Verify(m => m.CreateFunction("countDistinct", new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy }), Times.Once);
        }

        [Test]
        public void TestFunctionCountStruct()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy[]>()));
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Functions.Struct(column1, column2);
            mockSparkContextProxy.Verify(m => m.CreateFunction("struct", new IColumnProxy[] { column1.ColumnProxy, column2.ColumnProxy }), Times.Once);
        }

        [Test]
        public void TestFunctionApproxCountDistinct()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<Column>()));
            Column column = GeneratorColum();
            Functions.ApproxCountDistinct(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("approxCountDistinct", column), Times.Once);

            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<Column>(), It.IsAny<double>()));
            Functions.ApproxCountDistinct(column, (double)1.0);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("approxCountDistinct", column, (double)1.0), Times.Once);
        }

        [Test]
        public void TestFunctionExplode()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<Column>()));
            Column column = GeneratorColum();
            Functions.Explode(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("explode", column), Times.Once);
        }

        [Test]
        public void TestFunctionRand()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<long>()));
            Functions.Rand((long)100);
            mockSparkContextProxy.Verify(m => m.CreateFunction("rand", (long)100), Times.Once);
        }

        [Test]
        public void TestFunctionRandn()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<long>()));
            Functions.Randn((long)100);
            mockSparkContextProxy.Verify(m => m.CreateFunction("randn", (long)100), Times.Once);
        }

        [Test]
        public void TestFunctionNtile()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<long>()));
            Functions.Ntile(100);
            mockSparkContextProxy.Verify(m => m.CreateFunction("ntile", 100), Times.Once);
        }


        #endregion

        #region unary math functions

        [Test]
        public void TestFunctionAcos()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Acos(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("acos", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionAsin()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Asin(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("asin", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionAtan()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Atan(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("atan", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionCbrt()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Cbrt(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("cbrt", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionCeil()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Ceil(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("ceil", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionCos()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Cos(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("cos", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionCosh()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Cosh(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("cosh", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionExp()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Exp(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("exp", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionExpm1()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Expm1(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("expm1", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionFloor()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Floor(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("floor", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionLog()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Log(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("log", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionLog10()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Log10(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("log10", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionLog1p()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Log1p(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("log1p", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionRint()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Rint(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("rint", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionSignum()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Signum(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("signum", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionSin()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Sin(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("sin", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionSinh()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Sinh(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("sinh", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionTan()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Tan(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("tan", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionTanh()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.Tanh(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("tanh", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionToDegrees()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.ToDegrees(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("toDegrees", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionToRadians()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.ToRadians(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("toRadians", column.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionBitwiseNOT()
        {
            mockSparkContextProxy.Setup(m => m.CreateFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>()));
            Column column = GeneratorColum();
            Functions.BitwiseNOT(column);
            mockSparkContextProxy.Verify(m => m.CreateFunction("bitwiseNOT", column.ColumnProxy), Times.Once);
        }
        
        #endregion

        #region binary math functions

        [Test]
        public void TestFunctionAtan2()
        {
            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>(), It.IsAny<IColumnProxy>()));
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Functions.Atan2(column1,column2);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("atan2", column1.ColumnProxy, column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionHypot()
        {
            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>(), It.IsAny<IColumnProxy>()));
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Functions.Hypot(column1, column2);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("hypot", column1.ColumnProxy, column2.ColumnProxy), Times.Once);

            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>(), It.IsAny<double>()));
            Functions.Hypot(column1, (double)1.0);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("hypot", column1.ColumnProxy, (double)1.0), Times.Once);

            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<double>(), It.IsAny<IColumnProxy>()));
            Functions.Hypot((double)1.0,column2);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("hypot", (double)1.0, column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionPow()
        {
            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>(), It.IsAny<IColumnProxy>()));
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Functions.Pow(column1, column2);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("pow", column1.ColumnProxy, column2.ColumnProxy), Times.Once);

            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>(), It.IsAny<double>()));
            Functions.Pow(column1, (double)1.0);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("pow", column1.ColumnProxy, (double)1.0), Times.Once);

            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<double>(), It.IsAny<IColumnProxy>()));
            Functions.Pow((double)1.0, column2);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("pow", (double)1.0, column2.ColumnProxy), Times.Once);
        }

        [Test]
        public void TestFunctionWhen()
        {
            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<IColumnProxy>(), It.IsAny<object>()));
            Column column1 = GeneratorColum();
            Column column2 = GeneratorColum();
            Functions.When(column1, column2);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("when", column1, column2), Times.Once);
        }

        [Test]
        public void TestFunctionLag()
        {
            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<Column>(), It.IsAny<int>()));
            Column column1 = GeneratorColum();
            Functions.Lag(column1, 1);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("lag", column1, 1), Times.Once);
        }

        [Test]
        public void TestFunctionLead()
        {
            mockSparkContextProxy.Setup(m => m.CreateBinaryMathFunction(It.IsAny<string>(), It.IsAny<Column>(), It.IsAny<int>()));
            Column column1 = GeneratorColum();
            Functions.Lead(column1, 1);
            mockSparkContextProxy.Verify(m => m.CreateBinaryMathFunction("lead", column1, 1), Times.Once);
        }

        #endregion

        #region window functions

        [Test]
        public void TestwindowFunction()
        {
            mockSparkContextProxy.Setup(m => m.CreateWindowFunction(It.IsAny<string>()));
            Functions.RowNumber();
            mockSparkContextProxy.Verify(m => m.CreateWindowFunction("row_number"), Times.Once);

            Functions.DenseRank();
            mockSparkContextProxy.Verify(m => m.CreateWindowFunction("dense_rank"), Times.Once);

            Functions.Rank();
            mockSparkContextProxy.Verify(m => m.CreateWindowFunction("rank"), Times.Once);

            Functions.CumeDist();
            mockSparkContextProxy.Verify(m => m.CreateWindowFunction("cume_dist"), Times.Once);

            Functions.PercentRank();
            mockSparkContextProxy.Verify(m => m.CreateWindowFunction("percent_rank"), Times.Once);

            Functions.MonotonicallyIncreasingId();
            mockSparkContextProxy.Verify(m => m.CreateWindowFunction("monotonically_increasing_id"), Times.Once);

            Functions.SparkPartitionId();
            mockSparkContextProxy.Verify(m => m.CreateWindowFunction("spark_partition_id"), Times.Once);

            Functions.Rand();
            mockSparkContextProxy.Verify(m => m.CreateWindowFunction("rand"), Times.Once);

            Functions.Randn();
            mockSparkContextProxy.Verify(m => m.CreateWindowFunction("randn"), Times.Once);
        }

        #endregion

        #region udf functions

        [Test]
        public void TestUdfFunction()
        {
            var mockUdfProxy = new Mock<IUDFProxy>();
            mockUdfProxy.Setup(m => m.Apply(It.IsAny<IColumnProxy[]>()));
            mockSparkContextProxy.Setup(m => m.CreateUserDefinedCSharpFunction(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<string>())).Returns(mockUdfProxy.Object);

            Functions.Udf(() => 0).Invoke();
            mockUdfProxy.Verify(m => m.Apply(new IColumnProxy[] { }), Times.Once);

            var column1 = GeneratorColum();
            Functions.Udf<int, int>(i => 1).Invoke(column1);
            mockUdfProxy.Verify(m => m.Apply(new[] { column1.ColumnProxy }), Times.Once);

            var column2 = GeneratorColum();
            Functions.Udf<int, int, int>( (i1, i2) => 2).Invoke(column1, column2);
            mockUdfProxy.Verify(m => m.Apply(new[] { column1.ColumnProxy, column2.ColumnProxy }), Times.Once);

            var column3 = GeneratorColum();
            Functions.Udf<int, int, int, int>((i1, i2, i3) => 3).Invoke(column1, column2, column3);
            mockUdfProxy.Verify(m => m.Apply(new[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy }), Times.Once);

            var column4 = GeneratorColum();
            Functions.Udf<int, int, int, int, int>((i1, i2, i3, i4) => 4).Invoke(column1, column2, column3, column4);
            mockUdfProxy.Verify(m => m.Apply(new[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy }), Times.Once);

            var column5 = GeneratorColum();
            Functions.Udf<int, int, int, int, int, int>((i1, i2, i3, i4, i5) => 5).Invoke(column1, column2, column3, column4, column5);
            mockUdfProxy.Verify(m => m.Apply(new[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, column5.ColumnProxy }), Times.Once);

            var column6 = GeneratorColum();
            Functions.Udf<int, int, int, int, int, int, int>((i1, i2, i3, i4, i5, i6) => 6).Invoke(column1, column2, column3, column4, column5, column6);
            mockUdfProxy.Verify(m => m.Apply(new[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, column5.ColumnProxy, column6.ColumnProxy }), Times.Once);

            var column7 = GeneratorColum();
            Functions.Udf<int, int, int, int, int, int, int, int>((i1, i2, i3, i4, i5, i6, i7) => 7).Invoke(column1, column2, column3, column4, column5, column6, column7);
            mockUdfProxy.Verify(m => m.Apply(new[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, column5.ColumnProxy, column6.ColumnProxy, column7.ColumnProxy }), Times.Once);

            var column8 = GeneratorColum();
            Functions.Udf<int, int, int, int, int, int, int, int, int>((i1, i2, i3, i4, i5, i6, i7, i8) => 8).Invoke(column1, column2, column3, column4, column5, column6, column7, column8);
            mockUdfProxy.Verify(m => m.Apply(new[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, column5.ColumnProxy, column6.ColumnProxy, column7.ColumnProxy, column8.ColumnProxy }), Times.Once);

            var column9 = GeneratorColum();
            Functions.Udf<int, int, int, int, int, int, int, int, int, int>((i1, i2, i3, i4, i5, i6, i7, i8, i9) => 9).Invoke(column1, column2, column3, column4, column5, column6, column7, column8, column9);
            mockUdfProxy.Verify(m => m.Apply(new[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, column5.ColumnProxy, column6.ColumnProxy, column7.ColumnProxy, column8.ColumnProxy, column9.ColumnProxy }), Times.Once);

            var column10 = GeneratorColum();
            Functions.Udf<int, int, int, int, int, int, int, int, int, int, int>((i1, i2, i3, i4, i5, i6, i7, i8, i9, i10) => 10).Invoke(column1, column2, column3, column4, column5, column6, column7, column8, column9, column10);
            mockUdfProxy.Verify(m => m.Apply(new[] { column1.ColumnProxy, column2.ColumnProxy, column3.ColumnProxy, column4.ColumnProxy, column5.ColumnProxy, column6.ColumnProxy, column7.ColumnProxy, column8.ColumnProxy, column9.ColumnProxy, column10.ColumnProxy }), Times.Once);
        }
        #endregion

        private Column GeneratorColum()
        {
            Mock<IColumnProxy> mockColumnProxy = new Mock<IColumnProxy>();
            return new Column(mockColumnProxy.Object);
        }
    }
}

       