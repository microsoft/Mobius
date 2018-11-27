// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// Used for registering User Defined Functions. SparkSession.Udf is used to access instance of this type.
    /// </summary>
    public class UdfRegistration
    {
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(UdfRegistration));

        private IUdfRegistrationProxy udfRegistrationProxy;

        internal UdfRegistration(IUdfRegistrationProxy udfRegistrationProxy)
        {
            this.udfRegistrationProxy = udfRegistrationProxy;
        }

        //TODO - the following section is a copy of the same functionality in SQLContext..refactoring needed
        #region UDF Registration
        /// <summary>
        /// Register UDF with no input argument, e.g:
        ///     SqlContext.RegisterFunction&lt;bool>("MyFilter", () => true);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter()");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT>(string name, Func<RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);

            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 1 input argument, e.g:
        ///     SqlContext.RegisterFunction&lt;bool, string>("MyFilter", (arg1) => arg1 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1>(string name, Func<A1, RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 2 input arguments, e.g:
        ///     SqlContext.RegisterFunction&lt;bool, string, string>("MyFilter", (arg1, arg2) => arg1 != null &amp;&amp; arg2 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2>(string name, Func<A1, A2, RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 3 input arguments, e.g:
        ///     SqlContext.RegisterFunction&lt;bool, string, string, string>("MyFilter", (arg1, arg2, arg3) => arg1 != null &amp;&amp; arg2 != null &amp;&amp; arg3 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, columnName3)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3>(string name, Func<A1, A2, A3, RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 4 input arguments, e.g:
        ///     SqlContext.RegisterFunction&lt;bool, string, string, ..., string>("MyFilter", (arg1, arg2, ..., arg4) => arg1 != null &amp;&amp; arg2 != null &amp;&amp; ... &amp;&amp; arg3 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName4)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4>(string name, Func<A1, A2, A3, A4, RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 5 input arguments, e.g:
        ///     SqlContext.RegisterFunction&lt;bool, string, string, ..., string>("MyFilter", (arg1, arg2, ..., arg5) => arg1 != null &amp;&amp; arg2 != null &amp;&amp; ... &amp;&amp; arg5 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName5)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5>(string name, Func<A1, A2, A3, A4, A5, RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 6 input arguments, e.g:
        ///     SqlContext.RegisterFunction&lt;bool, string, string, ..., string>("MyFilter", (arg1, arg2, ..., arg6) => arg1 != null &amp;&amp; arg2 != null &amp;&amp; ... &amp;&amp; arg6 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName6)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <typeparam name="A6"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5, A6>(string name, Func<A1, A2, A3, A4, A5, A6, RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5, A6>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 7 input arguments, e.g:
        ///     SqlContext.RegisterFunction&lt;bool, string, string, ..., string>("MyFilter", (arg1, arg2, ..., arg7) => arg1 != null &amp;&amp; arg2 != null &amp;&amp; ... &amp;&amp; arg7 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName7)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <typeparam name="A6"></typeparam>
        /// <typeparam name="A7"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5, A6, A7>(string name, Func<A1, A2, A3, A4, A5, A6, A7, RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 8 input arguments, e.g:
        ///     SqlContext.RegisterFunction&lt;bool, string, string, ..., string>("MyFilter", (arg1, arg2, ..., arg8) => arg1 != null &amp;&amp; arg2 != null &amp;&amp; ... &amp;&amp; arg8 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName8)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <typeparam name="A6"></typeparam>
        /// <typeparam name="A7"></typeparam>
        /// <typeparam name="A8"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5, A6, A7, A8>(string name, Func<A1, A2, A3, A4, A5, A6, A7, A8, RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 9 input arguments, e.g:
        ///     SqlContext.RegisterFunction&lt;bool, string, string, ..., string>("MyFilter", (arg1, arg2, ..., arg9) => arg1 != null &amp;&amp; arg2 != null &amp;&amp; ... &amp;&amp; arg9 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName9)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <typeparam name="A6"></typeparam>
        /// <typeparam name="A7"></typeparam>
        /// <typeparam name="A8"></typeparam>
        /// <typeparam name="A9"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9>(string name, Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

        /// <summary>
        /// Register UDF with 10 input arguments, e.g:
        ///     SqlContext.RegisterFunction&lt;bool, string, string, ..., string>("MyFilter", (arg1, arg2, ..., arg10) => arg1 != null &amp;&amp; arg2 != null &amp;&amp; ... &amp;&amp; arg10 != null);
        ///     sqlContext.Sql("SELECT * FROM MyTable where MyFilter(columnName1, columnName2, ..., columnName10)");
        /// </summary>
        /// <typeparam name="RT"></typeparam>
        /// <typeparam name="A1"></typeparam>
        /// <typeparam name="A2"></typeparam>
        /// <typeparam name="A3"></typeparam>
        /// <typeparam name="A4"></typeparam>
        /// <typeparam name="A5"></typeparam>
        /// <typeparam name="A6"></typeparam>
        /// <typeparam name="A7"></typeparam>
        /// <typeparam name="A8"></typeparam>
        /// <typeparam name="A9"></typeparam>
        /// <typeparam name="A10"></typeparam>
        /// <param name="name"></param>
        /// <param name="f"></param>
        public void RegisterFunction<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10>(string name, Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT> f)
        {
            logger.LogInfo("Name of the function to register {0}, method info", name, f.Method);
            Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = new UdfHelper<RT, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10>(f).Execute;
            udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(typeof(RT)));
        }

		public void RegisterFunction(string name, MethodInfo f)
		{
			if (!f.IsStatic)
				throw new InvalidOperationException(f.DeclaringType?.FullName + "." + f.Name +
				                                    " is not a static method, can't be registered");
			logger.LogInfo("Name of the function to register {0}, method info", name, f.DeclaringType?.FullName + "." + f.Name);
			var helper = new UdfReflectionHelper(f);
			Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> udfHelper = helper.Execute;
			udfRegistrationProxy.RegisterFunction(name, SparkContext.BuildCommand(new CSharpWorkerFunc(udfHelper), SerializedMode.Row, SerializedMode.Row), Functions.GetReturnType(helper.ReturnType));
		}
		#endregion
	}
}
