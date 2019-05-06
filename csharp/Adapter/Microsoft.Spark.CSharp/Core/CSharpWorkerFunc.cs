// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SerializationHelpers.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using SerializationHelpers.Extensions;
using System.Runtime.Serialization;
using Serialize.Linq.Interfaces;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Function that will be executed in CSharpWorker
    /// </summary>
    [DataContract]
    [Serializable]
    internal class CSharpWorkerFunc
    {
        // using dynamic types to keep deserialization simple in worker side
        //private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> func;
        [DataMember]
        private LinqExpressionData expressionData;
        // stackTrace of this func, for debug purpose
        [DataMember]
        private readonly string stackTrace;

        public CSharpWorkerFunc() { }
        public CSharpWorkerFunc(Expression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>> func)
        {
            this.expressionData = func.ToExpressionData();
            stackTrace = new StackTrace(true).ToString().Replace("   at ", "   [STACK] ");
        }

        public CSharpWorkerFunc(Expression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>> func, string innerStackTrace)
            : this(func)
        {
            stackTrace += string.Format("   [STACK] --- Inner stack trace: ---{0}{1}",
                Environment.NewLine, innerStackTrace.Replace("   at ", "   [STACK] "));
        }      

        public Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> Func
        {
            get
            {
                return expressionData.ToFunc<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>>(ExpressionContext);
            }
        }

        internal Expression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>> Expr
        {
            get
            {
                return expressionData.ToExpression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>>(ExpressionContext);
            }
        }

        internal LinqExpressionData ExpressionData
        {
            get
            {
                return expressionData;
            }
        }

        public string StackTrace
        {
            get
            {
                return stackTrace;
            }
        }

        internal IExpressionContext ExpressionContext { get; set; }


        /// <summary>
        /// Used to chain functions
        /// </summary>
        public static CSharpWorkerFunc Chain(CSharpWorkerFunc innerCSharpWorkerFunc, CSharpWorkerFunc outCSharpWorkerFunc)
        {
            var helper = new CSharpWrokerFuncChainHelper(innerCSharpWorkerFunc.ExpressionData, outCSharpWorkerFunc.ExpressionData);
            return new CSharpWorkerFunc((chainX, chainY) => helper.Execute(chainX, chainY));
        }

        [Serializable]
        [DataContract]
        private class CSharpWrokerFuncChainHelper
        {
            //private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> outerFunc;
            //private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> innerFunc;
            [DataMember]
            private readonly LinqExpressionData outerFuncExpressionData;
            [DataMember]
            private readonly LinqExpressionData innerFuncExpressionData;

            internal CSharpWrokerFuncChainHelper() { }
            internal CSharpWrokerFuncChainHelper(Expression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>> iFunc,
                Expression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>> oFunc)
            {
                innerFuncExpressionData = iFunc.ToExpressionData();
                outerFuncExpressionData = oFunc.ToExpressionData();
            }

            internal CSharpWrokerFuncChainHelper(LinqExpressionData iFunc,
                LinqExpressionData oFunc)
            {
                innerFuncExpressionData = iFunc;
                outerFuncExpressionData = oFunc;
            }
            internal IEnumerable<dynamic> Execute(int split, IEnumerable<dynamic> input)
            {
                var outerFunc = outerFuncExpressionData.ToFunc<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>>();
                var innerFunc = innerFuncExpressionData.ToFunc<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>>();
                return outerFunc(split, innerFunc(split, input));
            }
        }
    }
}
