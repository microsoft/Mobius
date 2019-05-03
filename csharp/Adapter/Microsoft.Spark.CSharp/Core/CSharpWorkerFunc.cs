// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using SerializationHelpers.Data;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using SerializationHelpers.Extensions;
using System.Runtime.Serialization;

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
                return expressionData.ToFunc<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>>();
            }
        }

        internal Expression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>> Expr
        {
            get
            {
                return expressionData.ToExpression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>>();
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

        /// <summary>
        /// Used to chain functions
        /// </summary>
        public static CSharpWorkerFunc Chain(CSharpWorkerFunc innerCSharpWorkerFunc, CSharpWorkerFunc outCSharpWorkerFunc)
        {
            return new CSharpWorkerFunc((chainX, chainY) => new CSharpWrokerFuncChainHelper((innerX, innerY) => innerCSharpWorkerFunc.Func(innerX, innerY), (outerX, outerY) => outCSharpWorkerFunc.Func(outerX, outerY)).Execute(chainX, chainY));
        }

        [Serializable]
        private class CSharpWrokerFuncChainHelper
        {
            //private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> outerFunc;
            //private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> innerFunc;
            private readonly LinqExpressionData outerFuncExpressionData;
            private readonly LinqExpressionData innerFuncExpressionData;
            internal CSharpWrokerFuncChainHelper(Expression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>> iFunc,
                Expression<Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>>> oFunc)
            {
                innerFuncExpressionData = iFunc.ToExpressionData();
                outerFuncExpressionData = oFunc.ToExpressionData();
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
