// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Function that will be executed in CSharpWorker
    /// </summary>
    [Serializable]
    internal class CSharpWorkerFunc
    {
        // using dynamic types to keep deserialization simple in worker side
        private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> func;

        // stackTrace of this func, for debug purpose
        private readonly string stackTrace;

        public CSharpWorkerFunc(Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> func)
        {
            this.func = func;
            stackTrace = new StackTrace(true).ToString();
        }

        public CSharpWorkerFunc(Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> func, string innerStackTrace)
        {
            this.func = func;
            stackTrace = new StackTrace(true).ToString() + "\nInner stack trace ...\n" + innerStackTrace;
        }

        public Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> Func
        {
            get
            {
                return func;
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
            return new CSharpWorkerFunc(new CSharpWrokerFuncChainHelper(innerCSharpWorkerFunc.Func, outCSharpWorkerFunc.Func).Execute);
        }

        [Serializable]
        private class CSharpWrokerFuncChainHelper
        {
            private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> outerFunc;
            private readonly Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> innerFunc;

            internal CSharpWrokerFuncChainHelper(Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> iFunc,
                Func<int, IEnumerable<dynamic>, IEnumerable<dynamic>> oFunc)
            {
                innerFunc = iFunc;
                outerFunc = oFunc;
            }

            internal IEnumerable<dynamic> Execute(int split, IEnumerable<dynamic> input)
            {
                return outerFunc(split, innerFunc(split, input));
            }
        }
    }
}
