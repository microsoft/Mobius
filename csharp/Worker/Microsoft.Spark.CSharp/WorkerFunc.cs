// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System.Runtime.Serialization;
using Microsoft.Spark.CSharp.Core;
using System.Collections.Generic;
using SerializationHelpers.Context;

namespace Microsoft.Spark.CSharp
{
    internal class WorkerFunc
    {
        private CSharpWorkerFunc _charpWorkerFunc;
        internal CSharpWorkerFunc CharpWorkerFunc {
            get
            {
                this._charpWorkerFunc.ExpressionContext = new WorkerExpressionContext();
                return this._charpWorkerFunc;
            }

            set
            {
                this._charpWorkerFunc = value;
            }
        }

        internal int ArgsCount { get; }

        internal List<int> ArgOffsets { get; }

        internal WorkerFunc(CSharpWorkerFunc func, int argsCount, List<int> argOffsets)
        {
            CharpWorkerFunc = func;
            ArgsCount = argsCount;
            ArgOffsets = argOffsets;
        }                
    }
}
