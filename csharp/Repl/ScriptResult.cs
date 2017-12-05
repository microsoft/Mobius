// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Runtime.ExceptionServices;

namespace Microsoft.Spark.CSharp
{
    public class ScriptResult
    {
        public static readonly ScriptResult Incomplete = new ScriptResult { IsCompleteSubmission = false };

        public static readonly ScriptResult Empty = new ScriptResult();
        
        public ScriptResult(
            object returnValue = null,
            Exception executionException = null,
            Exception compilationException = null)
        {
            if (returnValue != null)
            {
                ReturnValue = returnValue;
            }

            if (executionException != null)
            {
                ExecuteExceptionInfo = ExceptionDispatchInfo.Capture(executionException);
            }

            if (compilationException != null)
            {
                CompileExceptionInfo = ExceptionDispatchInfo.Capture(compilationException);
            }

            IsCompleteSubmission = true;
        }

        public bool IsCompleteSubmission { get; private set; }

        public object ReturnValue { get; private set; }

        public ExceptionDispatchInfo ExecuteExceptionInfo { get; private set; }

        public ExceptionDispatchInfo CompileExceptionInfo { get; private set; }
    }
}
