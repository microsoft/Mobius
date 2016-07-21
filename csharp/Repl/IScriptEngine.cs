// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Spark.CSharp
{
    public interface IScriptEngine
    {
        ScriptResult Execute(string code);

        void Cleanup();
    }
}
