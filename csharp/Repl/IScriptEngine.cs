// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Spark.CSharp
{
    public interface IScriptEngine
    {
        bool AddReference(string localPath);

        ScriptResult Execute(string code);

        void Close();
    }
}
