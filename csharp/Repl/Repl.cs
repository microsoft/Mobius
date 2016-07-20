// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Text;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp
{
    class Repl
    {
        static void Main(string[] args)
        {
            var sparkConf = new SparkConf();
            var sc = new SparkContext(sparkConf);
            var scriptEngine = new RoslynScriptEngine(sparkConf, sc);

            scriptEngine.Execute(@"
            using System;
            using System.Collections.Generic;
            using Microsoft.Spark.CSharp.Core;
            using Microsoft.Spark.CSharp.Interop;
            ");
            Console.WriteLine("Spark context available as sc.");
            Console.WriteLine("SQL context available as sqlContext.");
            Console.WriteLine("Use :quit to exit.");

            while (true)
            {
                Console.Write("> ");
                var inputLines = new StringBuilder();
                bool cancelSubmission = false;
                ScriptResult scriptResult = null;

                while (true)
                {
                    var line = Console.ReadLine();
                    if (string.IsNullOrEmpty(line))
                    {
                        cancelSubmission = true;
                        break;
                    }

                    if (line.Trim().Equals(":quit", StringComparison.InvariantCultureIgnoreCase))
                    {
                        scriptEngine.Clear();
                        return;
                    }

                    inputLines.AppendLine(line);
                    scriptResult = scriptEngine.Execute(inputLines.ToString());
                    if (scriptResult.IsCompleteSubmission)
                    {
                        break;
                    }

                    Console.Write(". ");
                }

                if (cancelSubmission)
                {
                    continue;
                }

                if (scriptResult.CompileExceptionInfo != null)
                {
                    DisplayException(scriptResult.CompileExceptionInfo.SourceException);
                }
                else
                {
                    if (scriptResult.ReturnValue != null)
                    {
                        Console.WriteLine(scriptResult.ReturnValue);
                    }
                }
            }
        }

        /// <summary>
        /// Diplay exception on Console
        /// </summary>
        internal static void DisplayException(Exception e)
        {
            try
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(e.ToString());
            }
            finally
            {
                Console.ResetColor();
            }
        }
    }
}
