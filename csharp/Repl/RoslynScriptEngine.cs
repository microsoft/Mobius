// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using Razorvine.Pickle;
using Razorvine.Serpent;

namespace Microsoft.Spark.CSharp
{
    /// <summary>
    /// Class that holds all objects shared in Roslyn runtime context.
    /// </summary>
    public class SparkCLRHost
    {
        public SparkContext sc;
        public SqlContext sqlContext;
    }

    /// <summary>
    /// A C# script engine based on Roslyn(https://github.com/dotnet/roslyn).
    /// </summary>
    public class RoslynScriptEngine : IScriptEngine
    {
        private ScriptState<object> previousState;
        private int seq = 0;
        private readonly string compilationDumpDirectory;

        private readonly SparkConf sparkConf;
        private readonly SparkContext sc;
        private readonly SparkCLRHost host;
        private readonly ParseOptions options;

        public RoslynScriptEngine(SparkContext sc)
        {
            this.sc = sc;
            sparkConf = sc.GetConf();
            host = new SparkCLRHost
            {
                sc = sc,
                sqlContext = new SqlContext(sc)
            };

            var sparkLocalDir = sparkConf.Get("spark.local.dir", Path.GetTempPath());
            compilationDumpDirectory = Path.Combine(sparkLocalDir, Path.GetRandomFileName());
            Directory.CreateDirectory(compilationDumpDirectory);

            options = new CSharpParseOptions(LanguageVersion.CSharp6, DocumentationMode.Parse, SourceCodeKind.Script);
        }

        internal Script<object> CreateScript(string code)
        {
            var scriptOptions = ScriptOptions.Default
                                .AddReferences("System")
                                .AddReferences("System.Core")
                                .AddReferences("Microsoft.CSharp")
                                .AddReferences(typeof(SparkContext).Assembly)
                                .AddReferences(typeof(Pickler).Assembly)
                                .AddReferences(typeof(Parser).Assembly);

            return CSharpScript.Create(code, globalsType: typeof(SparkCLRHost)).WithOptions(scriptOptions);
        }

        public ScriptResult Execute(string code)
        {
            if (!IsCompleteSubmission(code))
            {
                return ScriptResult.Incomplete;
            }

            Script<object> script;
            if (previousState == null)
            {
                script = CreateScript(code);

                Environment.SetEnvironmentVariable("SPARKCLR_RUN_MODE", "R");
                if (sparkConf.Get("spark.master", "local").StartsWith("local", StringComparison.InvariantCultureIgnoreCase))
                {
                    Environment.SetEnvironmentVariable("SPARKCLR_SCRIPT_COMPILATION_DIR", compilationDumpDirectory);
                }
            }
            else
            {
                script = previousState.Script.ContinueWith(code);
            }

            var diagnostics = script.Compile();
            bool hasErrors = Enumerable.Any(diagnostics, diagnostic => diagnostic.Severity == DiagnosticSeverity.Error);

            if (hasErrors)
            {
                DisplayDiagnostics(diagnostics);
                var message = new StringBuilder();
                foreach (var diagnostic in diagnostics)
                {
                    message.Append("[").Append(diagnostic.Severity).Append("] ").Append(": ").Append(diagnostic).Append("\r\n");
                }
                return new ScriptResult(compilationException: new Exception(message.ToString()));
            }

            var compilationDump = DumpCompilation(script.GetCompilation());
            if (new FileInfo(compilationDump).Length > 0)
            {
                // Ship compilation binary to executor side leveraging sparkContext.AddFile() method.
                sc.AddFile(new Uri(compilationDump).ToString());
            }

            try
            {
                ScriptState<object> endState = null;
                if (previousState == null)
                {
                    endState = script.RunAsync(host).Result;
                }
                else
                {
                    // Currently "ContinueAsync" is a internal methold, might go public in 1.2.0(https://github.com/dotnet/roslyn/issues/6612)
                    const string methodName = "ContinueAsync";
                    var m = script.GetType().GetMethod(methodName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                    if (m != null)
                    {
                        endState = ((Task<ScriptState<object>>)m.Invoke(script, new object[] { previousState, default(CancellationToken) })).Result;
                    }
                    else
                    {
                        throw new InvalidOperationException(string.Format("Can't find method {0}", methodName));
                    }
                }
                previousState = endState;
                return new ScriptResult(returnValue: endState.ReturnValue);
            }
            catch (Exception e)
            {
                return new ScriptResult(executionException: e);
            }
        }

        internal void DisplayDiagnostics(IEnumerable<Diagnostic> diagnostics)
        {
            // refer to http://source.roslyn.io/#Microsoft.CodeAnalysis.Scripting/Hosting/CommandLine/CommandLineRunner.cs,268
            try
            {
                foreach (var diagnostic in diagnostics)
                {
                    Console.ForegroundColor = (diagnostic.Severity == DiagnosticSeverity.Error) ? ConsoleColor.Red : ConsoleColor.Yellow;
                    Console.WriteLine(diagnostic.ToString());
                }
            }
            finally
            {
                Console.ResetColor();
            }
        }

        /// <summary>
        /// Check whether the given code is a complete submission
        /// </summary>
        internal bool IsCompleteSubmission(string code)
        {
            SyntaxTree syntaxTree = SyntaxFactory.ParseSyntaxTree(code, options);
            return SyntaxFactory.IsCompleteSubmission(syntaxTree);
        }

        /// <summary>
        /// Dump generated compilation binary to dump directory for further use.
        /// </summary>
        /// <param name="compilation"></param>
        /// <returns></returns>
        private string DumpCompilation(Compilation compilation)
        {
            var dump = string.Format(@"{0}\ReplCompilation.{1}", compilationDumpDirectory, seq++);
            using (var stream = new FileStream(dump, FileMode.CreateNew))
            {
                compilation.Emit(stream);
                return dump;
            }
        }

        /// <summary>
        /// Clean up compilation dump directory.
        /// </summary>
        public void Cleanup()
        {
            // delete DLLs dump directory on exit
            if (Directory.Exists(compilationDumpDirectory))
            {
                Directory.Delete(compilationDumpDirectory, true);
            }
        }
    }
}
