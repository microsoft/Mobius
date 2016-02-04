using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
    public class SparkCLRHost
    {
        public SparkContext sc;
        public SqlContext sqlContext;
    }

    internal class CSharpScriptEngine
    {
        private static ScriptState<object> previousState;
        private static int seq = 0;
        private static string dllDirectory;
        private static SparkCLRHost host;

        static CSharpScriptEngine()
        {
            // TODO: honor spark.local.dir setting
            dllDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(dllDirectory);
        }

        public static bool IsCompleteSubmission(string code)
        {
            SyntaxTree syntaxTree = SyntaxFactory.ParseSyntaxTree(code);
            return SyntaxFactory.IsCompleteSubmission(syntaxTree);
        }

        public static object Execute(string code)
        {
            Script<object> script;
            if (previousState == null)
            {
                script = CSharpScript.Create(code, globalsType: typeof(SparkCLRHost)).WithOptions(
                    ScriptOptions.Default
                    .AddReferences("System")
                    .AddReferences("System.Core")
                    .AddReferences("Microsoft.CSharp")
                    .AddReferences(typeof(SparkContext).Assembly)
                    .AddReferences(typeof(Pickler).Assembly)
                    .AddReferences(typeof(Parser).Assembly));

                Environment.SetEnvironmentVariable("SPARKCLR_RUN_MODE", "shell");
                Environment.SetEnvironmentVariable("SPARKCLR_SCRIPT_COMPILATION_DIR", dllDirectory);
            }
            else
            {
                script = previousState.Script.ContinueWith(code);
            }

            var diagnostics = script.Compile();
            bool hasErrors = Enumerable.Any(diagnostics, diagnostic => diagnostic.Severity == DiagnosticSeverity.Error);

            if (hasErrors)
            {
                DisplayErrors(script.Compile());
                return null;
            }
            
            PersistCompilation(script.GetCompilation());
            if (host == null)
            {
                var conf = new SparkConf();
                var sc = new SparkContext(conf);
                host = new SparkCLRHost
                {
                    sc = sc, 
                    sqlContext = new SqlContext(sc)
                };
            }
            ScriptState<object> endState = null;
            if (previousState == null)
            {
                endState = script.RunAsync(host).Result;
            }
            else
            {
                // "ContinueAsync" is a internal methold now, might be public in 1.2.0(https://github.com/dotnet/roslyn/issues/6612)
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
            return endState.ReturnValue;
        }

        private static void DisplayErrors(IEnumerable<Diagnostic> diagnostics)
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

        private static void PersistCompilation(Compilation compilation)
        {
            using (FileStream stream = new FileStream(string.Format(@"{0}\{1}.dll", dllDirectory, seq++), FileMode.CreateNew))
            {
                compilation.Emit(stream);
            }
        }
    }

    class Shell
    {
        static void Main(string[] args)
        {
            CSharpScriptEngine.Execute(@"
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
                var line = Console.ReadLine();
                if (string.IsNullOrEmpty(line))
                {
                    continue;
                }

                if (line.Trim().Equals(":quit", StringComparison.InvariantCultureIgnoreCase))
                {
                    break;
                }

                var returnValue = CSharpScriptEngine.Execute(line);
                if (returnValue != null)
                {
                    Console.WriteLine(returnValue);
                }
            }
        }
    }
}
