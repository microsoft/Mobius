// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;

namespace Microsoft.Spark.CSharp
{
    internal interface IoHandler
    {
        void Write(Object obj);

        /// <summary>
        /// Write result to underlying output stream.
        /// </summary>
        void WriteLine(Object obj);

        /// <summary>
        /// Write exception to underlying output stream
        /// </summary>
        /// <param name="e"></param>
        void WriteException(Exception e);

        /// <summary>
        /// Read codes that will be executed. Current thread will be blocked if no input available.
        /// </summary>
        string ReadLine();
    }

    internal class ConsoleIoHandler : IoHandler
    {
        public void Write(object obj)
        {
            Console.Write(obj.ToString());
        }

        public void WriteLine(Object obj)
        {
            Console.WriteLine(obj);
        }

        /// <summary>
        /// Diplay exception on Console
        /// </summary>
        public void WriteException(Exception e)
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

        public string ReadLine()
        {
            return Console.ReadLine();
        }
    }

    internal class Repl
    {
        private readonly IScriptEngine scriptEngine;
        private readonly IoHandler ioHandler;

        public Repl(IScriptEngine scriptEngine, IoHandler ioHandler)
        {
            this.scriptEngine = scriptEngine;
            this.ioHandler = ioHandler;
        }

        public void Init()
        {
            scriptEngine.Execute(@"
            using System;
            using System.Collections.Generic;
            using Microsoft.Spark.CSharp.Core;
            using Microsoft.Spark.CSharp.Interop;
            ");

            ioHandler.WriteLine("Spark context available as sc.");
            ioHandler.WriteLine("SQL context available as sqlContext.");
            ioHandler.WriteLine("Use :quit to exit.");
            ioHandler.WriteLine("Type \":help\" for more information.");
        }

        public void Run()
        {
            var terminated = false;

            while (true)
            {
                ioHandler.Write("> ");
                var inputLines = new StringBuilder();
                var cancelSubmission = false;
                ScriptResult scriptResult = null;

                while (true)
                {
                    var line = ioHandler.ReadLine();

                    if (IsDirective(line))
                    {
                        ProcessDirective(line, ref terminated);
                        break;
                    }

                    inputLines.AppendLine(line);
                    scriptResult = scriptEngine.Execute(inputLines.ToString());

                    if (ScriptResult.Empty.Equals(scriptResult))
                    {
                        cancelSubmission = true;
                        break;
                    }

                    if (scriptResult.IsCompleteSubmission)
                    {
                        break;
                    }

                    ioHandler.Write(". ");
                }

                if (terminated) break;

                if (cancelSubmission || scriptResult == null) continue;

                ProcessExecutionResult(scriptResult);
            }
        }

        internal bool IsDirective(string line)
        {
            return Regex.Match(line.Trim(), "^:\\S+").Success;
        }

        internal void ProcessDirective(string directive, ref bool terminated)
        {
            var verb = directive.Split(new[] {" "}, StringSplitOptions.RemoveEmptyEntries)[0];
            switch (verb)
            {
                case ":quit": // quit
                    terminated = true;
                    break;

                case ":load": // load DLL
                    LoadAssebmly(directive);
                    break;

                case ":help": // display help message
                    Help();
                    break;

                default:
                    ioHandler.WriteException(new Exception("Invalid directive." + verb));
                    break;
            }
        }

        internal void ProcessExecutionResult(ScriptResult scriptResult)
        {
            if (scriptResult.CompileExceptionInfo != null)
            {
                ioHandler.WriteException(scriptResult.CompileExceptionInfo.SourceException);
            }
            else if (scriptResult.ExecuteExceptionInfo != null)
            {
                ioHandler.WriteException(scriptResult.ExecuteExceptionInfo.SourceException);
            }
            else if (scriptResult.ReturnValue != null)
            {
                ioHandler.WriteLine(scriptResult.ReturnValue);
            }
        }

        internal void LoadAssebmly(string directive)
        {
            var match = Regex.Match(directive.Trim(), ":load\\s+\"(.*?)\"");
            if (match.Success)
            {
                var assebmlyPath = match.Groups[1].Value;
                if (scriptEngine.AddReference(assebmlyPath))
                {
                    ioHandler.WriteLine("Loaded assebmly from " + assebmlyPath);
                }
                else
                {
                    ioHandler.WriteLine("Failed to load assebmly from " + assebmlyPath);
                }

            }
            else
            {
                ioHandler.WriteLine("[Error] Invalid :load directive.");
            }
        }

        internal void Help()
        {
            const string helps = "Commands:\r\n  :help\t\tDisplay help on available commands.\r\n  :load\t\tLoad extra library to current execution context, e.g. :load \"myLib.dll\".\r\n  :quit\t\tExit REPL.";
            ioHandler.WriteLine(helps);
        }

        static void Main(string[] args)
        {
            SparkConf sparkConf = new SparkConf();
            SparkContext sc = new SparkContext(sparkConf);
            SqlContext sqlContext = new SqlContext(sc);
            var scriptEngine = new RoslynScriptEngine(sc, sqlContext);
            var repl = new Repl(scriptEngine, new ConsoleIoHandler());
            repl.Init();
            repl.Run();
            scriptEngine.Close();
        }
    }
}
