// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Text;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp
{
    public interface IoHandler
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

    public class ConsoleIoHandler : IoHandler
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

    public class Repl
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
        }

        public void Run()
        {
            while (true)
            {
                ioHandler.Write("> ");
                var inputLines = new StringBuilder();
                bool cancelSubmission = false;
                ScriptResult scriptResult = null;

                while (true)
                {
                    var line = ioHandler.ReadLine();
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        cancelSubmission = true;
                        break;
                    }

                    if (line.Trim().Equals(":quit", StringComparison.InvariantCultureIgnoreCase))
                    {
                        return;
                    }

                    inputLines.AppendLine(line);
                    scriptResult = scriptEngine.Execute(inputLines.ToString());
                    if (scriptResult.IsCompleteSubmission)
                    {
                        break;
                    }

                    ioHandler.Write(". ");
                }

                if (cancelSubmission)
                {
                    continue;
                }

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
        }

        

        static void Main(string[] args)
        {
            SparkConf sparkConf = new SparkConf();
            SparkContext sc = new SparkContext(sparkConf);
            var scriptEngine = new RoslynScriptEngine(sc);
            var repl = new Repl(scriptEngine, new ConsoleIoHandler());
            repl.Init();
            repl.Run();
            scriptEngine.Cleanup();
        }
    }
}
