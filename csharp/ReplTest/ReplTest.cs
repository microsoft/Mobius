// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using Moq;
using NUnit.Framework;

namespace ReplTest
{
    [TestFixture]
    public class ReplTest
    {
        internal class TestIoHandler : IoHandler
        {
            internal BlockingCollection<string> input = new BlockingCollection<string>();
            internal List<string> output = new List<string>();

            public void Write(object obj)
            {
                WriteLine(obj);
            }

            public void WriteLine(object obj)
            {
                output.Add(obj.ToString());
            }

            public void WriteException(Exception e)
            {
                output.Add(e.ToString());
            }

            public string ReadLine()
            {
                return input.Take();
            }
        }

        [Test]
        public void Test()
        {
            var sc = new SparkContext("", "");
            var sqlContext = new SqlContext(sc);
 
            var scriptEngine = new RoslynScriptEngine(sc, sqlContext);
            var ioHandler = new TestIoHandler();

            var repl = new Repl(scriptEngine, ioHandler);

            repl.Init();
            var thread = new Thread(() => { repl.Run();}) { IsBackground = false };
            thread.Start();

            Thread.Sleep(1000);
            
            Assert.IsTrue(ioHandler.output.Any());
            Assert.AreEqual("> ", ioHandler.output.Last());

            ioHandler.output.Clear();

            // empty input
            ioHandler.input.Add(" ");

            // incomplete code block
            ioHandler.input.Add("if (true) {");
            ioHandler.input.Add("return 1024; }");

            // execution exception
            ioHandler.input.Add("new Exception(\"Test\")");

            // compile exception
            ioHandler.input.Add("var a=;");

            // load non-exist library
            ioHandler.input.Add(":load \"non-exist.dll\"");

            // load library
            var sampleDLL = scriptEngine.CompilationDumpPath(0);
            ioHandler.input.Add(":load \"" + sampleDLL + "\"");

            // invalid :load directive
            ioHandler.input.Add(":load x");

            // invalid directive
            ioHandler.input.Add(":invalid directive");

            // :help directive
            ioHandler.input.Add(":help");

            // quit REPL
            ioHandler.input.Add(":quit");
            thread.Join();
            scriptEngine.Close();

            Console.WriteLine(string.Join("\r\n", ioHandler.output));
            var seq = 0;
            Assert.AreEqual("> ", ioHandler.output[seq++]);
            Assert.AreEqual(". ", ioHandler.output[seq++]);
            Assert.AreEqual("1024", ioHandler.output[seq++]);
            Assert.AreEqual("> ", ioHandler.output[seq++]);

            // execution exception
            Assert.IsTrue(ioHandler.output[seq++].Contains("System.Exception: Test"));
            Assert.AreEqual("> ", ioHandler.output[seq++]);

            // compile exception
            Assert.IsTrue(ioHandler.output[seq++].Contains("Exception"));
            Assert.AreEqual("> ", ioHandler.output[seq++]);

            // load non-exist library
            Assert.IsTrue(ioHandler.output[seq++].Contains("Failed to load assebmly"));
            Assert.AreEqual("> ", ioHandler.output[seq++]);

            // load library
            Assert.IsTrue(ioHandler.output[seq++].Contains("Loaded assebmly"));
            Assert.AreEqual("> ", ioHandler.output[seq++]);

            // invalid :load directive
            Assert.IsTrue(ioHandler.output[seq++].Contains("Invalid :load directive"));
            Assert.AreEqual("> ", ioHandler.output[seq++]);

            // invalid directive
            Assert.IsTrue(ioHandler.output[seq++].Contains("Invalid directive"));
            Assert.AreEqual("> ", ioHandler.output[seq++]);

            // help directive
            Assert.IsTrue(ioHandler.output[seq++].Contains("Commands"));
            Assert.AreEqual("> ", ioHandler.output[seq++]);
        }

        [Test]
        public void TestConsoleIoHandler()
        {
            var handler = new ConsoleIoHandler();
            handler.WriteLine("line1");
            handler.Write("> ");
            handler.WriteException(new Exception("Message1"));
        }
    }
}
