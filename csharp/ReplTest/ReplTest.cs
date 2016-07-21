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
 
            var scriptEngine = new RoslynScriptEngine(sc);
            var ioHandler = new TestIoHandler();

            var repl = new Repl(scriptEngine, ioHandler);

            repl.Init();
            var thread = new Thread(() => { repl.Run();}) { IsBackground = false };
            thread.Start();

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

            // quit REPL
            ioHandler.input.Add(":quit");
            thread.Join();
            scriptEngine.Cleanup();

            Console.WriteLine(string.Join("\r\n", ioHandler.output));
            // Assert.AreEqual(1, ioHandler.output.Count);
            Assert.AreEqual("> ", ioHandler.output[0]);
            Assert.AreEqual(". ", ioHandler.output[1]);
            Assert.AreEqual("1024", ioHandler.output[2]);
            Assert.AreEqual("> ", ioHandler.output[3]);

            // execution exception
            Assert.IsTrue(ioHandler.output[4].Contains("System.Exception: Test"));
            Assert.AreEqual("> ", ioHandler.output[5]);
            // compile exception
            Assert.IsTrue(ioHandler.output[6].Contains("Exception"));
            Assert.AreEqual("> ", ioHandler.output[7]);
        }
    }
}
