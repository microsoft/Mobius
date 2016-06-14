using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Streaming;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Services;
using System.Text.RegularExpressions;
using System.Threading;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace testKeyValueStream
{
    public class testKeyValueStream
    {
        static string host = "127.0.0.1";
        static int port = 9111;
        static int windowSeconds = 1;
        static int runningSeconds = 30;
        static int totalTestTimes = 20;
        static string checkpointDirectory = "checkDir";
        static bool isReduceByKeyAndWindow = false;
        static bool isValueArray = true;
        static bool isUnevenArray = false;
        static long valueElements = 1024 * 1024 * 20;
        static bool needSaveToTxtFile = false;
        static bool checkArrayBeforeReduce = true;

        public static void Main(string[] args)
        {
            var exeName = Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().CodeBase);

            // TestReceivedLine();
            if (args.Length < 1 || args[0] == "-h" || args[0] == "--help")
            {
                Console.WriteLine("Usage :    {0}  Host       Port [windowSeconds] [runningSeconds] [testTimes] [checkpointDirectory] [isReduceByKeyAndWindow] [isValueArray] [isUnevenArray] [valueArrayElements] [saveTxt] [checkArrayBeforeReduce]", exeName);
                Console.WriteLine("Example-1: {0}  127.0.0.1  9111   1               30               20         checkDir               0                        1          0                  1048576              0         1", exeName);
                Console.WriteLine("Example-2: {0}  {1} 9111 1 30 20 checkDir 0 1 0 1048576 0 1", exeName, GetHost());
                Console.WriteLine("The above host and port are from a tool : SourceLinesSocket in this project.");
                return;
            }

            int idxArg = -1;

            host = TestUtils.GetArgValue(ref idxArg, args, "host", host, false);
            port = TestUtils.GetArgValue(ref idxArg, args, "port", port, false);
            windowSeconds = TestUtils.GetArgValue(ref idxArg, args, "windowSeconds", windowSeconds);
            runningSeconds = TestUtils.GetArgValue(ref idxArg, args, "runningSeconds", runningSeconds);
            totalTestTimes = TestUtils.GetArgValue(ref idxArg, args, "totalTestTimes", totalTestTimes);
            checkpointDirectory = TestUtils.GetArgValue(ref idxArg, args, "checkpointDirectory", checkpointDirectory);
            isReduceByKeyAndWindow = TestUtils.GetArgValue(ref idxArg, args, "isReduceByKeyAndWindow", isReduceByKeyAndWindow);
            isValueArray = TestUtils.GetArgValue(ref idxArg, args, "isValueArray", isValueArray);
            isUnevenArray = TestUtils.GetArgValue(ref idxArg, args, "isUnevenArray", isUnevenArray);
            valueElements = TestUtils.GetArgValue(ref idxArg, args, "valueArrayElements", valueElements);
            needSaveToTxtFile = TestUtils.GetArgValue(ref idxArg, args, "needSaveToTxtFile", needSaveToTxtFile);
            checkArrayBeforeReduce = TestUtils.GetArgValue(ref idxArg, args, "checkArrayBeforeReduce", checkArrayBeforeReduce);

            Log("will connect " + host + ":" + port + " , windowSeconds = " + windowSeconds + " s. " + checkpointDirectory + " = " + checkpointDirectory + ", is-array-test = " + isValueArray);

            var prefix = exeName + (isValueArray ? "-array" + (isUnevenArray ? "-uneven" : "-even") : "-single") + "-";
            var sc = new SparkContext(new SparkConf().SetAppName(prefix));

            var beginTime = DateTime.Now;

            Action<long> testOneStreaming = (testTime) =>
            {
                var timesInfo = " test[" + testTime + "]-" + totalTestTimes + " ";
                Log("============== begin of " + timesInfo + " =========================");
                var durationSeconds = windowSeconds;
                var ssc = new StreamingContext(sc, durationSeconds);
                ssc.Checkpoint(checkpointDirectory);
                var lines = ssc.SocketTextStream(host, port, StorageLevelType.MEMORY_AND_DISK_SER);

                StartOneTest(sc, lines, prefix);

                ssc.Start();
                var startTime = DateTime.Now;
                ssc.AwaitTerminationOrTimeout(runningSeconds * 1000);
                Log(string.Format("============= end of {0}, start from {1} , used {2} s. total cost {3} s.======================",
                    timesInfo, startTime.ToString(TestUtils.MilliTimeFormat), (DateTime.Now - startTime).TotalSeconds, (DateTime.Now - beginTime).TotalSeconds));
                ssc.Stop();
            };


            for (var times = 0; times < totalTestTimes; times++)
            {
                testOneStreaming(times + 1);
            }

            Log("finished all test , total test times = " + totalTestTimes + ", used time = " + (DateTime.Now - beginTime));
        }

        static void Log(string message)
        {
            Console.WriteLine("{0} {1} : {2}", TestUtils.NowMilli, typeof(testKeyValueStream).Name, message);
        }

        static void StartOneTest(SparkContext sc, DStream<string> lines, string prefix, string suffix = ".txt")
        {
            if (!isValueArray)
            {
                var pairs = lines.Map(line => new ParseKeyValue(0).Parse(line));
                var reducedStream = isReduceByKeyAndWindow ? pairs.ReduceByKeyAndWindow(SumReduce, InverseSum, windowSeconds)
                    : pairs.ReduceByKey(SumReduce);
                ForEachRDD("KeyValue", reducedStream, prefix, suffix);
            }
            else if (isUnevenArray)
            {
                var pairs = lines.Map(line => new ParseKeyValueUnevenArray(valueElements).Parse(line));
                var reducedStream = isReduceByKeyAndWindow ? pairs.ReduceByKeyAndWindow(SumReduce, InverseSum, windowSeconds)
                    : pairs.ReduceByKey(SumReduce);
                ForEachRDD("KeyValueUnevenArray", reducedStream, prefix, suffix);

            }
            else
            {
                var pairs = lines.Map(line => new ParseKeyValueArray(valueElements).Parse(line));
                var reducedStream = isReduceByKeyAndWindow ? pairs.ReduceByKeyAndWindow(SumReduce, InverseSum, windowSeconds)
                    : pairs.ReduceByKey(SumReduce);
                ForEachRDD("KeyValueEvenArray", reducedStream, prefix, suffix);
            }
        }

        static void ForEachRDD<V>(string title, DStream<KeyValuePair<string, V>> reducedStream, string prefix, string suffix = ".txt")
        {
            Log("ForEachRDD " + title);
            reducedStream.ForeachRDD((time, rdd) =>
            {
                var taken = rdd.Collect();
                Console.WriteLine("{0} taken.length = {1} , taken = {2}", TestUtils.NowMilli, taken.Length, taken);

                foreach (object record in taken)
                {
                    KeyValuePair<string, V> countByWord = (KeyValuePair<string, V>)record;
                    Console.WriteLine("{0} record = {1}, countByWord : key = {2}, value = {3}", TestUtils.NowMilli, record, countByWord.Key, GetValueText(countByWord.Value));
                }
            });

            Log(string.Format("{0} reducedStream.Count = {1}", title, reducedStream.Count()));

            if (needSaveToTxtFile)
            {
                reducedStream.SaveAsTextFiles(prefix, suffix);
            }
        }

        static int[] SumReduce(int[] a, int[] b)
        {
            Log(string.Format("SumReduce() a{0}, b{1}", TestUtils.ArrayToText(a), TestUtils.ArrayToText(b)));

            if (checkArrayBeforeReduce)
            {
                if (a == null || b == null)
                {
                    return a == null ? b : a;
                }

                if (a.Length == 0 || b.Length == 0)
                {
                    return a.Length == 0 ? b : a;
                }
            }

            var count = checkArrayBeforeReduce ? Math.Min(a.Length, b.Length) : a.Length;
            var c = new int[count];
            for (var k = 0; k < c.Length; k++)
            {
                c[k] = a[k] + b[k];
            }

            return c;
        }

        static int SumReduce(int a, int b)
        {
            return a + b;
        }

        static int[] InverseSum(int[] a, int[] b)
        {
            Log(string.Format("InverseSum() a{0}, b{1}", TestUtils.ArrayToText(a), TestUtils.ArrayToText(b)));
            if (checkArrayBeforeReduce)
            {
                if (a == null || b == null)
                {
                    return a == null ? b : a;
                }

                if (a.Length == 0 || b.Length == 0)
                {
                    return a.Length == 0 ? b : a;
                }
            }

            var count = checkArrayBeforeReduce ? Math.Min(a.Length, b.Length) : a.Length;
            var c = new int[count];
            for (var k = 0; k < c.Length; k++)
            {
                c[k] = a[k] - b[k];
            }
            return c;
        }

        static int InverseSum(int a, int b)
        {
            return a - b;
        }

        static string GetValueText(object value)
        {
            if (value == null)
            {
                return null;
            }

            if (value is int)
            {
                return ((int)value).ToString();
            }
            else if (value is int[])
            {
                return TestUtils.ArrayToText((int[])value);
            }
            else
            {
                return value.ToString();
            }
        }

        static IPAddress GetHost(bool print = false)
        {
            IPAddress[] ips = Dns.GetHostAddresses(Dns.GetHostName());
            var ipList = new List<IPAddress>();
            foreach (IPAddress ipa in ips)
            {
                if (ipa.AddressFamily != AddressFamily.InterNetwork)
                {
                    continue;
                }

                if (print)
                {
                    Console.WriteLine("ip = {0}, AddressFamily = {1}", ipa, ipa.AddressFamily);
                }

                var ip = ipa.ToString();
                if (!ip.StartsWith("10.0.2.") && !ip.StartsWith("192.168."))
                {
                    return ipa;
                }
            }

            return IPAddress.Parse("127.0.0.1");
        }
    }
}
