using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Services;

namespace testArgsQuotes
{
    class testArgsQuotes
    {
        const string TimeFormat = "yyyy-MM-dd HH:mm:ss";
        const string MilliTimeFormat = TimeFormat + ".fff";
        const string MicroTimeFormat = MilliTimeFormat + "fff";

        static void Log(string message)
        {
            Console.WriteLine("{0} {1} : {2}", DateTime.Now.ToString(MilliTimeFormat), typeof(testArgsQuotes).Name, message);
        }

        static void Main(string[] args)
        {
            var exe = Path.GetFileName(System.Reflection.Assembly.GetExecutingAssembly().CodeBase);
            if (args.Length < 1 || args[0] == "-h" || args[0] == "--help")
            {
                Console.WriteLine("Usage    : {0}  input-arguments", exe);
                Console.WriteLine("Example-1: {0}  any-thing that you-want-to-write=input", exe);
                var mapCurrentDir = new Dictionary<PlatformID, string> {
                    {PlatformID.Win32NT, "%CD%" }, {PlatformID.Win32S, "%CD%" }, {PlatformID.Win32Windows, "%CD%" }, {PlatformID.WinCE, "%CD%" },
                    {PlatformID.Unix, "$PWD" }
                };

                var currentDirectory = string.Empty;
                if (mapCurrentDir.TryGetValue(Environment.OSVersion.Platform, out currentDirectory))
                {
                    Console.WriteLine(@"Example-2: {0}  {1}  arg2@*#:,+.-\/~", exe, currentDirectory);
                }

                return;
            }

            var idx = 0;
            Log("args.Length = " + args.Length + Environment.NewLine
                + string.Join(Environment.NewLine, args.Select(arg => { idx++; return "args[" + idx + "] = " + arg; }))
                );

            var singleValueRDD = new List<KeyValuePair<string, int>>(
                args.Select(arg => new KeyValuePair<string, int>(arg, 1))
             );

            idx = 0;
            singleValueRDD.ForEach(kv => Log(string.Format("src-pair[{0}] : {1} = {2}", idx++, kv.Key, kv.Value)));

            var sparkContext = new SparkContext(new SparkConf().SetAppName(typeof(testArgsQuotes).Name));
            var rdd = sparkContext.Parallelize(singleValueRDD);
            Log(string.Format("Main() rdd = {0}", rdd));
            var reduced = rdd.ReduceByKey((v1, v2) => v1 + v2);
            Log("reduced.count = " + reduced.Count());
            sparkContext.Stop();
        }
    }
}
