using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace testKeyValueStream
{
    public class TestUtils
    {
        public const string TimeFormat = "yyyy-MM-dd HH:mm:ss";
        public const string MilliTimeFormat = TimeFormat + ".fff";
        public const string MicroTimeFormat = MilliTimeFormat + "fff";

        public static string Now { get { return DateTime.Now.ToString(TimeFormat); } }

        public static string NowMilli { get { return DateTime.Now.ToString(MilliTimeFormat); } }

        public static string NowMicro { get { return DateTime.Now.ToString(MicroTimeFormat); } }

        public static string ArrayToText<T>(T[] array, int takeMaxElementCount = 9)
        {
            if (array == null)
            {
                return "[] = null";
            }
            else if (array.Length == 0)
            {
                return "[0] = " + array;
            }
            else if (array.Length <= takeMaxElementCount)
            {
                return "[" + array.Length + "] = " + string.Join(", ", array);
            }
            else
            {
                return "[" + array.Length + "] = " + string.Join(", ", array.Take(takeMaxElementCount)) + ", ... , " + array.Last();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="ArgType"></typeparam>
        /// <param name="index">start from -1</param>
        /// <param name="args"></param>
        /// <param name="argName"></param>
        /// <param name="defaultValue"></param>
        /// <param name="canOutOfArgs">not in args</param>
        /// <returns></returns>
        public static ArgType GetArgValue<ArgType>(ref int index, string[] args, string argName, ArgType defaultValue, bool canOutOfArgs = true)
        {
            index++;
            if (args.Length > index)
            {
                Console.WriteLine("args[{0}] : {1} = {2}", index, argName, args[index]);
                var argValue = args[index];
                if (defaultValue is bool)
                {
                    argValue = Regex.IsMatch(args[index], "1|true", RegexOptions.IgnoreCase).ToString();
                }

                return (ArgType)TypeDescriptor.GetConverter(typeof(ArgType)).ConvertFromString(argValue);
            }
            else if (canOutOfArgs)
            {
                Console.WriteLine("args[{0}] : {1} = {2}", index, argName, defaultValue);
                return defaultValue;
            }
            else
            {
                throw new ArgumentException(string.Format("must set {0} at arg[{0}]", argName, index + 1), argName);
            }
        }
    }
}
