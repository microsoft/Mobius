using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace testKeyValueStream
{
    [Serializable]
    abstract class ParseKeyValuePairBase<ValueType>
    {
        protected readonly long valueArrayElements;

        protected readonly bool needPrintMessage;

        public ParseKeyValuePairBase(long valueElementCount = 0, bool needPrintMessage = true)
        {
            this.valueArrayElements = valueElementCount;
            this.needPrintMessage = needPrintMessage;
        }

        public virtual void Log(string message)
        {
            Console.WriteLine("{0} {1} : {2}", TestUtils.NowMilli, this.GetType().Name, message);
        }

        public virtual KeyValuePair<string, ValueType> Parse(string line)
        {
            throw new NotImplementedException();
        }

        protected static KeyValuePair<string, int[]> Parse(string line, long elements)
        {
            // Log("received line : " + line);
            var match = Regex.Match(line, @"^.*tick\s*=\s*(?<Key>\d{9})(?<Value>\d+)");
            var key = match.Groups["Key"].Value;
            var valueSet = Regex.Matches(match.Groups["Value"].Value, "\\d");
            var values = new int[elements];
            var n = Math.Min(valueSet.Count, values.Length);
            for (var k = 0; k < n; k++)
            {
                values[k] = int.Parse(valueSet[k].Value);
            }

            return new KeyValuePair<string, int[]>(key, values);
        }

        protected void Print(KeyValuePair<string, int[]> kv)
        {
            if (needPrintMessage)
            {
                Log(string.Format("key = {0} , value{1}", kv.Key, TestUtils.ArrayToText(kv.Value)));
            }
        }
    }

    [Serializable]
    class ParseKeyValueArray : ParseKeyValuePairBase<int[]>
    {
        public ParseKeyValueArray(long valueElementCount = 0, bool needPrintMessage = true) : base(valueElementCount, needPrintMessage) { }
        public override KeyValuePair<string, int[]> Parse(string line)
        {
            var kv = Parse(line, this.valueArrayElements);
            Print(kv);
            return kv;
        }
    }

    [Serializable]
    class ParseKeyValueUnevenArray : ParseKeyValueArray
    {
        private static Random random = new Random(DateTime.Now.Second);

        public ParseKeyValueUnevenArray(long valueElementCount = 0, bool needPrintMessage = true) : base(valueElementCount, needPrintMessage) { }

        public override KeyValuePair<string, int[]> Parse(string line)
        {
            var kv = Parse(line);
            var values = kv.Value.ToList();
            int removeCount = random.Next() % (values.Count + 1);
            values.RemoveRange(0, removeCount);
            var pair = new KeyValuePair<string, int[]>(kv.Key, values.ToArray());
            Print(pair);
            return pair;
        }
    }

    [Serializable]
    class ParseKeyValue : ParseKeyValuePairBase<int>
    {
        public ParseKeyValue(long valueArrayElements = 0, bool needPrintMessage = true) : base(valueArrayElements, needPrintMessage) { }

        public override KeyValuePair<string, int> Parse(string line)
        {
            var kv = Parse(line, 1);
            Print(kv);
            return new KeyValuePair<string, int>(kv.Key, kv.Value[0]);
        }
    }
}
