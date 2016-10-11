// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy;

namespace AdapterTest.Mocks
{
    internal class MockSparkConfProxy : ISparkConfProxy
    {
        internal Dictionary<string, string> stringConfDictionary = new Dictionary<string, string>();
        private Dictionary<string, int> intConfDictionary = new Dictionary<string, int>();

        internal const string MockMasterKey = "mockmaster";
        public void SetMaster(string master)
        {
            stringConfDictionary["mockmaster"] = master;
        }

        internal const string MockAppNameKey = "mockappName";
        public void SetAppName(string appName)
        {
            stringConfDictionary["mockappName"] = appName;
        }

        internal const string MockHomeKey = "mockhome";
        public void SetSparkHome(string sparkHome)
        {
            stringConfDictionary["mockhome"] = sparkHome;
        }

        public void Set(string key, string value)
        {
            stringConfDictionary[key] = value;
            int i;
            if (int.TryParse(value, out i))
            {
                intConfDictionary[key] = i;
            }
        }

        public int GetInt(string key, int defaultValue)
        {
            if (intConfDictionary.ContainsKey(key))
            {
                return intConfDictionary[key];
            }
            return defaultValue;
        }

        public string Get(string key, string defaultValue)
        {
            if (stringConfDictionary.ContainsKey(key))
            {
                return stringConfDictionary[key];
            }
            return defaultValue;
        }

        public string GetSparkConfAsString()
        {
            throw new NotImplementedException();
        }
    }
}
