// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;

namespace Microsoft.Spark.CSharp.Sql
{
    public class Builder
    {
        Dictionary<string, string> options = new Dictionary<string, string>();

        internal Builder() { }

        public Builder Master(string master)
        {
            Config("spark.master", master);
            return this;
        }

        public Builder AppName(string appName)
        {
            Config("spark.app.name", appName);
            return this;
        }

        public Builder Config(string key, string value)
        {
            options[key] = value;
            return this;
        }

        public Builder Config(string key, bool value)
        {
            options[key] = value.ToString();
            return this;
        }

        public Builder Config(string key, double value)
        {
            options[key] = value.ToString();
            return this;
        }

        public Builder Config(string key, long value)
        {
            options[key] = value.ToString();
            return this;
        }

        public Builder Config(SparkConf conf)
        {
            foreach (var keyValuePair in conf.GetAll())
            {
                options[keyValuePair.Key] = keyValuePair.Value;
            }

            return this;
        }

        public Builder EnableHiveSupport()
        {
            return Config("spark.sql.catalogImplementation", "hive");
        }


        public SparkSession GetOrCreate()
        {
            var sparkConf = new SparkConf();
            foreach (var option in options)
            {
                sparkConf.Set(option.Key, option.Value);
            }
            var sparkContext = SparkContext.GetOrCreate(sparkConf);
            return SqlContext.GetOrCreate(sparkContext).SparkSession;
        }
    }
}
