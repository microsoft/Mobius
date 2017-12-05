// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// Functionality for working with missing data in DataFrames.
    /// </summary>
    public class DataFrameNaFunctions
    {
        private readonly IDataFrameNaFunctionsProxy dataFrameNaFunctionsProxy;
        private readonly DataFrame df;
        private readonly SparkContext sparkContext;

        internal DataFrameNaFunctions(IDataFrameNaFunctionsProxy dataFrameNaFunctionsProxy, DataFrame df, SparkContext sparkContext)
        {
            this.dataFrameNaFunctionsProxy = dataFrameNaFunctionsProxy;
            this.df = df;
            this.sparkContext = sparkContext;
        }

        /// <summary>
        /// Returns a new DataFrame that drops rows containing any null values.
        /// </summary>
        public DataFrame Drop()
        {
            return Drop("any", df.Columns().ToArray());
        }

        /// <summary>
        /// Returns a new DataFrame that drops rows containing null values.
        /// 
        /// If `how` is "any", then drop rows containing any null values.
        /// If `how` is "all", then drop rows only if every column is null for that row.
        /// </summary>
        public DataFrame Drop(string how)
        {
            return Drop(how, df.Columns().ToArray());
        }

        /// <summary>
        /// Returns a new [[DataFrame]] that drops rows containing null values
        /// in the specified columns.
        /// 
        /// If `how` is "any", then drop rows containing any null values in the specified columns.
        /// If `how` is "all", then drop rows only if every specified column is null for that row.
        /// </summary>
        public DataFrame Drop(string how, string[] cols)
        {
            if ("any".Equals(how, StringComparison.InvariantCultureIgnoreCase))
            {
                return Drop(cols.Length, cols);
            }
            if ("all".Equals(how, StringComparison.InvariantCultureIgnoreCase))
            {
                return Drop(1, cols);
            }
            throw new ArgumentException(string.Format("how {0} must be 'any' or 'all'", how));
        }

        /// <summary>
        /// Returns a new DataFrame that drops rows containing any null values
        /// in the specified columns.
        /// </summary>
        public DataFrame Drop(string[] cols)
        {
            if (cols == null || cols.Length == 0)
            {
                return df;
            }

            return Drop(cols.Length, cols);
        }

        /// <summary>
        /// Returns a new DataFrame that drops rows containing less than `minNonNulls` non-null values.
        /// </summary>
        public DataFrame Drop(int minNonNulls)
        {
            return Drop(minNonNulls, df.Columns().ToArray());
        }

        /// <summary>
        /// Returns a new DataFrame that drops rows containing less than `minNonNulls` non-null values
        /// values in the specified columns.
        /// </summary>
        public DataFrame Drop(int minNonNulls, string[] cols)
        {
            return new DataFrame(dataFrameNaFunctionsProxy.Drop(minNonNulls, cols), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame that replaces null values in numeric columns with `value`.
        /// </summary>
        public DataFrame Fill(double value)
        {
            return new DataFrame(dataFrameNaFunctionsProxy.Fill(value, df.Columns().ToArray()), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame that replaces null values in string columns with `value`.
        /// </summary>
        public DataFrame Fill(string value)
        {
            return new DataFrame(dataFrameNaFunctionsProxy.Fill(value, df.Columns().ToArray()), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame that replaces null values in specified numeric columns.
        /// If a specified column is not a numeric column, it is ignored.
        /// </summary>
        public DataFrame Fill(double value, string[] cols)
        {
            return new DataFrame(dataFrameNaFunctionsProxy.Fill(value, cols), sparkContext);
        }

        /// <summary>
        /// Returns a new DataFrame that replaces null values in specified string columns.
        /// If a specified column is not a numeric column, it is ignored.
        /// </summary>
        public DataFrame Fill(string value, string[] cols)
        {
            return new DataFrame(dataFrameNaFunctionsProxy.Fill(value, cols), sparkContext);
        }

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// 
        /// Key and value of `replacement` map must have the same type, and can only be doubles or strings.
        /// The value must be of the following type: `Integer`, `Long`, `Float`, `Double`, `String`.
        /// 
        /// For example, the following replaces null values in column "A" with string "unknown", and
        /// null values in column "B" with numeric value 1.0.
        /// 
        ///   import com.google.common.collect.ImmutableMap;
        ///   df.na.fill(ImmutableMap.of("A", "unknown", "B", 1.0));
        /// </summary>
        public DataFrame Fill(Dictionary<string, object> valueMap)
        {
            return new DataFrame(dataFrameNaFunctionsProxy.Fill(valueMap), sparkContext);
        }

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// 
        /// Key and value of `replacement` map must have the same type, and can only be doubles or strings.
        /// If `col` is "*", then the replacement is applied on all string columns or numeric columns.
        /// 
        /// Example:
        /// 
        ///   import com.google.common.collect.ImmutableMap;
        ///   // Replaces all occurrences of 1.0 with 2.0 in column "height".
        ///   df.replace("height", ImmutableMap.of(1.0, 2.0));
        ///   
        ///   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "name".
        ///   df.replace("name", ImmutableMap.of("UNKNOWN", "unnamed"));
        ///   // Replaces all occurrences of "UNKNOWN" with "unnamed" in all string columns.
        ///   df.replace("*", ImmutableMap.of("UNKNOWN", "unnamed"));
        /// </summary>
        public DataFrame Replace<T>(string col, Dictionary<T, T> replacement)
        {
            return new DataFrame(dataFrameNaFunctionsProxy.Replace(col, replacement), sparkContext);
        }

        /// <summary>
        /// Replaces values matching keys in `replacement` map with the corresponding values.
        /// 
        /// Key and value of `replacement` map must have the same type, and can only be doubles or strings.
        /// If `col` is "*", then the replacement is applied on all string columns or numeric columns.
        /// 
        /// Example:
        /// 
        ///   import com.google.common.collect.ImmutableMap;
        ///   // Replaces all occurrences of 1.0 with 2.0 in column "height" and "weight".
        ///   df.replace(new String[] {"height", "weight"}, ImmutableMap.of(1.0, 2.0));
        ///  
        ///   // Replaces all occurrences of "UNKNOWN" with "unnamed" in column "firstname" and "lastname".
        ///   df.replace(new String[] {"firstname", "lastname"}, ImmutableMap.of("UNKNOWN", "unnamed"));
        /// </summary>
        public DataFrame Replace<T>(string[] cols, Dictionary<T, T> replacement)
        {
            return new DataFrame(dataFrameNaFunctionsProxy.Replace(cols, replacement), sparkContext);
        }
    }
}
