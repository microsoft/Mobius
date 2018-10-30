// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    ///  Dataset is a strongly typed collection of domain-specific objects that can be transformed
    /// in parallel using functional or relational operations.Each Dataset also has an untyped view 
    /// called a DataFrame, which is a Dataset of Row.
    /// </summary>
    public class Dataset
    {
        IDatasetProxy datasetProxy;

        internal Dataset(IDatasetProxy datasetProxy)
        {
            this.datasetProxy = datasetProxy;
        }

        /// <summary>
        /// Selects column based on the column name
        /// </summary>
        /// <param name="columnName">Name of the column</param>
        /// <returns></returns>
        public Column this[string columnName]
        {
            get { return ToDF()[columnName]; }
        }

        private DataFrame dataFrame;

        /// <summary>
        /// Converts this strongly typed collection of data to generic Dataframe. In contrast to the
        /// strongly typed objects that Dataset operations work on, a Dataframe returns generic[[Row]]
        /// objects that allow fields to be accessed by ordinal or name.
        /// </summary>
        /// <returns>DataFrame created from Dataset</returns>
        public DataFrame ToDF()
        {
            return dataFrame ?? (dataFrame = new DataFrame(datasetProxy.ToDF(), SparkContext.GetActiveSparkContext()));
        }

        /// <summary>
        /// Prints the schema to the console in a nice tree format.
        /// </summary>
        public void PrintSchema()
        {
            ToDF().ShowSchema();
        }

        /// <summary>
        /// Prints the plans (logical and physical) to the console for debugging purposes.
        /// </summary>
        /// <param name="extended"></param>
        public void Explain(bool extended)
        {
            ToDF().Explain(extended);
        }

        /// <summary>
        /// Prints the physical plan to the console for debugging purposes.
        /// </summary>
        public void Explain()
        {
            ToDF().Explain();
        }

        /// <summary>
        /// Returns all column names and their data types as an array.
        /// </summary>
        public IEnumerable<Tuple<string, string>> DTypes()
        {
            return ToDF().DTypes();
        }

        /// <summary>
        /// Returns all column names as an array.
        /// </summary>
        public IEnumerable<string> Columns()
        {
            return ToDF().Columns();
        }

        /// <summary>
        /// Displays the top 20 rows of Dataset in a tabular form. Strings more than 20 characters
        /// will be truncated, and all cells will be aligned right.
        /// </summary>
        /// <param name="numberOfRows">Number of rows - default is 20</param>
        /// <param name="truncate">Indicates if rows with more than 20 characters to be truncated</param>
        /// <param name="vertical">If set to true, prints output rows vertically (one line per column value).</param>
        public void Show(int numberOfRows = 20, int truncate = 20, bool vertical = false)
        {
            ToDF().Show(numberOfRows, truncate);
        }

        /// <summary>
        /// Prints schema
        /// </summary>
        public void ShowSchema()
        {
            ToDF().ShowSchema();
        }
    }

    /// <summary>
    /// Dataset of specific types
    /// </summary>
    /// <typeparam name="T">Type parameter</typeparam>
    public class Dataset<T> : Dataset
    {
        internal Dataset(IDatasetProxy datasetProxy): base(datasetProxy) {}

        /************************************************************
         * Would it be useful to expose methods like the following?
         * It would offer static type checking at the cost of runtime optimizations
         * because C# functionality need to execute in CLR
         ************************************************************

        public Dataset<T> Filter(Func<T, bool> func)
        {
            throw new NotImplementedException();
        }

        public Dataset<U> Map<U>(Func<T, U> mapFunc)
        {
            throw new NotImplementedException();
        }

        */
    }
}
