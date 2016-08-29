// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Sql
{
    public class Dataset
    {
        IDatasetProxy datasetProxy;

        internal Dataset(IDatasetProxy datasetProxy)
        {
            this.datasetProxy = datasetProxy;
        }

        public Column this[string columnName]
        {
            get
            {
                throw new NotImplementedException();
            }
        }


        public DataFrame ToDF()
        {
            return new DataFrame(datasetProxy.ToDF(), SparkContext.GetActiveSparkContext());
        }

        public void PrintSchema()
        {
            throw new NotImplementedException();
        }

        public void Explain(bool extended)
        {
            throw new NotImplementedException();
        }

        public void Explain()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<Tuple<string, string>> DTypes()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> Columns()
        {
            throw new NotImplementedException();
        }

        public void Show(int numberOfRows = 20, bool truncate = true)
        {
            throw new NotImplementedException();
        }

        public void ShowSchema()
        {
            throw new NotImplementedException();
        }
    }

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
