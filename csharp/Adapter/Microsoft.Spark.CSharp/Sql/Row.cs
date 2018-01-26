// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.CSharp.Services;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    ///  Represents one row of output from a relational operator.
    /// </summary>
    [Serializable]
    public abstract class Row
    {
        [NonSerialized]
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(Row));

	    public abstract dynamic[] Values { get; }

		/// <summary>
		/// Number of elements in the Row.
		/// </summary>
		/// <returns>elements count in this row</returns>
		public abstract int Size();

        /// <summary>
        /// Schema for the row.
        /// </summary>
        public abstract StructType GetSchema();

	    public virtual void ResetValues(dynamic[] values)
	    {
		    throw new NotImplementedException();
	    }

        /// <summary>
        /// Returns the value at position i.
        /// </summary>
        public abstract dynamic Get(int i);

        /// <summary>
        /// Returns the value of a given columnName.
        /// </summary>
        public abstract dynamic Get(string columnName);

        /// <summary>
        /// Returns the value at position i, the return value will be cast to type T.
        /// </summary>
        public T GetAs<T>(int i)
        {
            dynamic o = Get(i);
            try
            {
                T result = (T)o;
                return result;
            }
            catch
            {
                logger.LogError(string.Format("type convertion failed from {0} to {1}", o.GetType(), typeof(T)));
                throw;
            }
        }

        /// <summary>
        /// Returns the value of a given columnName, the return value will be cast to type T.
        /// </summary>
        public T GetAs<T>(string columnName)
        {
            dynamic o = Get(columnName);
            try
            {
                T result = (T)o;
                return result;
            }
            catch
            {
                logger.LogError(string.Format("type convertion failed from {0} to {1}", o.GetType(), typeof(T)));
                throw;
            }
        }
    }

    [Serializable]
    internal class RowImpl : Row
    {
        private readonly StructType schema;

	    public override dynamic[] Values
	    {
		    get
		    {
			    if (!valuesConverted)
			    {
				    schema.ConvertPickleObjects(rawValues,rawValues);
				    valuesConverted = true;
			    }
			    return rawValues;
		    }
	    }

        private dynamic[] rawValues;
	    private bool valuesConverted = false;

        private readonly int columnCount;

        public object this[int index]
        {
            get
            {
                return Get(index);
            }
        }
        internal RowImpl(dynamic data, StructType schema)
        {
            if (data is dynamic[])
            {
				rawValues = data as dynamic[];
            }
            else if (data is List<dynamic>)
            {
				rawValues = (data as List<dynamic>).ToArray();
            }
            else
            {
                throw new Exception(string.Format("unexpeted type {0}", data.GetType()));
            }

            this.schema = schema;
            
            columnCount = rawValues.Length;
            int schemaColumnCount = this.schema.Fields.Count;
            if (columnCount != schemaColumnCount)
            {
                throw new Exception(string.Format("column count inferred from data ({0}) and schema ({1}) mismatch", columnCount, schemaColumnCount));
            }
        }

	    public override void ResetValues(dynamic[] values)
	    {
			if (columnCount != values.Length)
			{
				throw new ArgumentException("column count inferred from data and schema mismatch");
			}
			rawValues = values;
		    valuesConverted = false;
		}

	    public override int Size()
        {
            return columnCount;
        }

        public override StructType GetSchema()
        {
            return schema;
        }

        public override dynamic Get(int i)
        {
	        if (i >= 0 && i < columnCount) return Values[i];
            if (i >= columnCount)
            {
                throw new Exception(string.Format("i ({0}) >= columnCount ({1})", i, columnCount));
            }
            else
            {
                throw new Exception(string.Format("i ({0}) < 0", i));
            }
        }

        public override dynamic Get(string columnName)
        {
            int index = schema.Fields.FindIndex(f => f.Name == columnName); // case sensitive
            return Get(index);
        }

        public override string ToString()
        {
            List<string> cols = new List<string>();
            foreach (var item in Values)
            {
                if (item != null)
                {
                    cols.Add(item.ToString());
                }
                else
                {
                    cols.Add(string.Empty);
                }
            }

            return string.Format("[{0}]", string.Join(",", cols.ToArray()));
        }
        
    }

}
