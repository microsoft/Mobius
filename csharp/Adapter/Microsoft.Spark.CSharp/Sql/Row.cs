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

        /// <summary>
        /// Number of elements in the Row.
        /// </summary>
        /// <returns>elements count in this row</returns>
        public abstract int Size();

        /// <summary>
        /// Schema for the row.
        /// </summary>
        public abstract StructType GetSchema();

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
        public dynamic[] Values { get { return values; } }
        private readonly dynamic[] values;

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
                values = data as dynamic[];
            }
            else if (data is List<dynamic>)
            {
                values = (data as List<dynamic>).ToArray();
            }
            else
            {
                throw new Exception(string.Format("unexpeted type {0}", data.GetType()));
            }

            this.schema = schema;
            
            columnCount = values.Count();
            int schemaColumnCount = this.schema.Fields.Count();
            if (columnCount != schemaColumnCount)
            {
                throw new Exception(string.Format("column count inferred from data ({0}) and schema ({1}) mismatch", columnCount, schemaColumnCount));
            }

            Initialize();
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
            if (i >= columnCount)
            {
                throw new Exception(string.Format("i ({0}) >= columnCount ({1})", i, columnCount));
            }

            return values[i];
        }

        public override dynamic Get(string columnName)
        {
            int index = schema.Fields.FindIndex(f => f.Name == columnName); // case sensitive
            return Get(index);
        }

        public override string ToString()
        {
            List<string> cols = new List<string>();
            foreach (var item in values)
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


        private void Initialize()
        {

            int index = 0;
            foreach (var field in schema.Fields)
            {
                if (field.DataType is ArrayType)
                {
                    Func<DataType, int, StructType> convertArrayTypeToStructTypeFunc = (dataType, length) =>
                                                                                      {
                                                                                          StructField[] fields = new StructField[length];
                                                                                          for(int i = 0; i < length ; i++)
                                                                                          {
                                                                                              fields[i] = new StructField(string.Format("_array_{0}", i), dataType);
                                                                                          }
                                                                                          return new StructType(fields);
                                                                                      };
                    var elementType = (field.DataType as ArrayType).ElementType;

                    // Note: When creating object from json, PySpark converts Json array to Python List (https://github.com/apache/spark/blob/branch-1.4/python/pyspark/sql/types.py, _create_cls(dataType)), 
                    // then Pyrolite unpickler converts Python List to C# ArrayList (https://github.com/irmen/Pyrolite/blob/v4.10/README.txt). So values[index] should be of type ArrayList;
                    // In case Python changes its implementation, which means value is not of type ArrayList, try cast to object[] because Pyrolite unpickler convert Python Tuple to C# object[].
                    object[] valueOfArray = values[index] is ArrayList ? (values[index] as ArrayList).ToArray() : values[index] as object[];
                    if (valueOfArray == null)
                    {
                        throw new ArgumentException("Cannot parse data of ArrayType: " + field.Name);
                    }

                    values[index] = new RowImpl(valueOfArray, elementType as StructType ?? convertArrayTypeToStructTypeFunc(elementType, valueOfArray.Length)).values;
                }
                else if (field.DataType is MapType)
                {
                    //TODO
                    throw new NotImplementedException();
                }
                else if (field.DataType is StructType)
                {
                    dynamic value = values[index];
                    if (value != null)
                    {
                        var subRow = new RowImpl(values[index], field.DataType as StructType);
                        values[index] = subRow;
                    }
                }
                else if (field.DataType is DecimalType)
                {
                    //TODO
                    throw new NotImplementedException();
                }
                else if (field.DataType is DateType)
                {
                    //TODO
                    throw new NotImplementedException();
                }
                else if (field.DataType is StringType)
                {
                    if (values[index] != null) values[index] = values[index].ToString();
                }
                else
                {
                    values[index] = values[index];
                }
                index++;
            }
        }
    }

}
