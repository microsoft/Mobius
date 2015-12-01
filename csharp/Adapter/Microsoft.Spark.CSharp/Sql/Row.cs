// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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
        public abstract RowSchema GetSchema();

        /// <summary>
        /// Returns the value at position i.
        /// </summary>
        public abstract object Get(int i);

        /// <summary>
        /// Returns the value of a given columnName.
        /// </summary>
        public abstract object Get(string columnName);

        /// <summary>
        /// Returns the value at position i, the return value will be cast to type T.
        /// </summary>
        public T GetAs<T>(int i)
        {
            object o = Get(i);
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
            object o = Get(columnName);
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

    /// <summary>
    /// Schema of Row
    /// </summary>
    [Serializable]
    public class RowSchema
    {
        public string type;
        public List<ColumnSchema> columns;

        private readonly Dictionary<string, int> columnName2Index = new Dictionary<string, int>();

        public RowSchema(string type)
        {
            this.type = type;
            this.columns = new List<ColumnSchema>();
        }

        public RowSchema(string type, List<ColumnSchema> cols)
        {
            int index = 0;
            foreach (var col in cols)
            {
                string columnName = col.name;

                if (string.IsNullOrEmpty(columnName))
                {
                    throw new Exception(string.Format("Null column name at pos: {0}", index));
                }

                if (columnName2Index.ContainsKey(columnName))
                {
                    throw new Exception(string.Format("duplicate column name ({0}) in pos ({1}) and ({2})",
                        columnName, columnName2Index[columnName], index));
                }
                columnName2Index[columnName] = index;
                index++;
            }

            this.type = type;
            this.columns = cols;
        }

        internal int GetIndexByColumnName(string ColumnName)
        {
            if (!columnName2Index.ContainsKey(ColumnName))
            {
                throw new Exception(string.Format("unknown ColumnName: {0}", ColumnName));
            }

            return columnName2Index[ColumnName];
        }

        public override string ToString()
        {
            string result;

            if (columns.Any())
            {
                List<string> cols = new List<string>();
                foreach (var col in columns)
                {
                    cols.Add(col.ToString());
                }

                result = "{" +
                    string.Format("type: {0}, columns: [{1}]", type, string.Join(", ", cols.ToArray())) +
                    "}";
            }
            else
            {
                result = type;
            }

            return result;
        }

        internal static RowSchema ParseRowSchemaFromJson(string json)
        {
            JObject joType = JObject.Parse(json);
            string type = joType["type"].ToString();

            List<ColumnSchema> columns = new List<ColumnSchema>();
            List<JToken> jtFields = joType["fields"].Children().ToList();
            foreach (JToken jtField in jtFields)
            {
                ColumnSchema col = ColumnSchema.ParseColumnSchemaFromJson(jtField.ToString());
                columns.Add(col);
            }

            return new RowSchema(type, columns);
        }

    }

    /// <summary>
    /// Schema for column
    /// </summary>
    [Serializable]
    public class ColumnSchema
    {
        public string name;
        public RowSchema type;
        public bool nullable;

        public override string ToString()
        {
            string str = string.Format("name: {0}, type: {1}, nullable: {2}", name, type, nullable);
            return "{" + str + "}";
        }

        internal static ColumnSchema ParseColumnSchemaFromJson(string json)
        {
            ColumnSchema col = new ColumnSchema();
            JObject joField = JObject.Parse(json);
            col.name = joField["name"].ToString();
            col.nullable = (bool)(joField["nullable"]);

            JToken jtType = joField["type"];
            if (jtType.Type == JTokenType.String)
            {
                col.type = new RowSchema(joField["type"].ToString());
            }
            else
            {
                col.type = RowSchema.ParseRowSchemaFromJson(joField["type"].ToString());
            }

            return col;
        }
    }

    [Serializable]
    internal class RowImpl : Row
    {
        private readonly RowSchema schema;
        private readonly object[] values;

        private readonly int columnCount;

        internal RowImpl(object data, RowSchema schema)
        {
            if (data is object[])
            {
                values = data as object[];
            }
            else if (data is List<object>)
            {
                values = (data as List<object>).ToArray();
            }
            else
            {
                throw new Exception(string.Format("unexpeted type {0}", data.GetType()));
            }

            this.schema = schema;

            columnCount = values.Count();
            int schemaColumnCount = this.schema.columns.Count();
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

        public override RowSchema GetSchema()
        {
            return schema;
        }

        public override object Get(int i)
        {
            if (i >= columnCount)
            {
                throw new Exception(string.Format("i ({0}) >= columnCount ({1})", i, columnCount));
            }

            return values[i];
        }

        public override object Get(string columnName)
        {
            int index = schema.GetIndexByColumnName(columnName);
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
            foreach (var col in schema.columns)
            {
                if (col.type.columns.Any()) // this column itself is a sub-row
                {
                    object value = values[index];
                    if (value != null)
                    {
                        RowImpl subRow = new RowImpl(values[index], col.type);
                        values[index] = subRow;
                    }
                }

                index++;
            }
        }
    }
}
