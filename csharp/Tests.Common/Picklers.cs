using System;
using System.IO;
using System.Text;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Sql;
using Razorvine.Pickle;

namespace Tests.Common
{
    /// <summary>
    /// Used to pickle StructType objects
    /// Reference: StructTypePickler from https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/python.scala#L240
    /// </summary>
    internal class StructTypePickler : IObjectPickler
    {
        private const string Module = "pyspark.sql.types";

        public void Register()
        {
            Pickler.registerCustomPickler(GetType(), this);
            Pickler.registerCustomPickler(typeof(StructType), this);
        }

        public void pickle(object o, Stream stream, Pickler currentPickler)
        {
            var schema = o as StructType;
            if (schema == null)
            {
                throw new InvalidOperationException(GetType().Name + " only accepts 'StructType' type objects.");
            }

            SerDe.Write(stream, Opcodes.GLOBAL);
            SerDe.Write(stream, Encoding.UTF8.GetBytes(Module + "\n" + "_parse_datatype_json_string" + "\n"));
            currentPickler.save(schema.Json);
            SerDe.Write(stream, Opcodes.TUPLE1);
            SerDe.Write(stream, Opcodes.REDUCE);
        }
    }

    /// <summary>
    /// Used to pickle Row objects
    /// Reference: RowPickler from https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/python.scala#L261
    /// </summary>
    internal class RowPickler : IObjectPickler
    {
        private const string Module = "pyspark.sql.types";

        public void Register()
        {
            Pickler.registerCustomPickler(GetType(), this);
            Pickler.registerCustomPickler(typeof(Row), this);
            Pickler.registerCustomPickler(typeof(RowImpl), this);
        }

        public void pickle(object o, Stream stream, Pickler currentPickler)
        {
            if (o.Equals(this))
            {
                SerDe.Write(stream, Opcodes.GLOBAL);
                SerDe.Write(stream, Encoding.UTF8.GetBytes(Module + "\n" + "_create_row_inbound_converter" + "\n"));
            }
            else
            {
                var row = o as Row;
                if (row == null)
                {
                    throw new InvalidOperationException(GetType().Name + " only accepts 'Row' type objects.");
                }

                currentPickler.save(this);
                currentPickler.save(row.GetSchema());
                SerDe.Write(stream, Opcodes.TUPLE1);
                SerDe.Write(stream, Opcodes.REDUCE);

                SerDe.Write(stream, Opcodes.MARK);

                var i = 0;
                while (i < row.Size())
                {
                    currentPickler.save(row.Get(i));
                    i++;
                }

                SerDe.Write(stream, Opcodes.TUPLE);
                SerDe.Write(stream, Opcodes.REDUCE);
            }
        }
    }
}
