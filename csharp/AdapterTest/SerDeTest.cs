// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Sql;
using NUnit.Framework;
using Razorvine.Pickle;

namespace AdapterTest
{
    /// <summary>
    /// Validates Serialization and Deserialization of data types between JVM & CLR.
    /// </summary>
    [TestFixture]
    class SerDeTest
    {
        [Test]
        public void TestToBytes()
        {
            // test ToBytes from bool variable
            Assert.AreEqual(new byte[] {1}, SerDe.ToBytes(true));
            Assert.AreEqual(new byte[] {0}, SerDe.ToBytes(false));

            // test ToBytes from string variable
            Assert.AreEqual(Encoding.UTF8.GetBytes("hello world!"), SerDe.ToBytes("hello world!"));

            // test ToBytes from int
            Assert.AreEqual(new byte[] {0, 0, 0, 0}, SerDe.ToBytes(0));
            Assert.AreEqual(new byte[] {0, 0, 0, 5}, SerDe.ToBytes(5));
            Assert.AreEqual(new byte[] {0, 0, 0, byte.MaxValue}, SerDe.ToBytes(byte.MaxValue));
            Assert.AreEqual(new byte[] {0, 0, 0, byte.MinValue}, SerDe.ToBytes(byte.MinValue));

            // test ToBytes from long variable
            long longVar = Convert.ToInt64(int.MaxValue) + 1;
            var byteRepresentationofLongVar = BitConverter.GetBytes(longVar);
            Array.Reverse(byteRepresentationofLongVar);
            Assert.AreEqual(byteRepresentationofLongVar, SerDe.ToBytes(longVar));

            // test ToBytes from double variable
            var byteRepresentationofDoubleVar = BitConverter.GetBytes(Double.MaxValue);
            Array.Reverse(byteRepresentationofDoubleVar);
            Assert.AreEqual(byteRepresentationofDoubleVar, SerDe.ToBytes(Double.MaxValue));
        }

        [Test]
        public void TestReadDouble()
        {
            byte[] byteRepresentationofDoubleVar = BitConverter.GetBytes(Math.PI);
            Array.Reverse(byteRepresentationofDoubleVar);
            Assert.AreEqual(Math.PI, SerDe.ReadDouble(new MemoryStream(byteRepresentationofDoubleVar)), 1e-14);
        }

        [Test]
        public void TestWriteDouble()
        {
            var ms = new MemoryStream();
            SerDe.Write(ms, Math.PI);
            ms.Position = 0;
            Assert.AreEqual(Math.PI, SerDe.ReadDouble(ms), 1e-14);
        }

        [Test]
        public void TestReadObjectId()
        {
            // test when objectId is null
            Assert.IsNull(SerDe.ReadObjectId(new MemoryStream(new byte[] { (byte)'n' })));

            // test when invalid type is passed
            Assert.IsNull(SerDe.ReadObjectId(new MemoryStream(new byte[] { (byte)'a' })));

            // test when objectId is not null
            var ms = new MemoryStream();
            SerDe.Write(ms, (byte)'j');
            var objectId = Guid.NewGuid().ToString();
            SerDe.Write(ms, objectId);

            // start to read
            ms.Position = 0;
            Assert.AreEqual(objectId, SerDe.ReadObjectId(ms));
        }

        [Test]
        public void TestReadBytes()
        {
            // test when invalid length given
            Assert.Throws<ArgumentOutOfRangeException>(() => SerDe.ReadBytes(new MemoryStream(), -1));

            // test read NULL value
            var ms = new MemoryStream();
            SerDe.Write(ms, (int)SpecialLengths.NULL);
            ms.Position = 0;
            Assert.IsNull(SerDe.ReadBytes(ms));
        }

        [Test]
        public void TestSerDeWithPythonSerDe()
        {
            const int expectedCount = 5;
            using (var ms = new MemoryStream())
            {
                new StructTypePickler().Register();
                new RowPickler().Register();
                var pickler = new Pickler();
                for (int i = 0; i < expectedCount; i++)
                {
                    var pickleBytes = pickler.dumps(new[] { BuildRow(i) });
                    SerDe.Write(ms, pickleBytes.Length);
                    SerDe.Write(ms, pickleBytes);
                }

                SerDe.Write(ms, (int)SpecialLengths.END_OF_STREAM);
                ms.Flush();

                ms.Position = 0;
                int count = 0;
                while (true)
                {
                    byte[] outBuffer = null;
                    int length = SerDe.ReadInt(ms);
                    if (length > 0)
                    {
                        outBuffer = SerDe.ReadBytes(ms, length);
                    }
                    else if (length == (int)SpecialLengths.END_OF_STREAM)
                    {
                        break;
                    }

                    var unpickledObjs = PythonSerDe.GetUnpickledObjects(outBuffer);
                    var rows = unpickledObjs.Select(item => (item as RowConstructor).GetRow()).ToList();
                    Assert.AreEqual(1, rows.Count);
                    Assert.AreEqual(count++, rows[0].Get("age"));
                }
                Assert.AreEqual(expectedCount, count);
            }
        }

        internal Row BuildRow(int seq)
        {
            const string jsonSchema = @"
                {
                  ""type"" : ""struct"",
                  ""fields"" : [{
                    ""name"" : ""age"",
                    ""type"" : ""long"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""id"",
                    ""type"" : ""string"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  }, {
                    ""name"" : ""name"",
                    ""type"" : ""string"",
                    ""nullable"" : true,
                    ""metadata"" : { }
                  } ]
                }";

            return new RowImpl(new object[] { seq, "id " + seq, "name" + seq }, DataType.ParseDataTypeFromJson(jsonSchema) as StructType);
        }
    }

    /// <summary>
    /// Used to pickle StructType objects
    /// Reference: StructTypePickler from https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/python.scala#L240
    /// </summary>
    internal class StructTypePickler : IObjectPickler
    {

        private const string module = "pyspark.sql.types";

        public void Register()
        {
            Pickler.registerCustomPickler(this.GetType(), this);
            Pickler.registerCustomPickler(typeof(StructType), this);
        }

        public void pickle(object o, Stream stream, Pickler currentPickler)
        {
            var schema = o as StructType;
            if (schema == null)
            {
                throw new InvalidOperationException(this.GetType().Name + " only accepts 'StructType' type objects.");
            }

            SerDe.Write(stream, Opcodes.GLOBAL);
            SerDe.Write(stream, Encoding.UTF8.GetBytes(module + "\n" + "_parse_datatype_json_string" + "\n"));
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
        private const string module = "pyspark.sql.types";

        public void Register()
        {
            Pickler.registerCustomPickler(this.GetType(), this);
            Pickler.registerCustomPickler(typeof(Row), this);
            Pickler.registerCustomPickler(typeof(RowImpl), this);
        }

        public void pickle(object o, Stream stream, Pickler currentPickler)
        {
            if (o.Equals(this))
            {
                SerDe.Write(stream, Opcodes.GLOBAL);
                SerDe.Write(stream, Encoding.UTF8.GetBytes(module + "\n" + "_create_row_inbound_converter" + "\n"));
            }
            else
            {
                var row = o as Row;
                if (row == null)
                {
                    throw new InvalidOperationException(this.GetType().Name + " only accepts 'Row' type objects.");
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
