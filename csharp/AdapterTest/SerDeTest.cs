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
using Tests.Common;

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
                    var pickleBytes = pickler.dumps(new[] { RowHelper.BuildRowForBasicSchema(i) });
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
    }
}
