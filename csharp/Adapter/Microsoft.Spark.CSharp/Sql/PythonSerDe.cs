// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Razorvine.Pickle;
using Razorvine.Pickle.Objects;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// Used for SerDe of Python objects
    /// </summary>
    class PythonSerDe
    {
        static PythonSerDe()
        {
            //custom picklers used in PySpark implementation
            Unpickler.registerConstructor("pyspark.sql.types", "_parse_datatype_json_string", new StringConstructor());
            Unpickler.registerConstructor("pyspark.sql.types", "_create_row_inbound_converter", new RowConstructor());
        }

        /// <summary>
        /// Unpickles objects from byte[]
        /// </summary>
        /// <param name="buffer"></param>
        /// <returns></returns>
        internal static object[] GetUnpickledObjects(byte[] buffer)
        {
            var unpickler = new Unpickler(); //not making any assumptions about the implementation and hence not a class member
            return (unpickler.loads(buffer) as object[]);
        }
    }
}
