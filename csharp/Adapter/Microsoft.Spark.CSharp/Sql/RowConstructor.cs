// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Razorvine.Pickle;

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// Used by Unpickler to unpickle pickled objects. It is also used to construct a Row (C# representation of pickled objects).
    /// Note this implementation is not ThreadSafe. Collect or RDD conversion where unpickling is done is not expected to be multithreaded.
    /// </summary>
    public class RowConstructor : IObjectConstructor
    {
        //construction is done using multiple RowConstructor objects with multiple calls to construct method with first call always
        //done using schema as the args. Using static variables to store the schema and if it is set (that is if the first call is made)
        /// <summary>
        /// Schema of the DataFrame currently being processed
        /// </summary>
        private static string currentSchema; 

        /// <summary>
        /// Indicates if Schema is already set during construction of this type
        /// </summary>
        private static bool isCurrentSchemaSet;

        /// <summary>
        /// Arguments used to construct this typ
        /// </summary>
        internal object[] Values;

        /// <summary>
        /// Schema of the values
        /// </summary>
        internal string Schema;

        public override string ToString()
        {
            return string.Format("{{{0}}}", string.Join(",", Values));
        }

        /// <summary>
        /// Used by Unpickler - do not use to construct Row. Use GetRow() method
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public object construct(object[] args)
        {
            if (!isCurrentSchemaSet) //first call always includes schema and schema is always in args[0]
            {
                currentSchema = args[0].ToString();
                isCurrentSchemaSet = true;
            }

            return new RowConstructor { Values = args, Schema = currentSchema };
        }

        /// <summary>
        /// Used to construct a Row
        /// </summary>
        /// <returns></returns>
        public Row GetRow()
        {
            var schema = DataType.ParseDataTypeFromJson(Schema) as StructType;
            var row = new RowImpl(GetValues(Values), schema);

            //Resetting schema here so that rows from multiple DataFrames can be processed in the same AppDomain
            //next row will have schema - so resetting is fine
            isCurrentSchemaSet = false;
            currentSchema = null;

            return row;
        }

        //removes objects of type RowConstructor and replacing them with actual values
        private object[] GetValues(object[] arguments)
        {
            var values = new object[arguments.Length];
            int i = 0;
            foreach (var argument in arguments)
            {
                if (argument != null && argument.GetType() == typeof(RowConstructor))
                {
                    values[i++] = (argument as RowConstructor).Values;
                }
                else
                {
                    values[i++] = argument;
                }

            }

            return values;
        }
    }
}
