// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using Microsoft.Spark.CSharp.Sql;

namespace AdapterTest.Mocks
{
    public class MockRow : Row
    {

        public override int Size()
        {
            throw new NotImplementedException();
        }

        public override StructType GetSchema()
        {
            throw new NotImplementedException();
        }

        public override object Get(int i)
        {
            throw new NotImplementedException();
        }

        public override object Get(string columnName)
        {
            throw new NotImplementedException();
        }
    }
}
