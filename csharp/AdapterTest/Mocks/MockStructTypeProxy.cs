// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Proxy;

namespace AdapterTest.Mocks
{
    class MockStructTypeProxy : IStructTypeProxy
    {
        private string mockJson;

        internal MockStructTypeProxy(string json)
        {
            mockJson = json;
        }

        public List<IStructFieldProxy> GetStructTypeFields()
        {
            throw new NotImplementedException();
        }

        public string ToJson()
        {
            return mockJson;
        }
    }
}
