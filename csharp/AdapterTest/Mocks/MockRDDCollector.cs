using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Core;

namespace AdapterTest.Mocks
{
    class MockRDDCollector : IRDDCollector
    {
        public IEnumerable<dynamic> Collect(int port, SerializedMode serializedMode, Type type)
        {
            throw new NotImplementedException();
        }
    }
}
