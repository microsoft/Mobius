using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Spark.CSharp.Sql;

namespace Microsoft.Spark.CSharp.Proxy
{
    interface ISparkCLRProxy
    {
        ISparkContextProxy SparkContextProxy { get; }
        ISparkConfProxy CreateSparkConf(bool loadDefaults = true);
        ISparkContextProxy CreateSparkContext(ISparkConfProxy conf);
        IStructFieldProxy CreateStructField(string name, string dataType, bool isNullable);
        IStructTypeProxy CreateStructType(List<StructField> fields);
    }
}
