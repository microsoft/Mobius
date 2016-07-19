using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp
{
    public interface IScriptEngine
    {
        ScriptResult Execute(string code);
    }
}
