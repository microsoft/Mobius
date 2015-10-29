using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Services
{
    public interface ILoggerService
    {
        ILoggerService GetLoggerInstance(Type type);
        void LogDebug(string message);
        void LogInfo(string message);
        void LogWarn(string message);
        void LogFatal(string message);
        void LogError(string message);
        void LogException(Exception e);
    }
}
