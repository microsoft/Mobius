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
        void LogDebug(string messageFormat, params object[] messageParameters);
        void LogInfo(string message);
        void LogInfo(string messageFormat, params object[] messageParameters);
        void LogWarn(string message);
        void LogWarn(string messageFormat, params object[] messageParameters);
        void LogFatal(string message);
        void LogFatal(string messageFormat, params object[] messageParameters);
        void LogError(string message);
        void LogError(string messageFormat, params object[] messageParameters);
        void LogException(Exception e);
    }
}
