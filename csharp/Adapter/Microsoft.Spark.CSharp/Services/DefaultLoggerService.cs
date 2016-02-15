using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Services
{
    /// <summary>
    /// This logger service will be used if the C# driver app did not configure a logger.
    /// Right now it just prints out the messages to Console
    /// </summary>
    public class DefaultLoggerService : ILoggerService
    {
        internal readonly static DefaultLoggerService Instance = new DefaultLoggerService(typeof (Type));
        public ILoggerService GetLoggerInstance(Type type)
        {
            return new DefaultLoggerService(type);
        }

        private readonly Type type;
        private DefaultLoggerService(Type t)
        {
            type = t;
        }
        
        public void LogDebug(string message)
        {
            Log("Debug", message);
        }

        public void LogDebug(string messageFormat, params object[] messageParameters)
        {
            Log("Debug", string.Format(messageFormat, messageParameters));
        }

        public void LogInfo(string message)
        {
            Log("Info", message);
        }

        public void LogInfo(string messageFormat, params object[] messageParameters)
        {
            Log("Info", string.Format(messageFormat, messageParameters));
        }

        public void LogWarn(string message)
        {
            Log("Warn", message);
        }

        public void LogWarn(string messageFormat, params object[] messageParameters)
        {
            Log("Warn", string.Format(messageFormat, messageParameters));
        }

        public void LogFatal(string message)
        {
            Log("Fatal", message);
        }

        public void LogFatal(string messageFormat, params object[] messageParameters)
        {
            Log("Fatal", string.Format(messageFormat, messageParameters));
        }

        public void LogError(string message)
        {
            Log("Error", message);
        }

        public void LogError(string messageFormat, params object[] messageParameters)
        {
            Log("Error", string.Format(messageFormat, messageParameters));
        }

        public void LogException(Exception e)
        {
            Log("Exception", string.Format("{0}{1}{2}", e.Message, Environment.NewLine, e.StackTrace));
        }

        private void Log(string level, string message)
        {
            Console.WriteLine("[{0}] [{1}] [{2}] [{3}] {4}", DateTime.UtcNow.ToString("o"), Environment.MachineName, level, type.Name, message);
        }
    }
}
