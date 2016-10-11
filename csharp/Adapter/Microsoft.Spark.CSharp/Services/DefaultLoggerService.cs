using System;

namespace Microsoft.Spark.CSharp.Services
{
    /// <summary>
    /// This logger service will be used if the C# driver app did not configure a logger.
    /// Right now it just prints out the messages to Console
    /// </summary>
    public class DefaultLoggerService : ILoggerService
    {
        internal static readonly DefaultLoggerService Instance = new DefaultLoggerService(typeof(Type));
        private readonly Type type;

        private DefaultLoggerService(Type t)
        {
            type = t;
        }

        /// <summary>
        /// Gets a value indicating whether logging is enabled for the Debug level.
        /// Always return true for the DefaultLoggerService object.
        /// </summary>
        public bool IsDebugEnabled { get { return true; } }

        /// <summary>
        /// Get an instance of ILoggerService by a given type of logger
        /// </summary>
        /// <param name="type">The type of a logger to return</param>
        /// <returns>An instance of ILoggerService</returns>
        public ILoggerService GetLoggerInstance(Type type)
        {
            return new DefaultLoggerService(type);
        }
        
        /// <summary>
        /// Logs a message at debug level.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        public void LogDebug(string message)
        {
            Log("Debug", message);
        }

        /// <summary>
        /// Logs a message at debug level with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        public void LogDebug(string messageFormat, params object[] messageParameters)
        {
            Log("Debug", string.Format(messageFormat, messageParameters));
        }

        /// <summary>
        /// Logs a message at info level.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        public void LogInfo(string message)
        {
            Log("Info", message);
        }

        /// <summary>
        /// Logs a message at info level with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        public void LogInfo(string messageFormat, params object[] messageParameters)
        {
            Log("Info", string.Format(messageFormat, messageParameters));
        }

        /// <summary>
        /// Logs a message at warning level.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        public void LogWarn(string message)
        {
            Log("Warn", message);
        }

        /// <summary>
        /// Logs a message at warning level with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        public void LogWarn(string messageFormat, params object[] messageParameters)
        {
            Log("Warn", string.Format(messageFormat, messageParameters));
        }

        /// <summary>
        /// Logs a fatal message.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        public void LogFatal(string message)
        {
            Log("Fatal", message);
        }

        /// <summary>
        /// Logs a fatal message with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        public void LogFatal(string messageFormat, params object[] messageParameters)
        {
            Log("Fatal", string.Format(messageFormat, messageParameters));
        }

        /// <summary>
        /// Logs a error message.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        public void LogError(string message)
        {
            Log("Error", message);
        }

        /// <summary>
        /// Logs a error message with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        public void LogError(string messageFormat, params object[] messageParameters)
        {
            Log("Error", string.Format(messageFormat, messageParameters));
        }

        /// <summary>
        /// Logs an exception
        /// </summary>
        /// <param name="e">The exception to be logged</param>
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
