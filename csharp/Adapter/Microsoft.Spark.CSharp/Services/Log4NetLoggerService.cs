using System;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using log4net;
using log4net.Config;

namespace Microsoft.Spark.CSharp.Services
{
    /// <summary>
    /// Represents a Log4Net logger.
    /// </summary>
    [ExcludeFromCodeCoverage] //unit test coverage not required for logger service
    public class Log4NetLoggerService : ILoggerService
    {
        private readonly ILog logger;
        private const string exceptionLogDelimiter = "*******************************************************************************************************************************";
        /// <summary>
        /// Gets a instance of Log4Net logger
        /// </summary>
        public static Log4NetLoggerService Instance = new Log4NetLoggerService(typeof(Type));

        static Log4NetLoggerService()
        {
            XmlConfigurator.Configure();
        }

        /// <summary>
        /// Initializes a instance of Log4NetLoggerService with a specific type.
        /// </summary>
        /// <param name="type">The type of the logger</param>
        public Log4NetLoggerService(Type type)
        {
            logger = LogManager.GetLogger(type);
            GlobalContext.Properties["pid"] = Process.GetCurrentProcess().Id;
        }

        /// <summary>
        /// Gets a value indicating whether logging is enabled for the Debug level.
        /// </summary>
        public bool IsDebugEnabled
        {
            get { return logger.IsDebugEnabled; }
        }

        /// <summary>
        /// Logs a message at debug level.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        public void LogDebug(string message)
        {
            logger.Debug(message);
        }

        /// <summary>
        /// Logs a message at debug level with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        public void LogDebug(string messageFormat, params object[] messageParameters)
        {
            logger.DebugFormat(messageFormat, messageParameters);
        }

        /// <summary>
        /// Logs a message at info level.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        public void LogInfo(string message)
        {
            logger.Info(message);
        }

        /// <summary>
        /// Logs a message at info level with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        public void LogInfo(string messageFormat, params object[] messageParameters)
        {
            logger.InfoFormat(messageFormat, messageParameters);
        }

        /// <summary>
        /// Logs a message at warning level.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        public void LogWarn(string message)
        {
            logger.Warn(message);
        }

        /// <summary>
        /// Logs a message at warning level with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        public void LogWarn(string messageFormat, params object[] messageParameters)
        {
            logger.WarnFormat(messageFormat, messageParameters);
        }

        /// <summary>
        /// Logs a fatal message.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        public void LogFatal(string message)
        {
            logger.Fatal(message);
        }

        /// <summary>
        /// Logs a fatal message with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        public void LogFatal(string messageFormat, params object[] messageParameters)
        {
            logger.FatalFormat(messageFormat, messageParameters);
        }

        /// <summary>
        /// Logs a error message.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        public void LogError(string message)
        {
            logger.Error(message);
        }

        /// <summary>
        /// Logs a error message with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        public void LogError(string messageFormat, params object[] messageParameters)
        {
            logger.ErrorFormat(messageFormat, messageParameters);
        }

        /// <summary>
        /// Logs an exception
        /// </summary>
        /// <param name="e">The exception to be logged</param>
        public void LogException(Exception e)
        {
            
            if (e.GetType() != typeof(AggregateException))
            {
                logger.Error(e.Message);

                logger.Error(string.Format("{5}{0}{1}{2}{3}{4}", exceptionLogDelimiter, Environment.NewLine, e.StackTrace, Environment.NewLine, exceptionLogDelimiter, Environment.NewLine));

                var innerException = e.InnerException;
                if (innerException != null)
                {
                    logger.Error("Inner exception 1 details....");
                    logger.Error(innerException.Message);
                    logger.Error(string.Format("{5}{0}{1}{2}{3}{4}", exceptionLogDelimiter, Environment.NewLine, innerException.StackTrace, Environment.NewLine, exceptionLogDelimiter, Environment.NewLine));

                    var innerException2 = innerException.InnerException;
                    if (innerException2 != null)
                    {
                        logger.Error("Inner exception 2 details....");
                        logger.Error(innerException2.Message);
                        logger.Error(string.Format("{5}{0}{1}{2}{3}{4}", exceptionLogDelimiter, Environment.NewLine, innerException2.StackTrace, Environment.NewLine, exceptionLogDelimiter, Environment.NewLine));
                    }
                }
            }
            else
            {
                LogError("Aggregate Exception thrown...");
                var aggregateException = e as AggregateException;
                int count = 1;
                foreach (var innerException in aggregateException.InnerExceptions)
                {
                    logger.Error(string.Format("Aggregate exception #{0} details....", count++));
                    LogException(innerException);
                }

            }
            
        }

        /// <summary>
        /// Get an instance of ILoggerService by a given type of logger
        /// </summary>
        /// <param name="type">The type of a logger to return</param>
        /// <returns>An instance of ILoggerService</returns>
        public ILoggerService GetLoggerInstance(Type type)
        {
            return new Log4NetLoggerService(type);
        }
    }

}
