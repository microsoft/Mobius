using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading.Tasks;
using log4net;
using log4net.Config;

namespace Microsoft.Spark.CSharp.Services
{
    [ExcludeFromCodeCoverage] //unit test coverage not reqiured for logger service
    public class Log4NetLoggerService : ILoggerService
    {
        private readonly ILog logger;
        private const string exceptionLogDelimiter = "*******************************************************************************************************************************";
        public static Log4NetLoggerService Instance = new Log4NetLoggerService(typeof(Type));

        static Log4NetLoggerService()
        {
            XmlConfigurator.Configure();
        }

        public Log4NetLoggerService(Type type)
        {
            logger = LogManager.GetLogger(type);
            log4net.GlobalContext.Properties["pid"] = Process.GetCurrentProcess().Id;
        }

        public void LogDebug(string message)
        {
            logger.Debug(message);
        }

        public void LogInfo(string message)
        {
            logger.Info(message);
        }

        public void LogWarn(string message)
        {
            logger.Warn(message);
        }

        public void LogFatal(string message)
        {
            logger.Fatal(message);
        }

        public void LogError(string message)
        {
            logger.Error(message);
        }

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
         
        public ILoggerService GetLoggerInstance(Type type)
        {
            return new Log4NetLoggerService(type);
        }
    }

}
