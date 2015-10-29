using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Services
{
    //TODO - add log4net NuGet and complete impl
    public class Log4NetLoggerService : ILoggerService
    {
        /*
        private ILog logger;
        private const string exceptionLogDelimiter = "*******************************************************************************************************************************";
        */
        public Log4NetLoggerService(Type type)
        {
            //logger = log4net.LogManager.GetLogger(type);
            throw new NotImplementedException();
        }

        public void LogDebug(string message)
        {
            //logger.Debug(message);
            throw new NotImplementedException();
        }

        public void LogInfo(string message)
        {
            //logger.Info(message);
            throw new NotImplementedException();
        }

        public void LogWarn(string message)
        {
            //logger.Warn(message);
            throw new NotImplementedException();
        }

        public void LogFatal(string message)
        {
            //logger.Fatal(message);
            throw new NotImplementedException();
        }

        public void LogError(string message)
        {
            //logger.Error(message);
            throw new NotImplementedException();
        }

        public void LogException(Exception e)
        {
            throw new NotImplementedException();
            /*
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
                AggregateException aggregateException = e as AggregateException;
                int count = 1;
                foreach (var innerException in aggregateException.InnerExceptions)
                {
                    logger.Error(string.Format("Aggregate exception #{0} details....", count++));
                    LogException(innerException);
                }

            }
            */
        }
         
        public ILoggerService GetLoggerInstance(Type type)
        {
            throw new NotImplementedException();
        }
    }

}
