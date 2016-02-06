using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Services
{
    /// <summary>
    /// Used to get logger service instances for different types
    /// </summary>
    public class LoggerServiceFactory
    {
        private static ILoggerService loggerService = DefaultLoggerService.Instance;
        
        public static void SetLoggerService(ILoggerService loggerServiceOverride)
        {
            loggerService = loggerServiceOverride;
            var logger = GetLogger(typeof(LoggerServiceFactory));
            logger.LogInfo("Logger service configured to use {0}", logger.GetType().Name);
        }

        public static ILoggerService GetLogger(Type type)
        {
            return  loggerService.GetLoggerInstance(type);
        }
    }
}
