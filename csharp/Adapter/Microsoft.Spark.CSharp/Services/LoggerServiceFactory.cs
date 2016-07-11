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
        private static ILoggerService loggerService = Log4NetLoggerService.Instance;
        
        /// <summary>
        /// Overrides an existing logger by a given logger service instance
        /// </summary>
        /// <param name="loggerServiceOverride">The logger service instance used to overrides</param>
        public static void SetLoggerService(ILoggerService loggerServiceOverride)
        {
            loggerService = loggerServiceOverride;
            var logger = GetLogger(typeof(LoggerServiceFactory));
            logger.LogInfo("Logger service configured to use {0}", logger.GetType().Name);
        }

        /// <summary>
        /// Gets an instance of logger service for a given type.
        /// </summary>
        /// <param name="type">The type of logger service to get</param>
        /// <returns>An instance of logger service</returns>
        public static ILoggerService GetLogger(Type type)
        {
            return  loggerService.GetLoggerInstance(type);
        }
    }
}
