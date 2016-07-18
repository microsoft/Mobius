using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
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
        private static Lazy<ILoggerService> loggerService = new Lazy<ILoggerService>(() => GetDefaultLogger());

        /// <summary>
        /// Overrides an existing logger by a given logger service instance
        /// </summary>
        /// <param name="loggerServiceOverride">The logger service instance used to overrides</param>
        public static void SetLoggerService(ILoggerService loggerServiceOverride)
        {
            loggerService = new Lazy<ILoggerService>(() => loggerServiceOverride);
        }

        /// <summary>
        /// Gets an instance of logger service for a given type.
        /// </summary>
        /// <param name="type">The type of logger service to get</param>
        /// <returns>An instance of logger service</returns>
        public static ILoggerService GetLogger(Type type)
        {
            return loggerService.Value.GetLoggerInstance(type);
        }

        /// <summary>
        /// if there exists xxx.exe.config file and log4net settings, then use log4net
        /// </summary>
        /// <returns></returns>
        private static ILoggerService GetDefaultLogger()
        {
            if (File.Exists(AppDomain.CurrentDomain.SetupInformation.ConfigurationFile)
                && ConfigurationManager.GetSection("log4net") != null)
            {
                return Log4NetLoggerService.Instance;
            }

            return DefaultLoggerService.Instance;
        }
    }
}
