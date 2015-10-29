using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Services
{
    public class LoggerServiceFactory
    {
        private static ILoggerService loggerService = DefaultLoggerService.BootstrappingLoggerService;
        public static void SetLoggerService(ILoggerService loggerServiceOverride)
        {
            loggerService = loggerServiceOverride;
        }

        public static ILoggerService GetLogger(Type type)
        {
            return  loggerService.GetLoggerInstance(type);
        }
    }
}
