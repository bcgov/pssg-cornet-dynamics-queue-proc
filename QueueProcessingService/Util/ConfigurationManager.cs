using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace QueueProcessingService.Util
{
	public static class ConfigurationManager
	{
        public static IConfiguration AppSetting { get; }
        public static IConfiguration EnvSetting { get; }

        static ConfigurationManager()
        {
            AppSetting = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();

            // Pulls in all CORDYN_ prefixed Environment Variables
            EnvSetting = new ConfigurationBuilder().AddEnvironmentVariables("CORDYN.").Build();
        }

        public static String FetchConfig(String ConfigKey)
        {
            String returnValue = "";
            String EnvKey = ConfigKey.Replace(":", ".");
            if (EnvSetting[EnvKey] != null)
            {
                returnValue = EnvSetting[EnvKey];
            }
            else if (AppSetting[ConfigKey] != null)
            {
                returnValue = AppSetting[ConfigKey];
            }
            else
            {
                // @TODO: Throw/Log Error
            }
            return returnValue;
        }

    }
}
