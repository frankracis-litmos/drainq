using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DrainQ
{
    internal class Program
    {
        public static IHost CreateHost(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                .ConfigureLogging((hostBuilderContext, loggingBuilder) =>
                {
                    loggingBuilder.AddSimpleConsole(options =>
                    {
                        options.IncludeScopes = true;
                        options.SingleLine = false;
                        options.TimestampFormat = "hh:mm:ss ";
                    });
                })
                .ConfigureAppConfiguration((hostBuilderContext, configApp) =>
                {
                })
                .ConfigureServices((hostBuilderContext, services) =>
                {
                    services.AddTransient<QueueDrainer>();
                })
                .Build();
        }

        class QueueOptions
        {
            public  string? ConnectionString { get; set; }
            public string? QueueName { get; set; }
            public bool DeadLetter { get; set; }
        }


        public static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
                .AddJsonFile("appsettings.local.json", optional: true, reloadOnChange: false)
                .Build();

            IConfiguration loadFrom = config;
            if (args.Length > 0)
            {
                var section = config.GetSection(args[0]);
                if (!section.Exists())
                {
                    Console.WriteLine("Section {0} not found", args[0]);
                    return;
                }
                loadFrom = section;
            }

            var options = new QueueOptions();
            loadFrom.Bind(options);
            if (string.IsNullOrEmpty(options.ConnectionString) || string.IsNullOrEmpty(options.QueueName))
            {
                Console.WriteLine("Configuration not found");
                return;
            }


            var host = CreateHost(args);
            var drainer = host.Services.GetRequiredService<QueueDrainer>();

            await drainer.DrainAsync(options.ConnectionString, options.QueueName, options.DeadLetter);
        }
    }
}
