using CosmosOptimizer.Abstractions;
using CosmosOptimizer.App;
using CosmosOptimizer.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.IO;

namespace CosmosOptimizer
{
    public class Program
    {
        static async System.Threading.Tasks.Task Main(string[] args)
        {
            var services = new ServiceCollection();
            // Provide concrete services implementation to the Interface abstractions
            ConfigureServices(services);

            // create service provider for injected services - (Dependency Injection)
            var serviceProvider = services.BuildServiceProvider();

            // entry to run app, all the logic exists in this Optimizer service
            await serviceProvider.GetService<IOptimizer>().RunAsync();

            Console.ReadKey();
        }

        private static void ConfigureServices(ServiceCollection services)
        {
            var configurationBuilder = new ConfigurationBuilder();
            var configuration = new ConfigurationBuilder()
                   .SetBasePath(Directory.GetCurrentDirectory())
                   .AddJsonFile("appsettings.json", optional: false)
                   .Build();

            services
                .AddLogging(configure => configure.AddConsole())
                .Configure<OptimizerSettings>(configuration.GetSection("optimizerSettings"))
                .AddTransient<ICosmosDbService, CosmosDbService>()
                .AddTransient<ICosmosBulkExecutor, CosmosBulkExecutor>()
                .AddTransient<IOptimizer, Optimizer>();
        }
    }
}
