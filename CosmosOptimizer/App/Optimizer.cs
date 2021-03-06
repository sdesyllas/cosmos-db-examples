using CosmosOptimizer.Abstractions;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace CosmosOptimizer.App
{
    public class Optimizer : IOptimizer
    {
        private readonly ILogger _logger;
        private readonly OptimizerSettings _appSettings;
        private readonly ICosmosDbService _cosmosDbService;
        private readonly ICosmosBulkExecutor _cosmosBulkExecutor;

        public Optimizer(ILogger<Optimizer> logger, IOptions<OptimizerSettings> appSettings, 
            ICosmosDbService cosmosDbService,
            ICosmosBulkExecutor cosmosBulkExecutor)
        {
            _logger = logger;
            _appSettings = appSettings.Value;
            _cosmosDbService = cosmosDbService;
            _cosmosBulkExecutor = cosmosBulkExecutor;
        }

        public async Task RunAsync()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            _logger.LogInformation("Optimizer Started at {dateTime}", DateTime.UtcNow);
            // Business Logic START
            var tempDatabaseId = Guid.NewGuid().ToString();

            // Create a temporary database with random generated GUID and manual Throuput provisioned for shared containers
            // This temp database will be in the same account as the one we want to optimize and we use it as a temporary database
            // in order to pull the data from the original one
                        
            var tempDatabase = await _cosmosDbService.
                CreateDatabaseAsync(tempDatabaseId, ThroughputProperties.CreateManualThroughput(_appSettings.TempThroughput));

            // Fetch all collections from the original database that needs optimization , Id and partitionKey
            var sourceContainers = await _cosmosDbService.ListContainersAsync(_appSettings.DbForOptimization.Name);

            // Foreach collection fetched create the collection in the temp database with the exact same id
            // and partition key path
            foreach (var sourceContainer in sourceContainers)
            {
                await _cosmosDbService.CreateContainerAsync(tempDatabase, sourceContainer.Id, sourceContainer.PartitionKeyPath);
            }

            // copy data from source database to the temp database
            foreach (var sourceContainer in sourceContainers)
            {
                await _cosmosBulkExecutor.CopyDocumentsFromSourceToDestinationContainers(_appSettings.DbForOptimization.Name, tempDatabase.Id, sourceContainer.Id, sourceContainer.Id);
            }

            // now that all data is moved to the temp database we delete the original one in order to re-create it
            // with the desired Throuput configuration
            await _cosmosDbService.DeleteDatabaseAsync(_appSettings.DbForOptimization.Name);

            // recreate the Microservices database with the correct configuration
            // we create it with high manual throuput to achieve the bulk copy that requires a lot of RUs
            var optimizedDatabase = await _cosmosDbService.CreateDatabaseAsync(_appSettings.DbForOptimization.Name, ThroughputProperties.CreateManualThroughput(_appSettings.TempThroughput));

            // Fetch all collections from the temp database, Id and partitionKey
            var tempContainers = await _cosmosDbService.ListContainersAsync(tempDatabaseId);

            // Foreach collection fetched re-create the collection in the optimized database with the exact same id
            // and partition key path
            foreach (var tempContainer in tempContainers)
            {
                await _cosmosDbService.CreateContainerAsync(optimizedDatabase, tempContainer.Id, tempContainer.PartitionKeyPath);
            }

            // copy data from temp database to the optimized database
            foreach (var tempContainer in tempContainers)
            {
                await _cosmosBulkExecutor.CopyDocumentsFromSourceToDestinationContainers(tempDatabaseId, optimizedDatabase.Id, tempContainer.Id, tempContainer.Id);
            }

            // now drop the temp database
            await _cosmosDbService.DeleteDatabaseAsync(tempDatabaseId);

            // finally configure the optimized database desired Throughput (20 containers * 100 = 2000RUs)
            var throughput = tempContainers.Count * _appSettings.DbForOptimization.ThroughputPerContainer;
            // 500RUs is the minimum throughput for more than 1 shared Throuput container
            if (throughput < 500)
                throughput = 500;
            await optimizedDatabase.ReplaceThroughputAsync(ThroughputProperties.CreateManualThroughput(throughput));

            // don't forget to check the database in the portal for manual validation
            _logger.LogInformation($"don't forget to check the database {optimizedDatabase.Id} in the portal for manual validation");

            // Business logic END
            _logger.LogInformation("Optimizer Ended at {dateTime}", DateTime.UtcNow);
            stopwatch.Stop();
            _logger.LogInformation($"total elapsed: {stopwatch.Elapsed}");
        }
    }
}
