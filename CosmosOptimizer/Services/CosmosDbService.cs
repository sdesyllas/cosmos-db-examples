using CosmosOptimizer.Abstractions;
using CosmosOptimizer.App;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CosmosOptimizer.Services
{
    /// <summary>
    /// Based on Cosmos SDK 3 https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-get-started
    /// </summary>
    public class CosmosDbService : ICosmosDbService
    {
        private readonly ILogger _logger;
        private readonly OptimizerSettings _appSettings;

        private readonly CosmosClient _cosmosClient;

        public CosmosDbService(ILogger<Optimizer> logger, IOptions<OptimizerSettings> appSettings)
        {
            _logger = logger;
            _appSettings = appSettings.Value;

            // Create a new instance of the Cosmos Client
            this._cosmosClient = new CosmosClient(_appSettings.EndpointUri, _appSettings.PrimaryKey);
        }


        /// <summary>
        /// Create the database if it does not exist
        /// </summary>
        public async Task<Database> CreateDatabaseAsync(string databaseId, ThroughputProperties throuput)
        {
            // Create a new database
            Database tempDatabase = await this._cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId,
                throuput);
            _logger.LogInformation($"Created database {tempDatabase.Id}, Throughput:{throuput.Throughput}RUs");
            return tempDatabase;
        }


        public async Task<List<ContainerProperties>> ListContainersAsync(string databaseId)
        {
            var optimizationDatabase = this._cosmosClient.GetDatabase(databaseId);

            FeedIterator<ContainerProperties> iterator = optimizationDatabase.GetContainerQueryIterator<ContainerProperties>();
            FeedResponse<ContainerProperties> containersFeed = await iterator.ReadNextAsync().ConfigureAwait(false);
            List<ContainerProperties> containers = new List<ContainerProperties>();   
            foreach (var container in containersFeed)
            {
                // do what you want with the container
                _logger.LogInformation($"Container fetched : {container.Id}, partitionKey : {container.PartitionKeyPath}");
                containers.Add(container);
            }
            return containers;
        }

        /// <summary>
        /// Create the container if it does not exist. 
        /// Specifiy "/LastName" as the partition key since we're storing family information, to ensure good distribution of requests and storage.
        /// </summary>
        /// <returns></returns>
        public async Task CreateContainerAsync(Database database, string containerId, string partitionKey)
        {
            // Create a new container
            ContainerProperties container = await database.CreateContainerIfNotExistsAsync(containerId, partitionKey);
            _logger.LogInformation($"Created container:{container.Id} with partitionKey:{container.PartitionKeyPath} in db:{database.Id}");
        }


        /// <summary>
        /// Delete the database and dispose of the Cosmos Client instance
        /// </summary>
        public async Task DeleteDatabaseAsync(string databaseId)
        {
            var databaseToDelete = this._cosmosClient.GetDatabase(databaseId);
            await databaseToDelete.DeleteAsync();
            _logger.LogInformation($"Database {databaseId} deleted successfully.");
        }
    }
}
