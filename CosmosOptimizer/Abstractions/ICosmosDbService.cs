using Microsoft.Azure.Cosmos;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CosmosOptimizer.Abstractions
{
    public interface ICosmosDbService
    {
        Task<Database> CreateDatabaseAsync(string databaseId, ThroughputProperties throughput);

        Task<List<ContainerProperties>> ListContainersAsync(string databaseId);

        Task CreateContainerAsync(Database database, string containerId, string partitionKey);

        Task DeleteDatabaseAsync(string databaseId);
    }
}