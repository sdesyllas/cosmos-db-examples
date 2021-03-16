using System.Threading.Tasks;

namespace CosmosOptimizer.Abstractions
{
    public interface ICosmosBulkExecutor
    {
        Task CopyDocumentsFromSourceToDestinationContainers(string sourceDataBase, string destinationDataBase, string sourceContainer, string destinationContainer);
    }
}