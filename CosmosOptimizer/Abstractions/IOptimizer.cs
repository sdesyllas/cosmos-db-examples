using System.Threading.Tasks;

namespace CosmosOptimizer.Abstractions
{
    public interface IOptimizer
    {
        Task RunAsync();
    }
}
