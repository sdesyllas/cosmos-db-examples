using CosmosOptimizer.Abstractions;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System;
using System.Linq;

namespace CosmosOptimizer.Services
{
    /// <summary>
    /// The bulk executor library helps you leverage this massive throughput and storage. 
    /// The bulk executor library allows you to perform bulk operations in Azure Cosmos DB through bulk import 
    /// and bulk update APIs.
    /// 
    /// 
    /// </summary>
    public class CosmosBulkExecutor : ICosmosBulkExecutor
    {
        private readonly ILogger _logger;
        private readonly OptimizerSettings _appSettings;

        private readonly CosmosClient _cosmosBulkClient;

        public CosmosBulkExecutor(ILogger<CosmosBulkExecutor> logger, IOptions<OptimizerSettings> appSettings)
        {
            _logger = logger;
            _appSettings = appSettings.Value;


            // enabling bulk execution
            // 
            var options = new CosmosClientOptions { AllowBulkExecution = true };

            // Create a new instance of the Cosmos Client
            this._cosmosBulkClient = new CosmosClient(_appSettings.EndpointUri, _appSettings.PrimaryKey, options);
        }


        /// <summary>
        /// Copy Documents from source to destination by utilizing Bulk executor library and parallel bulk data inserts
        /// It works by fetching documents in batches automatically adjusted by cosmos bulk executor, then creating documents in batches with parallel tasks
        /// for maximum efficiency in the destination container
        /// </summary>
        /// <param name="dataBase"></param>
        /// <param name="source"></param>
        /// <param name="destination"></param>
        /// <returns></returns>
        public async Task CopyDocumentsFromSourceToDestinationContainers(string sourceDataBase, string destinationDataBase, string sourceContainer, string destinationContainer)
        {
            _logger.LogInformation($"Reading all documents from {sourceDataBase}/{sourceContainer}");
            //var documents = await ReadAllDocumentsFromCollection(sourceDataBase, sourceContainer);
            Container source = _cosmosBulkClient.GetContainer(sourceDataBase, sourceContainer);
            var containerProperties = source.ReadContainerAsync().Result.Resource;
            // Get all items from the source container into a dictionary
            FeedIterator feedIterator = source.GetItemQueryStreamIterator("SELECT * FROM c");
            int count = 0;
            while (feedIterator.HasMoreResults)
            {
                // give some space for cosmos container to breath - half second
                // Thread.Sleep(500);
                using ResponseMessage response = await feedIterator.ReadNextAsync();
                using StreamReader sr = new StreamReader(response.Content);
                List<JToken> itemsToInsert = new List<JToken>();
                using JsonTextReader jtr = new JsonTextReader(sr);
                JObject result = JObject.Load(jtr);
                var docs = result["Documents"];

                foreach (var doc in docs)
                {
                    if (doc["_rid"] != null)
                        doc["_rid"].Parent.Remove();
                    if (doc["_self"] != null)
                        doc["_self"].Parent.Remove();
                    if (doc["_etag"] != null)
                        doc["_etag"].Parent.Remove();
                    if (doc["_attachments"] != null)
                        doc["_attachments"].Parent.Remove();
                    if (doc["_ts"] != null)
                        doc["_ts"].Parent.Remove();

                    var serializedJson = doc.ToString();
                    var document = JsonConvert.DeserializeObject<dynamic>(serializedJson);

                    itemsToInsert.Add(document);
                }

                // Insert all items from the dictionary to the destination container by utilizing parallelism
                Container destination = _cosmosBulkClient.GetContainer(destinationDataBase, destinationContainer);

                List<Task> tasks = new List<Task>(itemsToInsert.Count);

                // Next use the data streams to create concurrent tasks and populate the task list to insert the items into the container.
                foreach (var item in itemsToInsert)
                    tasks.Add(destination.CreateItemAsync(item).ContinueWith(itemResponse =>
                    {
                        if (!itemResponse.IsCompletedSuccessfully)
                        {
                            AggregateException innerExceptions = itemResponse.Exception.Flatten();
                            if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                            {
                                _logger.LogError(cosmosException, $"Received {cosmosException.StatusCode}");
                            }
                            else
                            {
                                _logger.LogError("Exception", $"Received {innerExceptions.InnerExceptions.FirstOrDefault()}");
                            }
                        }
                    }));

                count += itemsToInsert.Count;
                _logger.LogInformation($"creating {itemsToInsert.Count} in {destinationDataBase}/{destinationContainer}...");
                // Wait until all are done
                await Task.WhenAll(tasks);
              
            }
            _logger.LogInformation($"finished copying {count} documents from {sourceDataBase}/{sourceContainer} to {destinationDataBase}/{destinationContainer}");

        }
    }
}
