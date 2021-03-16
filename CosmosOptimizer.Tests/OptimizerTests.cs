using FizzWare.NBuilder;
using CosmosOptimizer.Abstractions;
using CosmosOptimizer.App;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace CosmosOptimizer.Tests
{
    public class OptimizerTests
    {
        private readonly Mock<ICosmosBulkExecutor> _cosmosBulkExecutor;
        private readonly Mock<ICosmosDbService> _cosmosDbService;
        private readonly Mock<ILogger<Optimizer>> _logger;
        private readonly Mock<IOptions<OptimizerSettings>> _settings;

        public OptimizerTests()
        {
            _cosmosBulkExecutor = new Mock<ICosmosBulkExecutor>();
            _cosmosDbService = new Mock<ICosmosDbService>();
            _logger = new Mock<ILogger<Optimizer>>();
            _settings = new Mock<IOptions<OptimizerSettings>>();

            var optimizerSettings = 
                Builder<OptimizerSettings>.CreateNew().
                With(x => x.DbForOptimization = Builder<OptimizerSettings.DbForOptimizationSettings>.CreateNew().Build()).Build();

            _settings.SetupGet(x => x.Value).Returns(optimizerSettings);
        }

        [Fact]
        public void RunAsync_AppStartedWithDefaultSettings_PipelineForOptimizationCompleted()
        {
            // Arrange
            List<ContainerProperties> mockContainers = Builder<ContainerProperties>.CreateListOfSize(10).Build().ToList();
            Mock<Database> mockDatabase = new Mock<Database>();
            mockDatabase.SetupGet(x => x.Id).Returns(Guid.NewGuid().ToString());

            _cosmosBulkExecutor.Setup(x => x.CopyDocumentsFromSourceToDestinationContainers(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>())).Verifiable();
            _cosmosDbService.Setup(x => x.CreateContainerAsync(It.IsAny<Database>(), It.IsAny<string>(), It.IsAny<string>())).Verifiable();
            _cosmosDbService.Setup(x => x.CreateDatabaseAsync(It.IsAny<string>(), It.IsAny<ThroughputProperties>())).ReturnsAsync(mockDatabase.Object).Verifiable();
            _cosmosDbService.Setup(x => x.DeleteDatabaseAsync(It.IsAny<string>())).Verifiable();
            _cosmosDbService.Setup(x => x.ListContainersAsync(It.IsAny<string>())).ReturnsAsync(mockContainers).Verifiable();

            IOptimizer optimizer = new Optimizer(_logger.Object, _settings.Object, _cosmosDbService.Object, _cosmosBulkExecutor.Object);

            // Act
            optimizer.RunAsync().GetAwaiter().GetResult();

            // Assert
            _cosmosDbService.Verify(x => x.CreateDatabaseAsync(It.IsAny<string>(), It.IsAny<ThroughputProperties>()), Times.Exactly(2));
            _cosmosDbService.Verify(x => x.CreateContainerAsync(It.IsAny<Database>(), It.IsAny<string>(), It.IsAny<string>()), Times.Exactly(20));
            _cosmosDbService.Verify(x => x.DeleteDatabaseAsync(It.IsAny<string>()), Times.Exactly(2));
            _cosmosDbService.Verify(x => x.ListContainersAsync(It.IsAny<string>()), Times.Exactly(2));
            _cosmosBulkExecutor.Verify(x => x.CopyDocumentsFromSourceToDestinationContainers(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<string>()), Times.Exactly(20));
        }
    }
}
