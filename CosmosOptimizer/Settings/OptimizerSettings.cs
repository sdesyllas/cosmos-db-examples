namespace CosmosOptimizer
{
    public class OptimizerSettings
    {
        public string EndpointUri { get; set; }

        public string PrimaryKey { get; set; }

        public int TempThroughput { get; set; }

        public DbForOptimizationSettings DbForOptimization { get; set; }

        public class DbForOptimizationSettings
        {
            public int ThroughputPerContainer { get; set; }
            public string Name { get; set; }
        }
    }
}
