using Microsoft.Extensions.Configuration;
using KafkaOidcClient;

class Program
{
    static async Task Main(string[] args)
    {
        // Load configuration
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false)
            .AddEnvironmentVariables()
            .Build();

        // Read Kafka settings
        var bootstrapServers = configuration["Kafka:BootstrapServers"]!;
        var logicalCluster = configuration["Kafka:LogicalCluster"]!;
        var identityPoolId = configuration["Kafka:IdentityPoolId"]!;
        var topic = configuration["Kafka:Topic"]!;
        var groupId = configuration["Kafka:GroupId"]!;

        // Read Azure AD settings
        var tenantId = configuration["AzureAd:TenantId"]!;
        var clientId = configuration["AzureAd:ClientId"]!;
        var clientSecret = Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET")
            ?? throw new InvalidOperationException("AZURE_CLIENT_SECRET environment variable is not set");
        var scope = configuration["AzureAd:Scope"]!;

        // Determine mode
        var mode = args.Length > 0 ? args[0].ToLower() : "both";

        // Create OAuth token provider
        var tokenProvider = new OAuthTokenProvider(tenantId, clientId, clientSecret, scope);

        switch (mode)
        {
            case "produce":
                await RunProducerAsync(bootstrapServers, logicalCluster, identityPoolId, tokenProvider, topic);
                break;

            case "consume":
                RunConsumer(bootstrapServers, logicalCluster, identityPoolId, groupId, tokenProvider, topic);
                break;

            case "both":
            default:
                Console.WriteLine("=== PRODUCING MESSAGES ===");
                await RunProducerAsync(bootstrapServers, logicalCluster, identityPoolId, tokenProvider, topic);

                Console.WriteLine("\n=== CONSUMING MESSAGES ===");
                RunConsumer(bootstrapServers, logicalCluster, identityPoolId, groupId, tokenProvider, topic);
                break;
        }
    }

    static async Task RunProducerAsync(
        string bootstrapServers,
        string logicalCluster,
        string identityPoolId,
        OAuthTokenProvider tokenProvider,
        string topic)
    {
        using var producer = new KafkaProducerService(
            bootstrapServers,
            logicalCluster,
            identityPoolId,
            tokenProvider,
            topic);

        await producer.ProduceMessagesAsync(10);
    }

    static void RunConsumer(
        string bootstrapServers,
        string logicalCluster,
        string identityPoolId,
        string groupId,
        OAuthTokenProvider tokenProvider,
        string topic)
    {
        using var consumer = new KafkaConsumerService(
            bootstrapServers,
            logicalCluster,
            identityPoolId,
            groupId,
            tokenProvider,
            topic);

        consumer.ConsumeMessages();
    }
}
