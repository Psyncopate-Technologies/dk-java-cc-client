using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace DkDotnetCcClient;

public static class Program
{
    private const int MessageCount = 10;

    public static int Main(string[] args)
    {
        try
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false)
                .AddJsonFile("appsettings.Development.json", optional: true)
                .AddEnvironmentVariables()
                .Build();

            var kafka = config.GetSection("Kafka").Get<KafkaSettings>()
                ?? throw new InvalidOperationException("Missing 'Kafka' section in appsettings.json.");
            var azure = config.GetSection("AzureAd").Get<AzureAdSettings>()
                ?? throw new InvalidOperationException("Missing 'AzureAd' section in appsettings.json.");

            kafka.Validate();
            azure.Validate();

            var clientSecret = Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET")
                ?? throw new InvalidOperationException("AZURE_CLIENT_SECRET env var is not set.");

            Produce(kafka.Topic, BuildProducerConfig(kafka, azure, clientSecret));
            Consume(kafka.Topic, BuildConsumerConfig(kafka, azure, clientSecret));
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Fatal: {ex}");
            return 1;
        }
    }

    private static ProducerConfig BuildProducerConfig(KafkaSettings k, AzureAdSettings a, string clientSecret)
    {
        var cfg = new ProducerConfig { Acks = Acks.All };
        ApplyOAuth(cfg, k, a, clientSecret);
        return cfg;
    }

    private static ConsumerConfig BuildConsumerConfig(KafkaSettings k, AzureAdSettings a, string clientSecret)
    {
        var cfg = new ConsumerConfig
        {
            GroupId = k.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
        };
        ApplyOAuth(cfg, k, a, clientSecret);
        return cfg;
    }

    private static void ApplyOAuth(ClientConfig cfg, KafkaSettings k, AzureAdSettings a, string clientSecret)
    {
        cfg.BootstrapServers = k.BootstrapServers;
        cfg.SecurityProtocol = SecurityProtocol.SaslSsl;
        cfg.SaslMechanism = SaslMechanism.OAuthBearer;
        cfg.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
        cfg.SaslOauthbearerClientId = a.ClientId;
        cfg.SaslOauthbearerClientSecret = clientSecret;
        cfg.SaslOauthbearerScope = $"api://{a.ClientId}/.default";
        cfg.SaslOauthbearerTokenEndpointUrl =
            $"https://login.microsoftonline.com/{a.TenantId}/oauth2/v2.0/token";
        cfg.SaslOauthbearerExtensions =
            $"logicalCluster={k.LogicalCluster},identityPoolId={k.IdentityPoolId}";
    }

    private static void Produce(string topic, ProducerConfig config)
    {
        using var producer = new ProducerBuilder<string, string>(config).Build();

        for (int i = 0; i < MessageCount; i++)
        {
            var key = $"key-{i}";
            var value = $"hello from .NET OIDC producer {i}";

            producer.Produce(topic, new Message<string, string> { Key = key, Value = value },
                report =>
                {
                    if (report.Error.IsError)
                    {
                        Console.Error.WriteLine($"Produce error: {report.Error.Reason}");
                    }
                    else
                    {
                        Console.WriteLine(
                            $"Produced message to topic {report.Topic} partition {report.Partition.Value} offset {report.Offset.Value}: key = {report.Message.Key} value = {report.Message.Value}");
                    }
                });
        }

        producer.Flush(TimeSpan.FromSeconds(30));
    }

    private static void Consume(string topic, ConsumerConfig config)
    {
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        try
        {
            while (true)
            {
                ConsumeResult<string, string> result;
                try
                {
                    result = consumer.Consume(cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (result?.Message == null) continue;

                Console.WriteLine(
                    $"Consumed message from topic {topic}: key = {result.Message.Key} value = {result.Message.Value}");
            }
        }
        finally
        {
            Console.WriteLine("Shutdown signal received; closing consumer.");
            consumer.Close();
        }
    }

    private sealed class KafkaSettings
    {
        public string BootstrapServers { get; set; } = "";
        public string LogicalCluster { get; set; } = "";
        public string IdentityPoolId { get; set; } = "";
        public string Topic { get; set; } = "";
        public string GroupId { get; set; } = "";

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(BootstrapServers)) throw Missing(nameof(BootstrapServers));
            if (string.IsNullOrWhiteSpace(LogicalCluster)) throw Missing(nameof(LogicalCluster));
            if (string.IsNullOrWhiteSpace(IdentityPoolId)) throw Missing(nameof(IdentityPoolId));
            if (string.IsNullOrWhiteSpace(Topic)) throw Missing(nameof(Topic));
            if (string.IsNullOrWhiteSpace(GroupId)) throw Missing(nameof(GroupId));
        }

        private static InvalidOperationException Missing(string name)
            => new($"Kafka:{name} is not set.");
    }

    private sealed class AzureAdSettings
    {
        public string TenantId { get; set; } = "";
        public string ClientId { get; set; } = "";

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(TenantId)) throw Missing(nameof(TenantId));
            if (string.IsNullOrWhiteSpace(ClientId)) throw Missing(nameof(ClientId));
        }

        private static InvalidOperationException Missing(string name)
            => new($"AzureAd:{name} is not set.");
    }
}
