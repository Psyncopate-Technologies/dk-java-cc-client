using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace DkDotnetCcClient;

public static class Program
{
    private const int MessageCount = 10;
    private static readonly HttpClient HttpClient = new();

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

            Produce(kafka, azure, clientSecret);
            Consume(kafka, azure, clientSecret);
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Fatal: {ex}");
            return 1;
        }
    }

    private static void Produce(KafkaSettings k, AzureAdSettings a, string clientSecret)
    {
        var cfg = new ProducerConfig
        {
            BootstrapServers = k.BootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            Acks = Acks.All,
        };

        using var producer = new ProducerBuilder<string, string>(cfg)
            .SetOAuthBearerTokenRefreshHandler((client, _) => RefreshToken(client, k, a, clientSecret))
            .Build();

        for (int i = 0; i < MessageCount; i++)
        {
            var key = $"key-{i}";
            var value = $"hello from .NET OIDC producer {i}";

            producer.Produce(k.Topic, new Message<string, string> { Key = key, Value = value },
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

    private static void Consume(KafkaSettings k, AzureAdSettings a, string clientSecret)
    {
        var cfg = new ConsumerConfig
        {
            BootstrapServers = k.BootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            GroupId = k.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
        };

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        using var consumer = new ConsumerBuilder<string, string>(cfg)
            .SetOAuthBearerTokenRefreshHandler((client, _) => RefreshToken(client, k, a, clientSecret))
            .Build();
        consumer.Subscribe(k.Topic);

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
                    $"Consumed message from topic {k.Topic}: key = {result.Message.Key} value = {result.Message.Value}");
            }
        }
        finally
        {
            Console.WriteLine("Shutdown signal received; closing consumer.");
            consumer.Close();
        }
    }

    private static void RefreshToken(IClient client, KafkaSettings k, AzureAdSettings a, string clientSecret)
    {
        try
        {
            var form = new Dictionary<string, string>
            {
                ["client_id"] = a.ClientId,
                ["client_secret"] = clientSecret,
                ["scope"] = $"api://{a.ClientId}/.default",
                ["grant_type"] = "client_credentials",
            };

            using var request = new HttpRequestMessage(
                HttpMethod.Post,
                $"https://login.microsoftonline.com/{a.TenantId}/oauth2/v2.0/token")
            {
                Content = new FormUrlEncodedContent(form),
            };

            using var response = HttpClient.Send(request);
            response.EnsureSuccessStatusCode();

            using var stream = response.Content.ReadAsStream();
            using var doc = JsonDocument.Parse(stream);
            var root = doc.RootElement;

            var accessToken = root.GetProperty("access_token").GetString()
                ?? throw new InvalidOperationException("Token response missing 'access_token'.");
            var expiresIn = root.GetProperty("expires_in").GetInt64();
            var lifetimeMs = DateTimeOffset.UtcNow.AddSeconds(expiresIn).ToUnixTimeMilliseconds();

            var extensions = new Dictionary<string, string>
            {
                ["logicalCluster"] = k.LogicalCluster,
                ["identityPoolId"] = k.IdentityPoolId,
            };

            client.OAuthBearerSetToken(accessToken, lifetimeMs, a.ClientId, extensions);
        }
        catch (Exception ex)
        {
            client.OAuthBearerSetTokenFailure(ex.ToString());
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
