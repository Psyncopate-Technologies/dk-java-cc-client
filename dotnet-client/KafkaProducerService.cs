using Confluent.Kafka;

namespace KafkaOidcClient;

/// <summary>
/// Kafka producer with OAuth/OIDC authentication.
/// </summary>
public class KafkaProducerService : IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;

    public KafkaProducerService(
        string bootstrapServers,
        string logicalCluster,
        string identityPoolId,
        OAuthTokenProvider tokenProvider,
        string topic)
    {
        _topic = topic;

        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            Acks = Acks.All,
            SaslOauthbearerConfig = $"extension_logicalCluster={logicalCluster} extension_identityPoolId={identityPoolId}"
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetOAuthBearerTokenRefreshHandler((client, cfg) =>
            {
                try
                {
                    var (token, expiresInMs) = tokenProvider.GetToken();
                    client.OAuthBearerSetToken(
                        tokenValue: token,
                        lifetimeMs: expiresInMs,
                        principalName: "kafka-client");
                }
                catch (Exception ex)
                {
                    client.OAuthBearerSetTokenFailure(ex.Message);
                }
            })
            .Build();
    }

    public async Task ProduceMessagesAsync(int messageCount = 10)
    {
        Console.WriteLine($"Producing {messageCount} messages to topic '{_topic}'...");

        for (int i = 0; i < messageCount; i++)
        {
            var key = $"key-{i}";
            var value = $"hello from .NET OIDC producer {i}";

            try
            {
                var result = await _producer.ProduceAsync(_topic, new Message<string, string>
                {
                    Key = key,
                    Value = value
                });

                Console.WriteLine($"Produced message to topic {result.Topic} " +
                                  $"partition {result.Partition} " +
                                  $"offset {result.Offset}: " +
                                  $"key = {key} value = {value}");
            }
            catch (ProduceException<string, string> ex)
            {
                Console.WriteLine($"Failed to produce message: {ex.Error.Reason}");
            }
        }

        _producer.Flush(TimeSpan.FromSeconds(30));
        Console.WriteLine("All messages delivered successfully!");
    }

    public void Dispose()
    {
        _producer?.Dispose();
    }
}
