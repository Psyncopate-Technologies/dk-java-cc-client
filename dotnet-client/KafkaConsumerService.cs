using Confluent.Kafka;

namespace KafkaOidcClient;

/// <summary>
/// Kafka consumer with OAuth/OIDC authentication.
/// </summary>
public class KafkaConsumerService : IDisposable
{
    private readonly IConsumer<string, string> _consumer;
    private readonly string _topic;
    private readonly CancellationTokenSource _cts;

    public KafkaConsumerService(
        string bootstrapServers,
        string logicalCluster,
        string identityPoolId,
        string groupId,
        OAuthTokenProvider tokenProvider,
        string topic)
    {
        _topic = topic;
        _cts = new CancellationTokenSource();

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            SaslOauthbearerConfig = $"extension_logicalCluster={logicalCluster} extension_identityPoolId={identityPoolId}"
        };

        _consumer = new ConsumerBuilder<string, string>(config)
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

    public void ConsumeMessages()
    {
        Console.WriteLine($"Subscribed to topic '{_topic}'. Waiting for messages...");
        Console.WriteLine("Press Ctrl+C to stop consuming...");

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            _cts.Cancel();
        };

        _consumer.Subscribe(_topic);

        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(TimeSpan.FromSeconds(5));

                    if (result == null)
                    {
                        Console.WriteLine("No records yet.");
                        continue;
                    }

                    Console.WriteLine($"Consumed message from topic {result.Topic}: " +
                                      $"key = {result.Message.Key} " +
                                      $"value = {result.Message.Value}");
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"Consume error: {ex.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Shutdown signal received; closing consumer.");
        }
        finally
        {
            _consumer.Close();
            Console.WriteLine("Consumer closed.");
        }
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _consumer?.Dispose();
    }
}
