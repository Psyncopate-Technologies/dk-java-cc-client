// =============================================================================
// DkDotnetCcClient — Program.cs
// -----------------------------------------------------------------------------
// Console app that authenticates to Confluent Cloud using Microsoft Entra ID
// (OAuth 2.0 client-credentials, OAUTHBEARER SASL), then produces a batch of
// test messages and consumes them on the configured topic.
//
// OAuth flow:
//   1. On each token refresh, POST to the Entra v2 token endpoint with
//      grant_type=client_credentials + scope=api://<ClientId>/.default.
//   2. Hand the resulting access_token to librdkafka via its
//      OAuthBearerTokenRefreshHandler callback, along with Confluent Cloud
//      extensions (logicalCluster, identityPoolId) which the broker uses to
//      route the identity through the right Confluent Cloud identity pool.
//
// Why a custom token-refresh handler (instead of librdkafka's built-in OIDC)?
//   librdkafka's OIDC path fetches the token via libcurl, which on Windows
//   uses SChannel and performs strict certificate-revocation checks. Behind a
//   corporate TLS-MITM proxy those checks typically fail (0x80092012
//   CRYPT_E_REVOCATION_OFFLINE) because revocation endpoints are unreachable.
//   .NET's HttpClient (SocketsHttpHandler) honors WinHTTP system-proxy
//   settings and the Windows cert store gracefully, so we do the HTTP call
//   ourselves and hand the token to librdkafka.
//
// Config:
//   appsettings.json (required), appsettings.Development.json (optional, for
//   local overrides), and environment variables (override everything; use
//   double-underscore for section nesting, e.g. Kafka__Topic=foo).
//   The app secret MUST come from the AZURE_CLIENT_SECRET env var — never
//   from config files.
// =============================================================================

using System.Text.Json;                     // JsonDocument — parse the Entra token response
using Confluent.Kafka;                      // ProducerBuilder/ConsumerBuilder/IClient and SASL/SSL config types
using Microsoft.Extensions.Configuration;   // ConfigurationBuilder + Get<T>() section binding

namespace DkDotnetCcClient;

public static class Program
{
    // How many sample messages the producer emits in one run.
    private const int MessageCount = 10;

    // Shared HttpClient for the token endpoint. Static so sockets are reused
    // across producer + consumer token refreshes (avoids socket exhaustion).
    private static readonly HttpClient HttpClient = new();

    public static int Main(string[] args)
    {
        try
        {
            // -----------------------------------------------------------------
            // 1) Build configuration.
            //    Precedence (lowest → highest): appsettings.json <
            //    appsettings.Development.json < environment variables.
            //    SetBasePath uses the executable's directory so appsettings.json
            //    is found whether launched via `dotnet run` or the published exe.
            // -----------------------------------------------------------------
            var config = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false)           // required base file
                .AddJsonFile("appsettings.Development.json", optional: true) // optional local overrides (gitignored)
                .AddEnvironmentVariables()                                   // env vars override file values
                .Build();

            // -----------------------------------------------------------------
            // 2) Bind the two config sections to strongly-typed POCOs.
            //    GetSection(...).Get<T>() returns null only if the section is
            //    completely missing; individual missing properties are checked
            //    in Validate() below.
            // -----------------------------------------------------------------
            var kafka = config.GetSection("Kafka").Get<KafkaSettings>()
                ?? throw new InvalidOperationException("Missing 'Kafka' section in appsettings.json.");
            var azure = config.GetSection("AzureAd").Get<AzureAdSettings>()
                ?? throw new InvalidOperationException("Missing 'AzureAd' section in appsettings.json.");

            // Fail fast if required fields are blank — better than letting the
            // broker return an opaque auth error several seconds later.
            kafka.Validate();
            azure.Validate();

            // -----------------------------------------------------------------
            // 3) Read the app secret from env var ONLY. This keeps it out of
            //    source control, out of `dotnet user-secrets`, and out of any
            //    committed config file.
            // -----------------------------------------------------------------
            var clientSecret = Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET")
                ?? throw new InvalidOperationException("AZURE_CLIENT_SECRET env var is not set.");

            // -----------------------------------------------------------------
            // 4) Produce then consume. Consume runs until Ctrl+C.
            // -----------------------------------------------------------------
            Produce(kafka, azure, clientSecret);
            Consume(kafka, azure, clientSecret);
            return 0;
        }
        catch (Exception ex)
        {
            // Top-level catch so any failure (config, auth, network) is logged
            // once with a nonzero exit code — friendly for CI / shell chaining.
            Console.Error.WriteLine($"Fatal: {ex}");
            return 1;
        }
    }

    // -------------------------------------------------------------------------
    // Produce a batch of MessageCount messages to the configured topic.
    // -------------------------------------------------------------------------
    private static void Produce(KafkaSettings k, AzureAdSettings a, string clientSecret)
    {
        // ProducerConfig only carries what librdkafka needs to connect +
        // negotiate SASL. The *token itself* is supplied later by the refresh
        // handler below — we deliberately omit SaslOauthbearer* fields so
        // librdkafka does NOT try to fetch the token itself (see file header).
        var cfg = new ProducerConfig
        {
            BootstrapServers = k.BootstrapServers,          // Confluent Cloud bootstrap host:port
            SecurityProtocol = SecurityProtocol.SaslSsl,    // TLS + SASL
            SaslMechanism = SaslMechanism.OAuthBearer,      // OAUTHBEARER (token-based), not PLAIN
            Acks = Acks.All,                                // wait for full ISR ack — safest for demos
        };

        // Build the producer and wire up the token refresh callback.
        // librdkafka invokes this handler on its background thread whenever it
        // needs a (new) bearer token, including at startup.
        using var producer = new ProducerBuilder<string, string>(cfg)
            .SetOAuthBearerTokenRefreshHandler((client, _) => RefreshToken(client, k, a, clientSecret))
            .Build();

        // Fire-and-forget produces. The callback below is called per-message
        // once librdkafka gets the delivery report from the broker.
        for (int i = 0; i < MessageCount; i++)
        {
            var key = $"key-{i}";
            var value = $"hello from .NET OIDC producer {i}";

            producer.Produce(k.Topic, new Message<string, string> { Key = key, Value = value },
                report =>
                {
                    // Delivery report: either an error reason or the final
                    // topic/partition/offset the broker assigned.
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

        // Block until all enqueued messages are sent (or 30s elapses). Without
        // this, `using` would dispose the producer while messages are still
        // buffered and they'd be silently dropped.
        producer.Flush(TimeSpan.FromSeconds(30));
    }

    // -------------------------------------------------------------------------
    // Consume from the configured topic until Ctrl+C.
    // -------------------------------------------------------------------------
    private static void Consume(KafkaSettings k, AzureAdSettings a, string clientSecret)
    {
        // Same rationale as ProducerConfig above: no SaslOauthbearer* fields —
        // the refresh handler supplies the token.
        var cfg = new ConsumerConfig
        {
            BootstrapServers = k.BootstrapServers,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.OAuthBearer,
            GroupId = k.GroupId,                            // consumer group — commit offsets here
            AutoOffsetReset = AutoOffsetReset.Earliest,     // if no committed offset, start from beginning
            EnableAutoCommit = true,                        // let the client commit offsets periodically
        };

        // Ctrl+C handler: signal the main loop to exit cleanly. Setting
        // e.Cancel = true prevents the runtime from killing us immediately, so
        // we get to run the `finally` block and close the consumer.
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        // Build consumer and wire up the same token refresh callback.
        using var consumer = new ConsumerBuilder<string, string>(cfg)
            .SetOAuthBearerTokenRefreshHandler((client, _) => RefreshToken(client, k, a, clientSecret))
            .Build();
        consumer.Subscribe(k.Topic);

        try
        {
            // Poll loop. consumer.Consume(token) blocks until a message arrives
            // or the token is cancelled (Ctrl+C), in which case it throws
            // OperationCanceledException and we exit cleanly.
            while (true)
            {
                ConsumeResult<string, string> result;
                try
                {
                    result = consumer.Consume(cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;  // Ctrl+C path
                }

                // Defensive: Consume can return a result with a null Message on
                // control events (partition EOF, etc.) — skip those.
                if (result?.Message == null) continue;

                Console.WriteLine(
                    $"Consumed message from topic {k.Topic}: key = {result.Message.Key} value = {result.Message.Value}");
            }
        }
        finally
        {
            // Close() leaves the group cleanly and commits final offsets —
            // important so the next run doesn't re-read the same records.
            Console.WriteLine("Shutdown signal received; closing consumer.");
            consumer.Close();
        }
    }

    // -------------------------------------------------------------------------
    // RefreshToken — called by librdkafka (on its background thread) whenever
    // a new bearer token is needed.
    //
    // Contract:
    //   - On success: call client.OAuthBearerSetToken(...) with the token,
    //     its absolute expiry (unix ms), a principal name, and any SASL
    //     extensions the broker requires.
    //   - On failure: call client.OAuthBearerSetTokenFailure(error). librdkafka
    //     will retry (and surface the error through its error callback).
    //
    // We MUST NOT let exceptions escape this method — they'd be swallowed by
    // the background thread and the producer/consumer would just look "stuck".
    // -------------------------------------------------------------------------
    private static void RefreshToken(IClient client, KafkaSettings k, AzureAdSettings a, string clientSecret)
    {
        try
        {
            // OAuth 2.0 client-credentials form body.
            // scope=api://<ClientId>/.default requests all app permissions
            // granted to this app registration — standard for app-only tokens.
            var form = new Dictionary<string, string>
            {
                ["client_id"] = a.ClientId,
                ["client_secret"] = clientSecret,
                ["scope"] = $"api://{a.ClientId}/.default",
                ["grant_type"] = "client_credentials",
            };

            // POST to Entra's v2 token endpoint for the configured tenant.
            // HttpClient here uses SocketsHttpHandler, which on Windows honors
            // WinHTTP/IE system-proxy settings and the Windows cert store —
            // the whole reason we're not using librdkafka's built-in OIDC path.
            using var request = new HttpRequestMessage(
                HttpMethod.Post,
                $"https://login.microsoftonline.com/{a.TenantId}/oauth2/v2.0/token")
            {
                Content = new FormUrlEncodedContent(form),
            };

            // Synchronous Send — this method is called on librdkafka's background
            // thread (not an ASP.NET request thread), so sync I/O is acceptable.
            using var response = HttpClient.Send(request);
            response.EnsureSuccessStatusCode();  // throws → caught below → reported via SetTokenFailure

            // Parse the JSON response. Fields we care about:
            //   access_token — the JWT to send to Kafka brokers
            //   expires_in   — seconds until expiry (relative)
            using var stream = response.Content.ReadAsStream();
            using var doc = JsonDocument.Parse(stream);
            var root = doc.RootElement;

            var accessToken = root.GetProperty("access_token").GetString()
                ?? throw new InvalidOperationException("Token response missing 'access_token'.");
            var expiresIn = root.GetProperty("expires_in").GetInt64();

            // librdkafka wants the absolute expiry as unix epoch millis, not a
            // relative duration — convert.
            var lifetimeMs = DateTimeOffset.UtcNow.AddSeconds(expiresIn).ToUnixTimeMilliseconds();

            // Confluent Cloud SASL extensions. These are NOT part of the JWT —
            // they're passed alongside the token in the SASL OAUTHBEARER
            // handshake so the Confluent Cloud broker knows which logical
            // cluster and identity pool this token is being used against.
            // This replaces the librdkafka `sasl.oauthbearer.extensions` config.
            var extensions = new Dictionary<string, string>
            {
                ["logicalCluster"] = k.LogicalCluster,
                ["identityPoolId"] = k.IdentityPoolId,
            };

            // Hand everything to librdkafka. principalName is informational
            // (used in client logs); using ClientId keeps it greppable.
            client.OAuthBearerSetToken(accessToken, lifetimeMs, a.ClientId, extensions);
        }
        catch (Exception ex)
        {
            // Any failure — network, HTTP non-2xx, JSON shape, missing fields —
            // is reported to librdkafka. It will retry on its own cadence and
            // surface the error through its standard error callback.
            client.OAuthBearerSetTokenFailure(ex.ToString());
        }
    }

    // =========================================================================
    // Strongly-typed config sections bound from appsettings.json.
    // Each property name matches the JSON key under its section.
    // =========================================================================

    // Mirrors the "Kafka" section in appsettings.json.
    private sealed class KafkaSettings
    {
        public string BootstrapServers { get; set; } = "";  // Confluent Cloud bootstrap host:port
        public string LogicalCluster { get; set; } = "";    // CC logical cluster id (e.g. lkc-...)
        public string IdentityPoolId { get; set; } = "";    // CC identity pool id (e.g. pool-...)
        public string Topic { get; set; } = "";             // topic to produce + consume
        public string GroupId { get; set; } = "";           // consumer group id

        // Throw on any missing/blank required value so the failure message
        // points at the exact config key instead of a downstream auth error.
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

    // Mirrors the "AzureAd" section in appsettings.json.
    // ClientSecret is intentionally NOT here — it comes from the env var.
    private sealed class AzureAdSettings
    {
        public string TenantId { get; set; } = "";  // Entra tenant (directory) GUID
        public string ClientId { get; set; } = "";  // Entra app registration (application) GUID

        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(TenantId)) throw Missing(nameof(TenantId));
            if (string.IsNullOrWhiteSpace(ClientId)) throw Missing(nameof(ClientId));
        }

        private static InvalidOperationException Missing(string name)
            => new($"AzureAd:{name} is not set.");
    }
}
