using Azure.Core;
using Azure.Identity;

namespace KafkaOidcClient;

/// <summary>
/// Provides OAuth tokens from Microsoft Entra ID for Kafka authentication.
/// </summary>
public class OAuthTokenProvider
{
    private readonly ClientSecretCredential _credential;
    private readonly string _scope;

    public OAuthTokenProvider(string tenantId, string clientId, string clientSecret, string scope)
    {
        _credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
        _scope = scope;
    }

    /// <summary>
    /// Acquires an access token from Microsoft Entra ID.
    /// </summary>
    public async Task<(string Token, long ExpiresInMs)> GetTokenAsync()
    {
        var tokenRequestContext = new TokenRequestContext(new[] { _scope });
        var token = await _credential.GetTokenAsync(tokenRequestContext);

        // Calculate expiry in milliseconds from now
        var expiresInMs = (long)(token.ExpiresOn - DateTimeOffset.UtcNow).TotalMilliseconds;

        return (token.Token, expiresInMs);
    }

    /// <summary>
    /// Synchronous wrapper for OAuth callback.
    /// </summary>
    public (string Token, long ExpiresInMs) GetToken()
    {
        return GetTokenAsync().GetAwaiter().GetResult();
    }
}
