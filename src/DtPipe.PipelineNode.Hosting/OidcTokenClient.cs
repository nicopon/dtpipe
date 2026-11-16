using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>The provider answered and refused the client: no identity of that name holds this secret there. The coordinator is reachable.</summary>
internal sealed class TokenRefusedException(string clientId, string provider, string reason)
    : InvalidOperationException($"The identity provider refused {clientId}: {reason}")
{
    public string ClientId { get; } = clientId;

    /// <summary>The provider's address (scheme, host and port): where the client is judged.</summary>
    public string Provider { get; } = provider;

    public string Reason { get; } = reason;
}

/// <summary>
/// Obtains this node's tokens from its OpenID Connect provider (client credentials), and reuses one
/// until shortly before it expires; <c>fresh</c> asks for a new one, which is what TransportR's retry
/// after a 401 needs. The provider is <c>Oidc:Authority</c> when given, else the one the coordinator's protected resource metadata names;
/// its token endpoint comes from its discovery document, read once. The client is <c>Oidc:ClientId</c> when given, else the node's name, and
/// its secret <c>Oidc:ClientSecret</c> or <c>Oidc:ClientSecretFile</c>, read once when the client is made (<see cref="SecretReference"/>), or what a provider gives each time a token is requested. What the token entitles the node
/// to is decided by the coordinator, never here.
/// </summary>
internal sealed class OidcTokenClient(NodeOidcOptions options, string nodeName, string coordinatorUrl) : IDisposable
{
    private static readonly TimeSpan Margin = TimeSpan.FromMinutes(2);

    private readonly Func<CancellationToken, ValueTask<string>> _secret = SecretOf(options);

    // A value or a file is read once, now: a start that cannot read it stops by the setting's name. A provider is asked each time a
    // token is requested, so a rotated key is the one a 401's retry presents. What it fails with is kept as the inner exception, out
    // of the message.
    private static Func<CancellationToken, ValueTask<string>> SecretOf(NodeOidcOptions options)
    {
        if (options.ClientSecretProvider is { } provider)
            return async ct =>
            {
                string secret;
                try
                {
                    secret = await provider(ct);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"The Oidc client secret provider failed: {ex.GetType().Name}.", ex);
                }
                return string.IsNullOrEmpty(secret) ? throw new InvalidOperationException("The Oidc client secret provider returned an empty secret.") : secret;
            };
        var value = string.IsNullOrEmpty(options.ClientSecretFile)
            ? SecretReference.Read(options.ClientSecret, "DtPipeNode:Oidc:ClientSecret")
            : SecretReference.ReadFile(options.ClientSecretFile, "DtPipeNode:Oidc:ClientSecretFile");
        return _ => ValueTask.FromResult(value);
    }

    private sealed record Discovery([property: JsonPropertyName("token_endpoint")] string TokenEndpoint);

    private sealed record ResourceMetadata([property: JsonPropertyName("authorization_servers")] string[]? AuthorizationServers);

    private sealed record TokenResponse(
        [property: JsonPropertyName("access_token")] string AccessToken,
        [property: JsonPropertyName("expires_in")] int ExpiresIn);

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(15) };
    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _tokenEndpoint;
    private string? _token;
    private DateTimeOffset _expires;

    public string ClientId => string.IsNullOrWhiteSpace(options.ClientId) ? nodeName : options.ClientId;

    public async Task<string> GetAsync(bool fresh = false, CancellationToken ct = default)
    {
        await _gate.WaitAsync(ct);
        try
        {
            if (!fresh && _token is not null && DateTimeOffset.UtcNow < _expires - Margin) return _token;
            _tokenEndpoint ??= await DiscoverAsync();

            var form = new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials",
                ["client_id"] = ClientId,
                ["client_secret"] = await _secret(ct),
            };
            if (!string.IsNullOrWhiteSpace(options.Scope)) form["scope"] = options.Scope;
            using var response = await _http.PostAsync(_tokenEndpoint, new FormUrlEncodedContent(form));
            if (!response.IsSuccessStatusCode)
                throw new TokenRefusedException(ClientId, new Uri(_tokenEndpoint!).GetLeftPart(UriPartial.Authority), Describe((int)response.StatusCode, await response.Content.ReadAsStringAsync()));
            var token = await response.Content.ReadFromJsonAsync<TokenResponse>() ?? throw new InvalidOperationException("Empty token response.");
            (_token, _expires) = (token.AccessToken, DateTimeOffset.UtcNow.AddSeconds(token.ExpiresIn));
            return _token;
        }
        finally
        {
            _gate.Release();
        }
    }

    // An OAuth error answer reads "error: description"; any other body is shown as it came.
    private static string Describe(int status, string body)
    {
        try
        {
            using var document = JsonDocument.Parse(body);
            var root = document.RootElement;
            if (root.TryGetProperty("error", out var error) && error.GetString() is { Length: > 0 } code)
                return root.TryGetProperty("error_description", out var description) && description.GetString() is { Length: > 0 } text
                    ? $"{code}: {text}"
                    : code;
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException) { }
        return $"{status} {body}";
    }

    private async Task<string> DiscoverAsync()
    {
        var authority = string.IsNullOrWhiteSpace(options.Authority) ? await AuthorityOfCoordinatorAsync() : options.Authority;
        var url = authority.TrimEnd('/') + "/.well-known/openid-configuration";
        var discovery = await _http.GetFromJsonAsync<Discovery>(url)
            ?? throw new InvalidOperationException($"{url}: empty discovery document.");
        return discovery.TokenEndpoint;
    }

    // The node posts its secret to whatever this names: a coordinator reached over HTTPS names a provider over HTTPS only.
    private async Task<string> AuthorityOfCoordinatorAsync()
    {
        var url = coordinatorUrl.TrimEnd('/') + "/.well-known/oauth-protected-resource";
        var metadata = await _http.GetFromJsonAsync<ResourceMetadata>(url) ?? throw new InvalidOperationException($"{url}: empty document.");
        var issuer = metadata.AuthorizationServers?.FirstOrDefault()
            ?? throw new InvalidOperationException($"{coordinatorUrl} names no identity provider: set Oidc:Authority.");
        if (!Uri.TryCreate(issuer, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
            throw new InvalidOperationException($"{coordinatorUrl} names '{issuer}' as its identity provider, which is no address.");
        if (coordinatorUrl.StartsWith("https://", StringComparison.OrdinalIgnoreCase) && uri.Scheme != "https")
            throw new InvalidOperationException($"{coordinatorUrl} is reached over HTTPS but names an identity provider over HTTP: refused.");
        return issuer;
    }

    public void Dispose() => _http.Dispose();
}
