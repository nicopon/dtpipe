using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// Obtains this node's tokens from its OpenID Connect provider (client credentials), and reuses one
/// until shortly before it expires; <c>fresh</c> asks for a new one, which is what TransportR's retry
/// after a 401 needs. The token endpoint comes from the provider's discovery document, read once. What
/// the token entitles the node to is decided by the coordinator, never here.
/// </summary>
internal sealed class OidcTokenClient(NodeOidcOptions options) : IDisposable
{
    private static readonly TimeSpan Margin = TimeSpan.FromMinutes(2);

    private sealed record Discovery([property: JsonPropertyName("token_endpoint")] string TokenEndpoint);

    private sealed record TokenResponse(
        [property: JsonPropertyName("access_token")] string AccessToken,
        [property: JsonPropertyName("expires_in")] int ExpiresIn);

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(15) };
    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _tokenEndpoint;
    private string? _token;
    private DateTimeOffset _expires;

    public string ClientId => options.ClientId;

    public async Task<string> GetAsync(bool fresh = false)
    {
        await _gate.WaitAsync();
        try
        {
            if (!fresh && _token is not null && DateTimeOffset.UtcNow < _expires - Margin) return _token;
            _tokenEndpoint ??= await DiscoverAsync();

            var form = new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials",
                ["client_id"] = options.ClientId,
                ["client_secret"] = options.ClientSecret,
            };
            if (!string.IsNullOrWhiteSpace(options.Scope)) form["scope"] = options.Scope;
            using var response = await _http.PostAsync(_tokenEndpoint, new FormUrlEncodedContent(form));
            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"The identity provider refused {options.ClientId}: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
            var token = await response.Content.ReadFromJsonAsync<TokenResponse>() ?? throw new InvalidOperationException("Empty token response.");
            (_token, _expires) = (token.AccessToken, DateTimeOffset.UtcNow.AddSeconds(token.ExpiresIn));
            return _token;
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<string> DiscoverAsync()
    {
        var url = options.Authority.TrimEnd('/') + "/.well-known/openid-configuration";
        var discovery = await _http.GetFromJsonAsync<Discovery>(url)
            ?? throw new InvalidOperationException($"{url}: empty discovery document.");
        return discovery.TokenEndpoint;
    }

    public void Dispose() => _http.Dispose();
}
