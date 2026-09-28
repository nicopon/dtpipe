using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace DtPipe.Lab.NodeHost;

/// <summary>
/// Obtains this node's tokens from the coordinator's embedded IDP (client credentials), and reuses
/// one until shortly before it expires. The group a token carries is decided by the IDP, not here.
/// </summary>
public sealed class IdpTokenClient(NodeHostOptions options, NodeConfig config) : IDisposable
{
    private static readonly TimeSpan Margin = TimeSpan.FromMinutes(2);

    private sealed record TokenResponse(
        [property: JsonPropertyName("access_token")] string AccessToken,
        [property: JsonPropertyName("expires_in")] int ExpiresIn);

    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(15) };
    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _token;
    private DateTimeOffset _expires;

    public string ClientId => config.ClientId ?? config.Name;

    public async Task<string> GetAsync(bool fresh = false)
    {
        await _gate.WaitAsync();
        try
        {
            if (!fresh && _token is not null && DateTimeOffset.UtcNow < _expires - Margin) return _token;
            using var response = await _http.PostAsync(options.CoordinatorUrl + "/connect/token", new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials",
                ["client_id"] = ClientId,
                ["client_secret"] = config.Secret ?? "",
            }));
            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"The coordinator's IDP refused {ClientId}: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
            var token = await response.Content.ReadFromJsonAsync<TokenResponse>() ?? throw new InvalidOperationException("Empty token response.");
            (_token, _expires) = (token.AccessToken, DateTimeOffset.UtcNow.AddSeconds(token.ExpiresIn));
            return _token;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>
    /// The hub URL a <c>PipelineNode</c> is given: <c>PipelineNodeOptions</c> has no credential, so
    /// the token rides in the URL's path, where the coordinator turns it back into a bearer header.
    /// </summary>
    public async Task<string> HubUrlAsync() => $"{options.CoordinatorUrl}/t/{await GetAsync()}";

    public void Dispose() => _http.Dispose();
}
