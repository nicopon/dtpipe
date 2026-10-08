using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Cli.Studio;

/// <summary>Why the studio could not reach or use its coordinator, in words a person acts on.</summary>
public sealed class CoordinatorLinkException(string message) : Exception(message);

/// <summary>
/// What the page is told of the link: the coordinator's address, whether a person is signed in, why
/// not, and who. Never a token.
/// </summary>
public sealed record CoordinatorStatus(string Url, string State, string? Reason, CoordinatorMe? Me, string StudioVersion, int Contract);

/// <summary>
/// The studio's link to one coordinator, named on the command line: the page never chooses where a
/// token goes. A person signs in the way an MCP client does: the coordinator's protected resource
/// metadata (RFC 9728) names its provider and its studio client, the provider's discovery document its
/// endpoints, then authorization code with PKCE (S256) through the browser, back to this studio's
/// loopback <c>/callback</c> (RFC 8252). The tokens live in this process's memory only, never in a
/// response or the page; the access token is renewed ahead of its expiry with the refresh token, and a
/// refused renewal signs the person out with the reason. Every request goes to the coordinator's
/// origin only, follows no redirect and carries the CSRF header the coordinator requires.
/// </summary>
public sealed class CoordinatorLink : IDisposable
{
    public const string Callback = "/callback";

    /// <summary>How long before its expiry an access token is renewed.</summary>
    public static readonly TimeSpan Margin = TimeSpan.FromMinutes(2);

    /// <summary>How long a sign-in started from the page may take to come back.</summary>
    public static readonly TimeSpan PendingLifetime = TimeSpan.FromMinutes(10);

    public const string SignedOut = "signed-out";
    public const string SigningIn = "signing-in";
    public const string SignedIn = "signed-in";

    private const string Scopes = "openid email profile offline_access";

    private sealed record Discovery(string Issuer, string AuthorizationEndpoint, string TokenEndpoint, string ClientId);

    private sealed record Pending(string State, string Verifier, string RedirectUri, DateTimeOffset Expires);

    private sealed record Tokens(string Access, string? Refresh, DateTimeOffset Expires);

    private sealed record OpenIdConfiguration(
        [property: JsonPropertyName("issuer")] string? Issuer,
        [property: JsonPropertyName("authorization_endpoint")] string? AuthorizationEndpoint,
        [property: JsonPropertyName("token_endpoint")] string? TokenEndpoint);

    private sealed record TokenResponse(
        [property: JsonPropertyName("access_token")] string? AccessToken,
        [property: JsonPropertyName("refresh_token")] string? RefreshToken,
        [property: JsonPropertyName("expires_in")] int? ExpiresIn);

    private readonly Uri _origin;
    private readonly HttpClient _http;
    private readonly TimeProvider _time;
    private readonly SemaphoreSlim _renewal = new(1, 1);
    private readonly object _lock = new();
    private Discovery? _discovery;
    private Pending? _pending;
    private Tokens? _tokens;
    private CoordinatorMe? _me;
    private string? _reason;

    /// <param name="coordinator">The coordinator's address: its origin is the only one a token is sent to.</param>
    public CoordinatorLink(Uri coordinator, TimeProvider? time = null)
    {
        if (!coordinator.IsAbsoluteUri || coordinator.Scheme is not ("http" or "https") || coordinator.UserInfo.Length > 0
            || coordinator.AbsolutePath != "/" || coordinator.Query.Length > 0 || coordinator.Fragment.Length > 0)
            throw new CoordinatorLinkException($"'{coordinator}' is no coordinator address: give its origin, as http(s)://host[:port].");
        _origin = new Uri(coordinator.GetLeftPart(UriPartial.Authority));
        _time = time ?? TimeProvider.System;
        _http = new HttpClient(new SocketsHttpHandler { AllowAutoRedirect = false, UseCookies = false }) { Timeout = TimeSpan.FromSeconds(30) };
    }

    public Uri Origin => _origin;

    /// <summary>This dtpipe's version, as <c>dtpipe --version</c> prints it.</summary>
    public static string StudioVersion { get; } =
        typeof(CoordinatorLink).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "unknown";

    public CoordinatorStatus Status()
    {
        lock (_lock)
        {
            var state = _tokens is not null ? SignedIn : _pending is not null && _time.GetUtcNow() < _pending.Expires ? SigningIn : SignedOut;
            return new CoordinatorStatus(Url, state, _reason, _tokens is null ? null : _me, StudioVersion, CoordinatorApi.Contract);
        }
    }

    private string Url => _origin.ToString().TrimEnd('/');

    /// <summary>
    /// Starts a sign-in: the address the page opens at the provider. One sign-in waits at a time; a new
    /// one replaces it.
    /// </summary>
    public async Task<string> BeginSignInAsync(int studioPort, CancellationToken ct)
    {
        var discovery = await DiscoverAsync(ct);
        var verifier = Base64Url(RandomNumberGenerator.GetBytes(32));
        var state = Base64Url(RandomNumberGenerator.GetBytes(24));
        var redirect = $"http://127.0.0.1:{studioPort}{Callback}";
        lock (_lock) _pending = new Pending(state, verifier, redirect, _time.GetUtcNow() + PendingLifetime);
        var query = string.Join('&', new Dictionary<string, string>
        {
            ["client_id"] = discovery.ClientId,
            ["response_type"] = "code",
            ["redirect_uri"] = redirect,
            ["scope"] = Scopes,
            ["state"] = state,
            ["code_challenge"] = Base64Url(SHA256.HashData(Encoding.ASCII.GetBytes(verifier))),
            ["code_challenge_method"] = "S256",
        }.Select(kv => $"{kv.Key}={Uri.EscapeDataString(kv.Value)}"));
        return discovery.AuthorizationEndpoint + (discovery.AuthorizationEndpoint.Contains('?') ? "&" : "?") + query;
    }

    /// <summary>
    /// The provider's answer, brought back by the browser to <c>/callback</c>: the code is exchanged here,
    /// server to server. Returns who signed in. The <paramref name="state"/> must be the one waiting, used
    /// once and in time; an unknown one leaves the waiting sign-in as it was.
    /// </summary>
    public async Task<string> CompleteSignInAsync(string? code, string? state, string? issuer, string? error, CancellationToken ct)
    {
        Pending pending;
        lock (_lock)
        {
            if (_pending is null || state is null
                || !CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(state), Encoding.UTF8.GetBytes(_pending.State)))
                throw new CoordinatorLinkException("This sign-in is unknown or was already used: start it again from the studio.");
            pending = _pending;
            _pending = null;
        }
        if (_time.GetUtcNow() >= pending.Expires)
            throw new CoordinatorLinkException("This sign-in took too long: start it again from the studio.");
        if (error is not null) throw SignOut($"The identity provider did not sign you in: {error}.");
        var discovery = await DiscoverAsync(ct);
        // RFC 9207: an answer that names an issuer must name ours, or it comes from another provider.
        if (issuer is not null && !SameIssuer(issuer, discovery.Issuer))
            throw SignOut("The answer names another identity provider than the coordinator's: sign in again.");
        if (string.IsNullOrEmpty(code)) throw SignOut("The identity provider sent no code: sign in again.");

        var tokens = await RequestTokensAsync(discovery, new Dictionary<string, string>
        {
            ["grant_type"] = "authorization_code",
            ["code"] = code,
            ["redirect_uri"] = pending.RedirectUri,
            ["client_id"] = discovery.ClientId,
            ["code_verifier"] = pending.Verifier,
        }, ct) ?? throw SignOut("The identity provider refused the code: sign in again.");
        lock (_lock)
        {
            _tokens = tokens;
            _reason = null;
            _me = null;
        }

        using var response = await SendAsync(HttpMethod.Get, CoordinatorApi.Me, ct: ct);
        if (!response.IsSuccessStatusCode)
            throw SignOut($"{Url} refused this person ({(int)response.StatusCode}).");
        var me = await response.Content.ReadFromJsonAsync<CoordinatorMe>(JsonSerializerOptions.Web, ct);
        lock (_lock) _me = me;
        return me?.Email ?? me?.Name ?? "a person the coordinator does not name";
    }

    /// <summary>Forgets the tokens: the person signs in again to reach the coordinator.</summary>
    public void SignOutNow()
    {
        lock (_lock)
        {
            _tokens = null;
            _me = null;
            _pending = null;
            _reason = null;
        }
    }

    /// <summary>
    /// A request to the coordinator's <paramref name="path"/>, as the person signed in. A 401 renews the
    /// access token once and sends again once; a second 401 signs the person out. A redirect is
    /// returned as it came, never followed.
    /// </summary>
    public async Task<HttpResponseMessage> SendAsync(HttpMethod method, string path, byte[]? body = null, string contentType = "application/json",
        IReadOnlyDictionary<string, string>? headers = null, CancellationToken ct = default)
    {
        if (!path.StartsWith(CoordinatorApi.Prefix + "/", StringComparison.Ordinal))
            throw new ArgumentException($"'{path}' is not a route of the coordinator's API.", nameof(path));
        var uri = new Uri(_origin, path);
        if (uri.GetLeftPart(UriPartial.Authority) != _origin.GetLeftPart(UriPartial.Authority))
            throw new ArgumentException($"'{path}' leaves the coordinator's origin.", nameof(path));

        for (var attempt = 0; ; attempt++)
        {
            var token = await AccessTokenAsync(fresh: attempt > 0, ct);
            using var request = new HttpRequestMessage(method, uri);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Headers.Add(CoordinatorApi.CsrfHeader, "1");
            foreach (var (name, value) in headers ?? new Dictionary<string, string>()) request.Headers.TryAddWithoutValidation(name, value);
            if (body is not null)
            {
                request.Content = new ByteArrayContent(body);
                request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(contentType);
            }
            var response = await _http.SendAsync(request, ct);
            if (response.StatusCode != HttpStatusCode.Unauthorized) return response;
            response.Dispose();
            if (attempt > 0) throw SignOut($"{Url} no longer accepts this session: sign in again.");
        }
    }

    private async Task<string> AccessTokenAsync(bool fresh, CancellationToken ct)
    {
        await _renewal.WaitAsync(ct);
        try
        {
            Tokens tokens;
            lock (_lock) tokens = _tokens ?? throw new CoordinatorLinkException(_reason ?? $"Nobody is signed in to {Url}: sign in from the studio.");
            if (!fresh && _time.GetUtcNow() < tokens.Expires - Margin) return tokens.Access;
            if (tokens.Refresh is null) throw SignOut("The session has ended: sign in again.");

            var discovery = await DiscoverAsync(ct);
            var renewed = await RequestTokensAsync(discovery, new Dictionary<string, string>
            {
                ["grant_type"] = "refresh_token",
                ["refresh_token"] = tokens.Refresh,
                ["client_id"] = discovery.ClientId,
            }, ct) ?? throw SignOut("The identity provider no longer renews this session: sign in again.");
            // A provider that rotates refresh tokens sends a new one; the one spent is never sent again.
            lock (_lock) _tokens = renewed with { Refresh = renewed.Refresh ?? tokens.Refresh };
            return renewed.Access;
        }
        finally
        {
            _renewal.Release();
        }
    }

    /// <summary>Tokens from the provider's token endpoint, or null when it refuses the grant.</summary>
    private async Task<Tokens?> RequestTokensAsync(Discovery discovery, Dictionary<string, string> form, CancellationToken ct)
    {
        using var response = await _http.PostAsync(discovery.TokenEndpoint, new FormUrlEncodedContent(form), ct);
        if (response.StatusCode is HttpStatusCode.BadRequest or HttpStatusCode.Unauthorized) return null;
        if (!response.IsSuccessStatusCode)
            throw new CoordinatorLinkException($"The identity provider's token endpoint answered {(int)response.StatusCode}.");
        var answer = await response.Content.ReadFromJsonAsync<TokenResponse>(ct);
        if (answer?.AccessToken is not { Length: > 0 } access)
            throw new CoordinatorLinkException("The identity provider sent no access token.");
        return new Tokens(access, answer.RefreshToken, _time.GetUtcNow().AddSeconds(Math.Max(answer.ExpiresIn ?? 300, 0)));
    }

    private async Task<Discovery> DiscoverAsync(CancellationToken ct)
    {
        lock (_lock)
        {
            if (_discovery is not null) return _discovery;
        }
        var metadata = await GetJsonAsync<ResourceMetadata>(new Uri(_origin, CoordinatorApi.Metadata).ToString(), Url, ct);
        if (metadata.DtpipeContract != CoordinatorApi.Contract)
            throw new CoordinatorLinkException(
                $"{Url} speaks contract {metadata.DtpipeContract} of the studio's API, this dtpipe contract {CoordinatorApi.Contract}: update the older of the two.");
        if (!SameIssuer(metadata.Resource ?? "", Url))
            throw new CoordinatorLinkException($"{Url} names itself {metadata.Resource}: open the studio with that address.");
        if (string.IsNullOrEmpty(metadata.StudioClientId))
            throw new CoordinatorLinkException($"{Url} accepts no studio: its administrator sets Coordinator:Studio:ClientId.");
        var issuer = metadata.AuthorizationServers?.FirstOrDefault()
            ?? throw new CoordinatorLinkException($"{Url} names no identity provider.");
        if (!Uri.TryCreate(issuer, UriKind.Absolute, out var issuerUri) || issuerUri.Scheme is not ("http" or "https"))
            throw new CoordinatorLinkException($"{Url} names '{issuer}' as its identity provider, which is no address.");
        // No downgrade: a coordinator reached over HTTPS signs people in over HTTPS only.
        if (_origin.Scheme == "https" && issuerUri.Scheme != "https")
            throw new CoordinatorLinkException($"{Url} is reached over HTTPS but names an identity provider over HTTP: refused.");

        var configuration = await GetJsonAsync<OpenIdConfiguration>(issuer.TrimEnd('/') + "/.well-known/openid-configuration", issuer, ct);
        // RFC 8414, section 3.3: the document must be the issuer's own. A trailing slash is the only difference allowed.
        if (configuration.Issuer is null || !SameIssuer(configuration.Issuer, issuer))
            throw new CoordinatorLinkException($"The identity provider at {issuer} names itself {configuration.Issuer}: refused.");
        foreach (var endpoint in new[] { configuration.AuthorizationEndpoint, configuration.TokenEndpoint })
        {
            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) || (issuerUri.Scheme == "https" && uri.Scheme != "https"))
                throw new CoordinatorLinkException($"The identity provider at {issuer} gives an endpoint that is no address over {issuerUri.Scheme}: '{endpoint}'.");
        }
        var discovery = new Discovery(configuration.Issuer, configuration.AuthorizationEndpoint!, configuration.TokenEndpoint!, metadata.StudioClientId);
        lock (_lock) _discovery = discovery;
        return discovery;
    }

    private async Task<T> GetJsonAsync<T>(string url, string who, CancellationToken ct)
    {
        HttpResponseMessage response;
        try { response = await _http.GetAsync(url, ct); }
        catch (HttpRequestException ex) { throw new CoordinatorLinkException($"{who} does not answer: {ex.Message}"); }
        using (response)
        {
            if (!response.IsSuccessStatusCode)
                throw new CoordinatorLinkException($"{url} answered {(int)response.StatusCode}: {who} is not one this studio can sign in to.");
            try
            {
                return await response.Content.ReadFromJsonAsync<T>(ct) ?? throw new JsonException("empty");
            }
            catch (JsonException)
            {
                throw new CoordinatorLinkException($"{url} is not the document this studio reads.");
            }
        }
    }

    /// <summary>Forgets the tokens and keeps why, for the page; returns the exception to throw.</summary>
    private CoordinatorLinkException SignOut(string reason)
    {
        lock (_lock)
        {
            _tokens = null;
            _me = null;
            _reason = reason;
        }
        return new CoordinatorLinkException(reason);
    }

    private static bool SameIssuer(string a, string b) => string.Equals(a.TrimEnd('/'), b.TrimEnd('/'), StringComparison.Ordinal);

    private static string Base64Url(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    public void Dispose()
    {
        _http.Dispose();
        _renewal.Dispose();
    }
}
