using System.Globalization;
using System.Security.Cryptography;
using System.Threading.RateLimiting;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.DataProtection.Repositories;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The coordinator's issuer for the identities verified by a key (<see cref="CoordinatorServerOptions.KeyIssuer"/>): a key is the client
/// secret of a client-credentials grant, exchanged at <c>/connect/token</c> for a short token the server then trusts like any other, by
/// its issuer (<see cref="TrustedIdp"/>). One grant, no other: no password, no authorization code, no refresh token. A node finds the
/// token endpoint through the discovery document, as it would at any provider, so it needs no code of its own. The record's kind
/// decides the token's shape: a service's names itself as its client (<c>sub == client_id</c>), a person's names the studio's client
/// (<c>client_id</c> = <c>Coordinator:Studio:ClientId</c>, <c>sub</c> = its id), so the claims that tell a person from a service are made
/// here, from the rights, never taken from the caller.
/// </summary>
public static class KeyIssuer
{
    public const string DiscoveryPath = "/.well-known/openid-configuration";
    public const string JwksPath = "/.well-known/jwks.json";
    public const string TokenPath = "/connect/token";

    /// <summary>The largest form the token endpoint reads: three short fields.</summary>
    private const int MaxForm = 4096;

    public static IServiceCollection AddKeyIssuer(this IServiceCollection services)
    {
        services.AddSingleton<IssuerKeys>();
        services.AddSingleton<KeyExchangeLimits>();
        return services;
    }

    /// <summary>Why this server cannot verify an identity by a key, or null when it can.</summary>
    public static string? Unavailable(CoordinatorServerOptions server) =>
        server.KeyIssuer is not null ? null
        : server.IsTrialIdp
            ? "the trial identity provider issues no token for a key, and it serves the paths and the issuer of the coordinator's key issuer, so the two never run together"
            : "this coordinator issues the tokens of a key at Coordinator:PublicUrl, which is not set";

    /// <summary>Why this server signs no person in by a key, or null when it does.</summary>
    public static string? PersonUnavailable(CoordinatorServerOptions server) =>
        Unavailable(server) ?? (server.PersonKeys ? null : "Coordinator:Auth:PersonKeys is false, so people sign in through the identity provider only");

    /// <summary>
    /// Stops the start when the rights hold an identity verified by a key and this server issues no token for one, or a person while
    /// people may not be verified by a key: the identity could never connect, and nothing else would say why.
    /// </summary>
    public static void CheckIdentities(RightsStore rights, CoordinatorServerOptions server)
    {
        var identities = rights.Snapshot().Identities;
        if (Unavailable(server) is { } reason && identities.Where(i => i.Auth.Mode == AuthModes.Key).Select(i => i.ClientId).ToList() is { Count: > 0 } keyed)
            throw new InvalidOperationException($"The rights hold identities verified by a key ({string.Join(", ", keyed)}), but {reason}.");
        if (PersonUnavailable(server) is { } refusal && identities.Where(i => i.IsPerson).Select(i => i.ClientId).ToList() is { Count: > 0 } people)
            throw new InvalidOperationException($"The rights hold people verified by a key ({string.Join(", ", people)}), but {refusal}.");
    }

    public static void MapKeyIssuer(this WebApplication app, CoordinatorServerOptions server)
    {
        var issuer = server.KeyIssuer!;
        app.MapGet(DiscoveryPath, () => new Discovery(issuer, issuer + TokenPath, issuer + JwksPath, ["client_credentials"], ["client_secret_post"]));
        app.MapGet(JwksPath, (IssuerKeys keys) => keys.Jwks());
        app.MapPost(TokenPath, ExchangeAsync);
    }

    /// <summary>
    /// A key for a token. Every answer about the client is the same <c>invalid_client</c>, whether the key names no identity, is wrong,
    /// revoked or expired, opens a disabled identity or one the IdP verifies, opens a person while no studio's client is set or people
    /// may not be verified by a key, or the client id is not the one the key names; a key, and a client id that could hold one, are
    /// written in no log.
    /// </summary>
    private static async Task<IResult> ExchangeAsync(
        HttpContext http, CoordinatorServerOptions server, RightsStore rights, IssuerKeys keys, KeyExchangeLimits limits, ILogger<IssuerKeys> logger)
    {
        http.Response.Headers.CacheControl = "no-store";
        http.Response.Headers.Pragma = "no-cache";
        if (!server.ExchangesKeys)
            return Error(StatusCodes.Status400BadRequest, "invalid_request",
                "This coordinator exchanges a key over HTTPS only: Coordinator:PublicUrl is http, and Coordinator:Auth:AllowHttpKeys is not set.");
        var source = http.Connection.RemoteIpAddress?.ToString() ?? "";
        using (var lease = limits.FromSource(source))
            if (!lease.IsAcquired) return TooMany(http);
        if (!http.Request.HasFormContentType || http.Request.ContentLength is not (> 0 and <= MaxForm))
            return Error(StatusCodes.Status400BadRequest, "invalid_request", "A token request is a form of grant_type, client_id and client_secret.");
        IFormCollection form;
        try { form = await http.Request.ReadFormAsync(http.RequestAborted); }
        catch (InvalidDataException) { return Error(StatusCodes.Status400BadRequest, "invalid_request", "The token request's form cannot be read."); }
        if (form["grant_type"] != "client_credentials")
            return Error(StatusCodes.Status400BadRequest, "unsupported_grant_type", "This issuer grants client_credentials only.");
        var clientId = form["client_id"].ToString();
        var named = PipelineLibrary.IsValidId(clientId) ? clientId : null;
        using (var lease = limits.ForIdentity(named ?? ""))
            if (!lease.IsAcquired) return TooMany(http);
        var now = DateTimeOffset.UtcNow;
        if (named is null || rights.KeyOf(form["client_secret"].ToString(), now) is not ({ } identity, { } key) || identity.ClientId != named
            || identity.IsPerson && (!server.PersonKeys || server.StudioClientId is null))
        {
            logger.LogWarning("A key exchange for {ClientId} from {Source} was refused", named ?? "an invalid client id", source);
            return Error(StatusCodes.Status400BadRequest, "invalid_client", "The client is not known, or its key does not open it.");
        }
        if (identity.IsPerson)
        {
            var personLifetime = server.PersonTokenLifetime;
            return Results.Json(new TokenAnswer(
                keys.IssuePerson(identity.ClientId, server.StudioClientId!, key.Id, server.KeyIssuer!, server.Audience, now, personLifetime),
                "Bearer", (int)personLifetime.TotalSeconds));
        }
        var lifetime = server.ServiceTokenLifetime;
        return Results.Json(new TokenAnswer(keys.Issue(identity.ClientId, server.KeyIssuer!, server.Audience, now, lifetime), "Bearer", (int)lifetime.TotalSeconds));
    }

    private static IResult TooMany(HttpContext http)
    {
        http.Response.Headers.RetryAfter = "60";
        return Error(StatusCodes.Status429TooManyRequests, "invalid_request", "Too many token requests: try again in a minute.");
    }

    private static IResult Error(int status, string error, string description) =>
        Results.Json(new TokenError(error, description), statusCode: status);

    private sealed record Discovery(
        [property: System.Text.Json.Serialization.JsonPropertyName("issuer")] string Issuer,
        [property: System.Text.Json.Serialization.JsonPropertyName("token_endpoint")] string TokenEndpoint,
        [property: System.Text.Json.Serialization.JsonPropertyName("jwks_uri")] string JwksUri,
        [property: System.Text.Json.Serialization.JsonPropertyName("grant_types_supported")] IReadOnlyList<string> GrantTypes,
        [property: System.Text.Json.Serialization.JsonPropertyName("token_endpoint_auth_methods_supported")] IReadOnlyList<string> AuthMethods);

    private sealed record TokenAnswer(
        [property: System.Text.Json.Serialization.JsonPropertyName("access_token")] string AccessToken,
        [property: System.Text.Json.Serialization.JsonPropertyName("token_type")] string TokenType,
        [property: System.Text.Json.Serialization.JsonPropertyName("expires_in")] int ExpiresIn);

    private sealed record TokenError(
        [property: System.Text.Json.Serialization.JsonPropertyName("error")] string Error,
        [property: System.Text.Json.Serialization.JsonPropertyName("error_description")] string Description);
}

/// <summary>
/// How often a key may be presented: per source address and per client id, in fixed windows of a minute, whether or not the client id
/// is an identity's, so a refusal says nothing of which exist. The server reads no forwarded header, so behind a proxy every caller is
/// one source and its limit is the fleet's: a node asks once per token, a few minutes apart.
/// </summary>
public sealed class KeyExchangeLimits : IDisposable
{
    public const int PerSource = 300;
    public const int PerIdentity = 30;

    private readonly PartitionedRateLimiter<string> _bySource = Limiter(PerSource);
    private readonly PartitionedRateLimiter<string> _byIdentity = Limiter(PerIdentity);

    private static PartitionedRateLimiter<string> Limiter(int permits) => PartitionedRateLimiter.Create<string, string>(key =>
        RateLimitPartition.GetFixedWindowLimiter(key, _ => new FixedWindowRateLimiterOptions { PermitLimit = permits, Window = TimeSpan.FromMinutes(1), QueueLimit = 0 }));

    public RateLimitLease FromSource(string source) => _bySource.AttemptAcquire(source);

    /// <summary>A client id that is no identity's shape shares one window, so no text a caller sends is kept as a key of its own.</summary>
    public RateLimitLease ForIdentity(string clientId) => _byIdentity.AttemptAcquire(clientId);

    public void Dispose()
    {
        _bySource.Dispose();
        _byIdentity.Dispose();
    }
}

/// <summary>
/// The issuer's signing keys: RSA, kept on a shelf of the state's store (<see cref="Shelf"/>, apart from DataProtection's repository,
/// whose key manager reads every element there as a key of its own), each protected by DataProtection, whose keys are the session keys
/// of the same store. A restored copy of the store, or a second instance over it, reads the same keys and validates the same tokens with
/// no request to this server's own address. A start finds the keys or makes one; a shelf that cannot be read stops it, and a key the
/// session keys cannot open is skipped with a warning (its tokens, short-lived, are refused, and a node asks for another). The issuer
/// signs with the key of the smallest id it read at start. A token signed with a key this instance has not read (another instance's,
/// made at the same first start) sends it back to the shelf, at most every <see cref="RereadEvery"/>.
/// </summary>
public sealed class IssuerKeys
{
    public const string Shelf = "issuer-keys";

    private const string Element = "signingKey";
    private const string Purpose = "dtpipe-coordinator.issuer-signing-key";
    private static readonly TimeSpan RereadEvery = TimeSpan.FromSeconds(30);

    private readonly IXmlRepository _shelf;
    private readonly IDataProtector _protector;
    private readonly ILogger<IssuerKeys> _logger;
    private readonly object _gate = new();
    // Replaced whole, never edited: a reader takes the reference once and needs no lock.
    private volatile IReadOnlyDictionary<string, RsaSecurityKey> _keys;
    private DateTimeOffset _readAt;

    public IssuerKeys([FromKeyedServices(Shelf)] IXmlRepository shelf, IDataProtectionProvider protection, ILogger<IssuerKeys> logger)
    {
        (_shelf, _protector, _logger) = (shelf, protection.CreateProtector(Purpose), logger);
        var keys = Read();
        if (keys.Count == 0)
        {
            Store();
            keys = Read();
            if (keys.Count == 0) throw new InvalidOperationException("The key issuer's signing key was stored and cannot be read back.");
        }
        (_keys, _readAt) = (keys, DateTimeOffset.UtcNow);
        Signing = keys.MinBy(k => k.Key, StringComparer.Ordinal).Value;
    }

    /// <summary>The key the issuer signs with.</summary>
    public RsaSecurityKey Signing { get; }

    /// <summary>A token for the service <paramref name="clientId"/>: its client is its subject (RFC 9068), which is what makes it a service's.</summary>
    public string Issue(string clientId, string issuer, string audience, DateTimeOffset now, TimeSpan lifetime) =>
        Create(new Dictionary<string, object> { ["sub"] = clientId, ["client_id"] = clientId }, issuer, audience, now, lifetime);

    /// <summary>
    /// A token for the person <paramref name="personId"/>, obtained through the studio's client: that client, a subject that is not it,
    /// which is what makes it a person's (<see cref="TrustedIdp.IsStudioPerson"/>), and the id of the key it was exchanged with, by
    /// which every request checks that key is still live. Never the key, never its hash.
    /// </summary>
    public string IssuePerson(string personId, string studioClientId, string keyId, string issuer, string audience, DateTimeOffset now, TimeSpan lifetime) =>
        Create(new Dictionary<string, object> { ["sub"] = personId, ["client_id"] = studioClientId, [TrustedIdp.KeyIdClaim] = keyId }, issuer, audience, now, lifetime);

    private string Create(Dictionary<string, object> claims, string issuer, string audience, DateTimeOffset now, TimeSpan lifetime)
    {
        claims["jti"] = Guid.NewGuid().ToString("N");
        return new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
        {
            Issuer = issuer,
            Audience = audience,
            TokenType = "at+jwt",
            IssuedAt = now.UtcDateTime,
            NotBefore = now.UtcDateTime,
            Expires = (now + lifetime).UtcDateTime,
            Claims = claims,
            SigningCredentials = new SigningCredentials(Signing, SecurityAlgorithms.RsaSha256),
        });
    }

    /// <summary>The keys a token of this issuer may be signed with: the one its <c>kid</c> names, read again from the shelf when unknown.</summary>
    public IEnumerable<SecurityKey> Resolve(string? kid)
    {
        if (kid is null) return [];
        if (_keys.TryGetValue(kid, out var key)) return [key];
        lock (_gate)
        {
            if (DateTimeOffset.UtcNow - _readAt >= RereadEvery)
                (_keys, _readAt) = (Read(), DateTimeOffset.UtcNow);
        }
        return _keys.TryGetValue(kid, out key) ? [key] : [];
    }

    /// <summary>The public half of every key, as a JSON Web Key Set: a modulus and an exponent, never a private parameter.</summary>
    public object Jwks() => new
    {
        keys = _keys.Values.Select(k =>
        {
            var parameters = k.Rsa.ExportParameters(includePrivateParameters: false);
            return new Dictionary<string, string>
            {
                ["kty"] = "RSA",
                ["use"] = "sig",
                ["alg"] = SecurityAlgorithms.RsaSha256,
                ["kid"] = k.KeyId,
                ["n"] = Base64UrlEncoder.Encode(parameters.Modulus),
                ["e"] = Base64UrlEncoder.Encode(parameters.Exponent),
            };
        }),
    };

    private Dictionary<string, RsaSecurityKey> Read()
    {
        var keys = new Dictionary<string, RsaSecurityKey>(StringComparer.Ordinal);
        foreach (var element in _shelf.GetAllElements())
        {
            var kid = element.Attribute("kid")?.Value;
            if (element.Name != Element || string.IsNullOrEmpty(kid))
            {
                _logger.LogWarning("An element of the key issuer's shelf that is no signing key is skipped");
                continue;
            }
            byte[]? pkcs8 = null;
            try
            {
                pkcs8 = _protector.Unprotect(Convert.FromBase64String(element.Value));
                var rsa = RSA.Create();
                rsa.ImportPkcs8PrivateKey(pkcs8, out _);
                keys[kid] = new RsaSecurityKey(rsa) { KeyId = kid };
            }
            catch (Exception ex) when (ex is CryptographicException or FormatException)
            {
                _logger.LogWarning("Signing key {Kid} of the key issuer cannot be opened with this coordinator's session keys: it is skipped", kid);
            }
            finally
            {
                if (pkcs8 is not null) CryptographicOperations.ZeroMemory(pkcs8);
            }
        }
        return keys;
    }

    private void Store()
    {
        using var rsa = RSA.Create(2048);
        var pkcs8 = rsa.ExportPkcs8PrivateKey();
        try
        {
            var kid = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(8));
            _shelf.StoreElement(new XElement(Element,
                new XAttribute("kid", kid),
                new XAttribute("created", DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)),
                Convert.ToBase64String(_protector.Protect(pkcs8))), $"signing-key-{kid}");
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
        }
    }
}
