using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Nodes;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.JsonWebTokens;
using TransportR.Protocol;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The coordinator's key issuer: a key is exchanged for a short token of the coordinator, which opens what the identity's kind grants,
/// beside the external provider's tokens and never in their place. Every refusal of a client reads the same, and no log carries a key.
/// </summary>
public sealed class KeyIssuerTests : IDisposable
{
    private const string PublicUrl = "https://coordinator.test";
    private const string Negotiate = "/node-channel/negotiate?negotiateVersion=1";
    // A route of TransportR's control plane that has no effect without a body: a caller it admits is answered 415, one it does not, 401.
    private const string ControlPlane = WireRoutes.ClientBase + "/6f1c0d52-8d0b-4a39-9a5e-3f6d1d6a7b10/" + WireRoutes.TransfersSegment;

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly CoordinatorHost _host = new(publicUrl: PublicUrl);
    private readonly CapturedLog _log = new();
    private readonly WebApplicationFactory<Program> _app;

    public KeyIssuerTests() => _app = Logged(_host, _log);

    public void Dispose()
    {
        _app.Dispose();
        _host.Dispose();
    }

    private static WebApplicationFactory<Program> Logged(CoordinatorHost host, CapturedLog log) =>
        host.WithWebHostBuilder(b => b.ConfigureLogging(l => l.AddProvider(log).AddFilter<CapturedLog>(null, LogLevel.Trace)));

    private static CoordinatorServerOptions Options(params (string Key, string Value)[] values) => CoordinatorServerOptions.FromConfiguration(
        new ConfigurationBuilder().AddInMemoryCollection(values.ToDictionary(v => "Coordinator:" + v.Key, v => (string?)v.Value)).Build());

    private static HttpClient Client(WebApplicationFactory<Program> app, string? bearer = null)
    {
        var client = app.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });
        if (bearer is not null) client.DefaultRequestHeaders.Authorization = new("Bearer", bearer);
        client.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
        return client;
    }

    private static HttpClient Admin(WebApplicationFactory<Program> app) => Client(app, CoordinatorHost.Person(CoordinatorHost.Admin));

    /// <summary>Adds the identity <paramref name="id"/> of <paramref name="kind"/> verified by <paramref name="auth"/>.</summary>
    private static async Task<HttpResponseMessage> AddAsync(WebApplicationFactory<Program> app, string id, string kind = IdentityKinds.Node, string auth = AuthModes.Key)
    {
        using var admin = Admin(app);
        return await admin.PostAsJsonAsync("/api/rights/identities", new
        {
            clientId = id,
            name = id,
            kind,
            groups = new[] { "crm" },
            pipelines = new[] { "sales" },
            auth,
        }, Web, Ct);
    }

    /// <summary>Adds the identity <paramref name="id"/>, verified by a key, and answers the key made for it.</summary>
    private static async Task<string> KeyedAsync(WebApplicationFactory<Program> app, string id, string kind = IdentityKinds.Node, DateTimeOffset? expires = null)
    {
        using (var added = await AddAsync(app, id, kind)) added.EnsureSuccessStatusCode();
        using var admin = Admin(app);
        using var made = await admin.PostAsJsonAsync($"/api/rights/identities/{id}/keys", new { expires }, Web, Ct);
        made.EnsureSuccessStatusCode();
        return (await made.Content.ReadFromJsonAsync<JsonElement>(Web, Ct)).GetProperty("key").GetString()!;
    }

    private static async Task<HttpResponseMessage> ExchangeAsync(WebApplicationFactory<Program> app, string clientId, string secret, string grant = "client_credentials")
    {
        using var client = Client(app);
        return await client.PostAsync(KeyIssuer.TokenPath, new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = grant,
            ["client_id"] = clientId,
            ["client_secret"] = secret,
        }), Ct);
    }

    private static async Task<string> TokenAsync(WebApplicationFactory<Program> app, string clientId, string key)
    {
        using var answer = await ExchangeAsync(app, clientId, key);
        Assert.Equal(HttpStatusCode.OK, answer.StatusCode);
        return (await answer.Content.ReadFromJsonAsync<JsonElement>(Web, Ct)).GetProperty("access_token").GetString()!;
    }

    private static async Task<HttpStatusCode> StatusAsync(WebApplicationFactory<Program> app, string token, string path, bool post = false)
    {
        using var client = Client(app, token);
        using var response = post ? await client.PostAsync(path, null, Ct) : await client.GetAsync(path, Ct);
        return response.StatusCode;
    }

    [Fact]
    public async Task A_Key_Is_Exchanged_For_A_Short_Token_Of_The_Coordinator_Itself()
    {
        var key = await KeyedAsync(_app, "edge");

        using var answer = await ExchangeAsync(_app, "edge", key);

        Assert.Equal(HttpStatusCode.OK, answer.StatusCode);
        Assert.Equal("no-store", answer.Headers.CacheControl?.ToString());
        var body = await answer.Content.ReadFromJsonAsync<JsonElement>(Web, Ct);
        Assert.Equal("Bearer", body.GetProperty("token_type").GetString());
        Assert.Equal(600, body.GetProperty("expires_in").GetInt32());
        var token = new JsonWebToken(body.GetProperty("access_token").GetString());
        Assert.Equal(PublicUrl, token.Issuer);
        Assert.Equal(new[] { CoordinatorHost.Audience }, token.Audiences);
        Assert.Equal("edge", token.Subject);
        Assert.Equal("edge", token.GetClaim("client_id").Value);
        Assert.Equal(TimeSpan.FromMinutes(10), token.ValidTo - token.IssuedAt);

        using var client = Client(_app);
        var discovery = await client.GetFromJsonAsync<JsonElement>(KeyIssuer.DiscoveryPath, Ct);
        Assert.Equal(PublicUrl, discovery.GetProperty("issuer").GetString());
        Assert.Equal(PublicUrl + KeyIssuer.TokenPath, discovery.GetProperty("token_endpoint").GetString());
        Assert.Equal(PublicUrl + KeyIssuer.JwksPath, discovery.GetProperty("jwks_uri").GetString());
        var jwk = Assert.Single((await client.GetFromJsonAsync<JsonElement>(KeyIssuer.JwksPath, Ct)).GetProperty("keys").EnumerateArray());
        Assert.Equal(token.Kid, jwk.GetProperty("kid").GetString());
        Assert.False(jwk.TryGetProperty("d", out _));
        // The external provider stays first: the studio and a node with no Oidc:Authority read the first.
        var metadata = await client.GetFromJsonAsync<JsonElement>(CoordinatorApi.Metadata, Ct);
        Assert.Equal(new[] { CoordinatorHost.Issuer.TrimEnd('/'), PublicUrl },
            metadata.GetProperty("authorization_servers").EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public async Task A_Token_It_Made_Opens_What_The_Identitys_Kind_Grants_And_Nothing_Else()
    {
        var node = await TokenAsync(_app, "edge", await KeyedAsync(_app, "edge"));
        var monitor = await TokenAsync(_app, "watch", await KeyedAsync(_app, "watch", IdentityKinds.Monitor));
        var launcher = await TokenAsync(_app, "cron", await KeyedAsync(_app, "cron", IdentityKinds.Launcher));

        Assert.Equal(HttpStatusCode.OK, await StatusAsync(_app, node, Negotiate, post: true));
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, await StatusAsync(_app, node, ControlPlane, post: true));
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(_app, monitor, "/health/pipelines"));
        // Admitted: what answers is the run's absence, not the authorization.
        Assert.Equal(HttpStatusCode.NotFound, await StatusAsync(_app, launcher, "/service/runs/run-1"));

        Assert.Equal(HttpStatusCode.Forbidden, await StatusAsync(_app, node, "/health/pipelines"));
        Assert.Equal(HttpStatusCode.Forbidden, await StatusAsync(_app, launcher, "/health/pipelines"));
        Assert.Equal(HttpStatusCode.Forbidden, await StatusAsync(_app, monitor, "/service/runs/run-1"));
        foreach (var token in new[] { node, monitor, launcher })
            Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, token, "/api/me"));
        Assert.Empty(_app.Services.GetRequiredService<EnrollmentQueue>().List());
    }

    [Fact]
    public async Task Every_Refusal_Of_A_Client_Reads_The_Same_And_No_Log_Carries_A_Key()
    {
        var edge = await KeyedAsync(_app, "edge");
        await KeyedAsync(_app, "other");
        var revoked = await KeyedAsync(_app, "gone");
        var expiring = await KeyedAsync(_app, "brief", expires: DateTimeOffset.UtcNow.AddSeconds(1));
        var disabled = await KeyedAsync(_app, "off");
        using (var added = await AddAsync(_app, "plain", auth: AuthModes.Idp)) added.EnsureSuccessStatusCode();
        using (var admin = Admin(_app))
        {
            (await admin.PostAsync("/api/rights/identities/gone/keys/1/revoke", null, Ct)).EnsureSuccessStatusCode();
            (await admin.PutAsJsonAsync("/api/rights/identities/off", new { clientId = "off", enabled = false }, Web, Ct)).EnsureSuccessStatusCode();
        }
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);

        var presented = new (string ClientId, string Secret)[]
        {
            ("ghost", ClearKey.New("ghost").Reveal()), // no such identity
            ("edge", ClearKey.New("edge").Reveal()), // the right shape, not its key
            ("gone", revoked),
            ("brief", expiring),
            ("off", disabled),
            ("plain", ClearKey.New("plain").Reveal()), // verified by the IdP
            ("other", edge), // a key, but not the client's
            ("edge", "not a key"),
            (edge, edge), // the key where the client id goes
        };
        var answers = new List<string>();
        foreach (var (clientId, secret) in presented)
        {
            using var refused = await ExchangeAsync(_app, clientId, secret);
            Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
            Assert.Equal("no-store", refused.Headers.CacheControl?.ToString());
            answers.Add(await refused.Content.ReadAsStringAsync(Ct));
        }

        Assert.Single(answers.Distinct());
        Assert.Equal("invalid_client", JsonDocument.Parse(answers[0]).RootElement.GetProperty("error").GetString());
        Assert.Contains(_log.Entries, e => e.Contains("A key exchange for edge"));
        Assert.Contains(_log.Entries, e => e.Contains("A key exchange for an invalid client id"));
        foreach (var secret in presented.Select(p => p.Secret).Where(s => s.StartsWith(ClearKey.Prefix, StringComparison.Ordinal)))
            Assert.DoesNotContain(_log.Entries, e => e.Contains(secret[^ClearKey.SecretLength..][..8]) || e.Contains(secret[^8..]));
        Assert.DoesNotContain(_log.Entries, e => e.Contains(ClearKey.Prefix));
    }

    [Fact]
    public async Task A_Request_That_Is_No_Client_Credentials_Grant_Is_Told_So()
    {
        var key = await KeyedAsync(_app, "edge");

        using var grant = await ExchangeAsync(_app, "edge", key, grant: "password");
        using var client = Client(_app);
        using var json = await client.PostAsJsonAsync(KeyIssuer.TokenPath, new { grant_type = "client_credentials", client_id = "edge", client_secret = key }, Ct);

        Assert.Equal("unsupported_grant_type", (await grant.Content.ReadFromJsonAsync<JsonElement>(Web, Ct)).GetProperty("error").GetString());
        Assert.Equal(HttpStatusCode.BadRequest, json.StatusCode);
        Assert.Equal("invalid_request", (await json.Content.ReadFromJsonAsync<JsonElement>(Web, Ct)).GetProperty("error").GetString());
    }

    [Fact]
    public async Task A_Token_Opens_Only_The_Identities_Its_Issuer_Verifies_Beside_The_External_Provider()
    {
        var edge = await TokenAsync(_app, "edge", await KeyedAsync(_app, "edge"));
        using (var added = await AddAsync(_app, "plain", auth: AuthModes.Idp)) added.EnsureSuccessStatusCode();
        var issuer = _app.Services.GetRequiredService<IssuerKeys>();
        var now = DateTimeOffset.UtcNow;

        // Each issuer opens the identities it verifies, side by side, and a person still reaches /api.
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(_app, edge, Negotiate, post: true));
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(_app, CoordinatorHost.Service("plain"), Negotiate, post: true));
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(_app, CoordinatorHost.Person(CoordinatorHost.Reader), "/api/me"));
        // The external provider never opens an identity verified by a key, and the key issuer never one it does not verify.
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, CoordinatorHost.Service("edge"), Negotiate, post: true));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, issuer.Issue("plain", PublicUrl, CoordinatorHost.Audience, now, TimeSpan.FromMinutes(10)), Negotiate, post: true));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, issuer.Issue("ghost", PublicUrl, CoordinatorHost.Audience, now, TimeSpan.FromMinutes(10)), Negotiate, post: true));
        // A token that names the coordinator as its issuer and is signed by another is validated against the coordinator's keys, and fails.
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, CoordinatorHost.Token("edge", "edge", issuer: PublicUrl), Negotiate, post: true));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, issuer.Issue("edge", PublicUrl, "another-audience", now, TimeSpan.FromMinutes(10)), Negotiate, post: true));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, issuer.Issue("edge", PublicUrl, CoordinatorHost.Audience, now.AddMinutes(-20), TimeSpan.FromMinutes(10)), Negotiate, post: true));
        Assert.Empty(_app.Services.GetRequiredService<EnrollmentQueue>().List());
    }

    [Fact]
    public async Task The_Signing_Key_Outlives_A_Restart_On_The_Same_Store()
    {
        var state = Path.Combine(Path.GetTempPath(), "coordinator-host-" + Guid.NewGuid().ToString("N"));
        string token;
        using (var first = new CoordinatorHost(publicUrl: PublicUrl, state: state))
            token = await TokenAsync(first, "watch", await KeyedAsync(first, "watch", IdentityKinds.Monitor));

        using var second = new CoordinatorHost(publicUrl: PublicUrl, state: state);

        Assert.Equal(HttpStatusCode.OK, await StatusAsync(second, token, "/health/pipelines"));
        Assert.Equal(new JsonWebToken(token).Kid, second.Services.GetRequiredService<IssuerKeys>().Signing.KeyId);
    }

    [Fact]
    public async Task Over_Http_A_Key_Is_Exchanged_Only_Where_The_Setting_Allows_It_And_Then_Every_Page_Says_So()
    {
        using var plain = new CoordinatorHost(publicUrl: "http://coordinator.test");
        var key = await KeyedAsync(plain, "edge");

        using var refused = await ExchangeAsync(plain, "edge", key);

        Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        var reason = await refused.Content.ReadAsStringAsync(Ct);
        Assert.Contains("Coordinator:Auth:AllowHttpKeys", reason);
        Assert.DoesNotContain(ClearKey.Prefix, reason);

        var log = new CapturedLog();
        using var allowedHost = new CoordinatorHost(publicUrl: "http://coordinator.test", settings: new Dictionary<string, string> { ["Auth:AllowHttpKeys"] = "true" });
        using var allowed = Logged(allowedHost, log);
        await TokenAsync(allowed, "edge", await KeyedAsync(allowed, "edge"));

        using var reader = Client(allowed, CoordinatorHost.Person(CoordinatorHost.Reader));
        Assert.True((await reader.GetFromJsonAsync<JsonElement>("/api/info", Ct)).GetProperty("httpKeys").GetBoolean());
        Assert.Contains(log.Entries, e => e.Contains("Coordinator:Auth:AllowHttpKeys is set"));
        using var https = Client(_app, CoordinatorHost.Person(CoordinatorHost.Reader));
        Assert.False((await https.GetFromJsonAsync<JsonElement>("/api/info", Ct)).GetProperty("httpKeys").GetBoolean());
    }

    [Fact]
    public async Task A_Client_Id_Presented_Too_Often_Is_Told_To_Wait_And_Another_Is_Not()
    {
        for (var i = 0; i < KeyExchangeLimits.PerIdentity; i++)
        {
            using var refused = await ExchangeAsync(_app, "ghost", ClearKey.New("ghost").Reveal());
            Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        }

        using var limited = await ExchangeAsync(_app, "ghost", ClearKey.New("ghost").Reveal());
        using var another = await ExchangeAsync(_app, "phantom", ClearKey.New("phantom").Reveal());

        Assert.Equal(HttpStatusCode.TooManyRequests, limited.StatusCode);
        Assert.Equal("60", limited.Headers.RetryAfter?.ToString());
        Assert.Equal(HttpStatusCode.BadRequest, another.StatusCode);
    }

    [Fact]
    public async Task Without_An_Issuer_No_Identity_Is_Verified_By_A_Key()
    {
        using var host = new CoordinatorHost();

        using var refused = await AddAsync(host, "edge");

        Assert.Equal(HttpStatusCode.Conflict, refused.StatusCode);
        Assert.Contains("Coordinator:PublicUrl", await refused.Content.ReadAsStringAsync(Ct));
        using var client = Client(host);
        using var token = await client.PostAsync(KeyIssuer.TokenPath, new FormUrlEncodedContent([]), Ct);
        Assert.Equal(HttpStatusCode.NotFound, token.StatusCode);
    }

    [Fact]
    public void Public_Url_Turns_The_Issuer_On()
    {
        (string, string)[] external = [("Authority", "https://idp.example"), ("Ui:ClientSecret", "s")];

        Assert.Equal("https://c.example", Options([.. external, ("PublicUrl", "https://c.example/")]).KeyIssuer);
        Assert.Null(Options(external).KeyIssuer);
        Assert.Equal(TimeSpan.FromMinutes(15), Options([.. external, ("Auth:ServiceTokenLifetime", "00:15:00")]).ServiceTokenLifetime);
    }

    [Fact]
    public void An_External_Provider_At_The_Public_Url_Stops_The_Start()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => Options(
            ("Authority", "https://c.example/"), ("Ui:ClientSecret", "s"), ("PublicUrl", "https://c.example")));
        Assert.Contains("issuer of its own", ex.Message);
    }

    [Fact]
    public void A_Token_Lifetime_Shorter_Than_Five_Minutes_Stops_The_Start()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => Options(
            ("Authority", "https://idp.example"), ("Ui:ClientSecret", "s"), ("Auth:ServiceTokenLifetime", "00:04:00")));
        Assert.Contains("Coordinator:Auth:ServiceTokenLifetime", ex.Message);
    }

    [Fact]
    public void Identities_Verified_By_A_Key_Stop_A_Start_That_Cannot_Issue_Their_Tokens()
    {
        var issuing = Options(("Authority", "https://idp.example"), ("Ui:ClientSecret", "s"), ("PublicUrl", "https://c.example"));
        var rights = new RightsStore(MemoryRightsStore.At(Guid.NewGuid().ToString("N")), issuing);
        rights.AddIdentity(new Identity("edge", "edge", "", ["crm"]) { Auth = new IdentityAuth(AuthModes.Key, []) });
        KeyIssuer.CheckIdentities(rights, issuing);

        var noAddress = Assert.Throws<InvalidOperationException>(() => KeyIssuer.CheckIdentities(rights,
            Options(("Authority", "https://idp.example"), ("Ui:ClientSecret", "s"))));

        Assert.Contains("edge", noAddress.Message);
        Assert.Contains("Coordinator:PublicUrl", noAddress.Message);
    }
}
