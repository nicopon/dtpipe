using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.RegularExpressions;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A coordinator that trusts no identity provider: every identity is verified by a key, its own issuer is the only one it names and
/// trusts, the sign-in form is the only way in, and the first administrator and the runner come from <c>Coordinator:Bootstrap</c>, which
/// the deployment gives and the rights keep the hashes of. No log carries a key or a hash, and the last administrator stays.
/// </summary>
public sealed partial class KeyOnlyCoordinatorTests : IDisposable
{
    private const string PublicUrl = "https://coordinator.test";
    private const string Negotiate = "/node-channel/negotiate?negotiateVersion=1";

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly string _adminKey = ClearKey.New("boss").Reveal();
    private readonly string _runnerKey = ClearKey.New("runner").Reveal();
    private readonly string _secrets = Path.Combine(Path.GetTempPath(), "bootstrap-" + Guid.NewGuid().ToString("N"));
    private readonly CoordinatorHost _host;
    private readonly CapturedLog _log = new();
    private readonly WebApplicationFactory<Program> _app;

    public KeyOnlyCoordinatorTests()
    {
        Directory.CreateDirectory(_secrets);
        File.WriteAllText(Path.Combine(_secrets, "runner.key"), _runnerKey + "\n");
        File.WriteAllText(Path.Combine(_secrets, "admin.key"), _adminKey);
        _host = KeyOnly();
        _app = _host.WithWebHostBuilder(b => b.ConfigureLogging(l => l.AddProvider(_log).AddFilter<CapturedLog>(null, LogLevel.Trace)));
    }

    public void Dispose()
    {
        _app.Dispose();
        _host.Dispose();
        Directory.Delete(_secrets, recursive: true);
    }

    /// <summary>
    /// A coordinator with no provider, the administrator's and the runner's keys in files: the test host hands its settings over as
    /// command-line arguments, where a key itself is refused.
    /// </summary>
    private CoordinatorHost KeyOnly(string? state = null) => new(publicUrl: PublicUrl, state: state, settings: new Dictionary<string, string>
    {
        ["Authority"] = "",
        ["Ui:ClientSecret"] = "",
        ["Bootstrap:AdminKey"] = "@" + Path.Combine(_secrets, "admin.key"),
        ["Bootstrap:Identities:0:Id"] = "runner",
        ["Bootstrap:Identities:0:Groups:0"] = "runners",
        ["Bootstrap:Identities:0:Key"] = "@" + Path.Combine(_secrets, "runner.key"),
    });

    private static CoordinatorServerOptions Options(params (string Key, string Value)[] values) => CoordinatorServerOptions.FromConfiguration(
        new ConfigurationBuilder().AddInMemoryCollection(values.ToDictionary(v => "Coordinator:" + v.Key, v => (string?)v.Value)).Build());

    private static HttpClient Client(WebApplicationFactory<Program> app, string? bearer = null)
    {
        var client = app.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });
        if (bearer is not null) client.DefaultRequestHeaders.Authorization = new("Bearer", bearer);
        client.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
        return client;
    }

    /// <summary>A browser: it keeps cookies, follows no redirect, and speaks HTTPS as the public address does.</summary>
    private static HttpClient Browser(WebApplicationFactory<Program> app)
    {
        var client = app.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false, HandleCookies = true, BaseAddress = new Uri("https://localhost") });
        client.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
        return client;
    }

    [GeneratedRegex("name=\"__RequestVerificationToken\" value=\"([^\"]+)\"")]
    private static partial Regex AntiforgeryField();

    private static async Task<HttpResponseMessage> ExchangeAsync(WebApplicationFactory<Program> app, string clientId, string secret)
    {
        using var client = Client(app);
        return await client.PostAsync(KeyIssuer.TokenPath, new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
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

    private static async Task<string[]> RolesAsync(HttpClient client) =>
        (await client.GetFromJsonAsync<CoordinatorMe>(CoordinatorApi.Me, Web, Ct))!.Roles.ToArray();

    /// <summary>Signs <paramref name="key"/> in through the form, in <paramref name="browser"/>.</summary>
    private static async Task<HttpResponseMessage> SignInAsync(HttpClient browser, string key)
    {
        var form = await browser.GetStringAsync(AccountPages.LoginPath, Ct);
        var token = WebUtility.HtmlDecode(AntiforgeryField().Match(form).Groups[1].Value);
        return await browser.PostAsync(AccountPages.LoginPath, new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["key"] = key,
            ["returnUrl"] = "/",
            ["__RequestVerificationToken"] = token,
        }), Ct);
    }

    [Fact]
    public void Without_A_Provider_The_Start_Needs_An_Address_An_Admin_Key_And_A_Way_To_Exchange_It()
    {
        var admin = ("Bootstrap:AdminKey", _adminKey);
        var address = ("PublicUrl", PublicUrl);

        var options = Options(address, admin);
        Assert.False(options.HasProvider);
        Assert.Null(options.UiClientSecret);
        Assert.Equal(new[] { PublicUrl }, options.AuthorizationServers);
        Assert.Equal(new[] { PersonAuth.Key }, options.PersonAuth);
        Assert.Equal("boss", options.Bootstrap.Admin!.ClientId);

        var noAddress = Assert.Throws<InvalidOperationException>(() => Options(admin));
        Assert.Contains("Coordinator:PublicUrl", noAddress.Message);
        var noAdmin = Assert.Throws<InvalidOperationException>(() => Options(address));
        Assert.Contains("Coordinator:Bootstrap:AdminKey is required", noAdmin.Message);
        var pages = Assert.Throws<InvalidOperationException>(() => Options(address, admin, ("Ui:ClientSecret", "s")));
        Assert.Contains("Coordinator:Ui:ClientSecret", pages.Message);
        var noPeople = Assert.Throws<InvalidOperationException>(() => Options(address, admin, ("Auth:PersonKeys", "false")));
        Assert.Contains("nobody could sign in", noPeople.Message);
        var http = Assert.Throws<InvalidOperationException>(() => Options(("PublicUrl", "http://coordinator.test"), admin));
        Assert.Contains("Coordinator:Auth:AllowHttpKeys", http.Message);
        Assert.False(Options(("PublicUrl", "http://coordinator.test"), admin, ("Auth:AllowHttpKeys", "true")).HasProvider);
    }

    [Fact]
    public async Task Without_A_Provider_Only_The_Key_Issuer_Is_Named_And_Trusted()
    {
        var schemes = (await _app.Services.GetRequiredService<IAuthenticationSchemeProvider>().GetAllSchemesAsync()).Select(s => s.Name).ToList();
        Assert.DoesNotContain(TrustedIdp.IdpServiceScheme, schemes);
        Assert.DoesNotContain(TrustedIdp.IdpPersonScheme, schemes);
        Assert.DoesNotContain(TrustedIdp.OidcScheme, schemes);
        Assert.Contains(TrustedIdp.KeyServiceScheme, schemes);
        Assert.Equal(TrustedIdp.CookieScheme, (await _app.Services.GetRequiredService<IAuthenticationSchemeProvider>().GetDefaultChallengeSchemeAsync())!.Name);

        using var anonymous = Client(_app);
        var metadata = await anonymous.GetFromJsonAsync<JsonElement>(CoordinatorApi.Metadata, Ct);
        Assert.Equal(new[] { PublicUrl }, metadata.GetProperty("authorization_servers").EnumerateArray().Select(s => s.GetString()));
        Assert.Equal(new[] { PersonAuth.Key }, metadata.GetProperty("dtpipe_auth").EnumerateArray().Select(s => s.GetString()));

        // A token of the test provider, however well signed, opens nothing: there is no provider to trust.
        using (var provider = Client(_app, CoordinatorHost.Person(CoordinatorHost.Admin)))
            Assert.Equal(HttpStatusCode.Unauthorized, (await provider.GetAsync(CoordinatorApi.Me, Ct)).StatusCode);
        using (var service = Client(_app, CoordinatorHost.Service("runner")))
            Assert.Equal(HttpStatusCode.Unauthorized, (await service.PostAsync(Negotiate, null, Ct)).StatusCode);

        // The runner's key, read from its file, opens the node channel; the administrator reads rights that name no provider.
        using (var runner = Client(_app, await TokenAsync(_app, "runner", _runnerKey)))
            Assert.Equal(HttpStatusCode.OK, (await runner.PostAsync(Negotiate, null, Ct)).StatusCode);
        using var admin = Client(_app, await TokenAsync(_app, "boss", _adminKey));
        var rights = await admin.GetFromJsonAsync<JsonElement>("/api/rights", Ct);
        Assert.Equal(JsonValueKind.Null, rights.GetProperty("idp").ValueKind);
    }

    [Fact]
    public async Task Without_A_Provider_The_Form_Is_The_Only_Way_In_And_Out()
    {
        using var browser = Browser(_app);
        using (var page = await browser.GetAsync("/", Ct))
        {
            Assert.Equal(HttpStatusCode.Redirect, page.StatusCode);
            Assert.StartsWith(AccountPages.LoginPath + "?returnUrl=", page.Headers.Location!.OriginalString);
        }
        Assert.DoesNotContain(AccountPages.ProviderLoginPath, await browser.GetStringAsync(AccountPages.LoginPath, Ct));
        Assert.Equal(HttpStatusCode.NotFound, (await browser.GetAsync(AccountPages.ProviderLoginPath, Ct)).StatusCode);

        using (var signedIn = await SignInAsync(browser, _adminKey)) Assert.Equal(HttpStatusCode.Redirect, signedIn.StatusCode);
        Assert.Equal(CoordinatorRoles.All, await RolesAsync(browser));

        using (var signedOut = await browser.GetAsync("/account/logout", Ct))
        {
            Assert.Equal(HttpStatusCode.Redirect, signedOut.StatusCode);
            Assert.Equal("/", signedOut.Headers.Location!.OriginalString);
        }
        Assert.Equal(HttpStatusCode.Unauthorized, (await browser.GetAsync(CoordinatorApi.Me, Ct)).StatusCode);
        var denied = await browser.GetStringAsync("/account/denied", Ct);
        Assert.DoesNotContain("identity provider", denied);
    }

    [Fact]
    public async Task Without_A_Provider_An_Identity_Of_The_Provider_Is_Refused_When_Added_And_At_Start()
    {
        using var admin = Client(_app, await TokenAsync(_app, "boss", _adminKey));
        foreach (var identity in new object[]
        {
            new { clientId = "edge", groups = new[] { "crm" } },
            new { clientId = "edge", groups = new[] { "crm" }, auth = AuthModes.Idp },
            new { clientId = "ann", kind = IdentityKinds.Person, role = CoordinatorRoles.Reader, account = "ann@x.test", auth = AuthModes.Idp },
        })
        {
            using var refused = await admin.PostAsJsonAsync("/api/rights/identities", identity, Web, Ct);
            Assert.Equal(HttpStatusCode.Conflict, refused.StatusCode);
            Assert.Contains("trusts no identity provider", await refused.Content.ReadAsStringAsync(Ct));
        }

        var withProvider = Options(("Authority", "https://idp.example"), ("Ui:ClientSecret", "s"));
        var rights = new RightsStore(MemoryRightsStore.At(Guid.NewGuid().ToString("N")), withProvider);
        rights.AddIdentity(new Identity("edge", "edge", "", ["crm"]));
        var start = Assert.Throws<InvalidOperationException>(() => KeyIssuer.CheckIdentities(rights, Options(("PublicUrl", PublicUrl), ("Bootstrap:AdminKey", _adminKey))));
        Assert.Contains("verified by the identity provider (edge)", start.Message);
    }

    [Fact]
    public async Task The_Bootstrap_Adds_The_Administrator_And_The_Runner_In_One_Audited_Change_Once()
    {
        var rights = _app.Services.GetRequiredService<RightsStore>().Snapshot();

        var boss = Assert.Single(rights.Identities, i => i.ClientId == "boss");
        Assert.Equal((IdentityKinds.Person, CoordinatorRoles.Admin, AuthModes.Key), (boss.Kind, boss.Role, boss.Auth.Mode));
        Assert.Equal(RightsBootstrap.KeyLabel, Assert.Single(boss.Auth.Keys).Label);
        Assert.True(ClearKey.Matches(_adminKey, boss.Auth.Keys[0].Hash));
        var runner = Assert.Single(rights.Identities, i => i.ClientId == "runner");
        Assert.Equal((IdentityKinds.Node, AuthModes.Key), (runner.Kind, runner.Auth.Mode));
        Assert.Equal(new[] { "runners" }, runner.Groups);
        Assert.True(ClearKey.Matches(_runnerKey, Assert.Single(runner.Auth.Keys).Hash));
        var bootstrapped = Assert.Single(rights.Audit!, a => a.By == RightsBootstrap.Author);
        Assert.Contains("Identity boss added", bootstrapped.Change);
        Assert.Contains("Identity runner added", bootstrapped.Change);
        Assert.Equal(2, rights.Audit!.Count);

        // A second start over the same store adds nothing and writes nothing.
        using var again = KeyOnly(_host.StateDirectory);
        Assert.Equal(rights.Audit.Count, again.Services.GetRequiredService<RightsStore>().Snapshot().Audit!.Count);
    }

    [Fact]
    public void The_Bootstrap_Replaces_No_Identity()
    {
        var withProvider = Options(("Authority", "https://idp.example"), ("Ui:ClientSecret", "s"), ("PublicUrl", PublicUrl));
        var bootstrap = Options(("Authority", "https://idp.example"), ("Ui:ClientSecret", "s"), ("PublicUrl", PublicUrl), ("Bootstrap:AdminKey", _adminKey)).Bootstrap;

        // An identity of the administrator's id that is no administrator stops the start, naming it.
        var taken = new RightsStore(MemoryRightsStore.At(Guid.NewGuid().ToString("N")), withProvider);
        taken.AddIdentity(new Identity("boss", "boss", "", ["crm"]));
        var stopped = Assert.Throws<InvalidOperationException>(() => taken.Bootstrap(bootstrap));
        Assert.Contains("'boss', which the rights hold as a node", stopped.Message);
        Assert.Single(taken.Snapshot().Audit!, a => a.Change.StartsWith("Identity boss added"));

        // An administrator of that id is left as it is, and so is the rights' every other identity.
        var held = new RightsStore(MemoryRightsStore.At(Guid.NewGuid().ToString("N")), withProvider);
        held.AddIdentity(new Identity("boss", "boss", "", []) { Kind = IdentityKinds.Person, Role = CoordinatorRoles.Admin, Account = "boss@x.test" });
        var before = held.Snapshot().Audit!.Count;
        var (added, left) = held.Bootstrap(bootstrap);
        Assert.Empty(added);
        Assert.Equal("boss", left);
        Assert.Equal(before, held.Snapshot().Audit!.Count);
        Assert.Equal(AuthModes.Idp, held.Find("boss")!.Auth.Mode);

        // While an administrator with a live key exists, the key names nobody new.
        var served = new RightsStore(MemoryRightsStore.At(Guid.NewGuid().ToString("N")), withProvider);
        served.AddIdentity(new Identity("ann", "ann", "", []) { Kind = IdentityKinds.Person, Role = CoordinatorRoles.Admin, Auth = new IdentityAuth(AuthModes.Key, []) });
        served.AddKey("ann", null, null);
        var (none, kept) = served.Bootstrap(bootstrap);
        Assert.Empty(none);
        Assert.Null(kept);
        Assert.Null(served.Find("boss"));
    }

    [Fact]
    public void A_Bootstrap_Key_That_Is_Malformed_Or_Misplaced_Stops_The_Start_Without_Being_Written()
    {
        (string, string)[] keyOnly = [("PublicUrl", PublicUrl)];
        var missing = Path.Combine(_secrets, _runnerKey);
        var empty = Path.Combine(_secrets, "empty.key");
        File.WriteAllText(empty, "  \n");
        var cases = new (string Text, (string, string)[] Settings, string Says)[]
        {
            (_adminKey[..^1], [.. keyOnly, ("Bootstrap:AdminKey", _adminKey[..^1])], "is not a key of this coordinator"),
            (_adminKey[..^2] + "!!", [.. keyOnly, ("Bootstrap:AdminKey", _adminKey[..^2] + "!!")], "is not a key of this coordinator"),
            (_runnerKey, [.. keyOnly, ("Bootstrap:AdminKey", "@" + missing)], "names a file (@…) that cannot be read"),
            ("empty.key", [.. keyOnly, ("Bootstrap:AdminKey", "@" + empty)], "names a file (@…) that is empty"),
            (_runnerKey, [.. keyOnly, ("Bootstrap:AdminKey", _adminKey), ("Bootstrap:Identities:0:Id", "edge"), ("Bootstrap:Identities:0:Key", _runnerKey)],
                "opens another identity than 'edge'"),
            (_adminKey, [.. keyOnly, ("Bootstrap:AdminKey", _adminKey), ("Bootstrap:Identities:0:Id", "edge")], "Identities:0:Key is required"),
            (_adminKey, [.. keyOnly, ("Bootstrap:AdminKey", _adminKey), ("Bootstrap:Identities:0:Id", "boss"), ("Bootstrap:Identities:0:Key", _adminKey)],
                "names 'boss' more than once"),
            // A key needs the issuer: with the trial provider there is none, and without people's keys there is no administrator's.
            (_adminKey, [("PublicUrl", PublicUrl), ("InsecureTrialIdp:Password", "0123456789abcdef"), ("Bootstrap:AdminKey", _adminKey)], "the trial identity provider"),
            (_adminKey, [.. keyOnly, ("Authority", "https://idp.example"), ("Ui:ClientSecret", "s"), ("Auth:PersonKeys", "false"), ("Bootstrap:AdminKey", _adminKey)],
                "names people (boss)"),
        };
        foreach (var (text, settings, says) in cases)
        {
            var refused = Assert.Throws<InvalidOperationException>(() => Options(settings));
            Assert.Contains(says, refused.Message);
            Assert.DoesNotContain(text, refused.Message);
        }

        // A key itself on the command line could be listed by any process; a file's path there is no secret.
        var commandLine = Assert.Throws<InvalidOperationException>(() => CoordinatorServerOptions.FromConfiguration(new ConfigurationBuilder()
            .AddCommandLine([$"--Coordinator:PublicUrl={PublicUrl}", $"--Coordinator:Bootstrap:AdminKey={_adminKey}"]).Build()));
        Assert.Contains("given on the command line", commandLine.Message);
        File.WriteAllText(empty, _adminKey);
        Assert.Equal("boss", CoordinatorServerOptions.FromConfiguration(new ConfigurationBuilder()
            .AddCommandLine([$"--Coordinator:PublicUrl={PublicUrl}", $"--Coordinator:Bootstrap:AdminKey=@{empty}"]).Build()).Bootstrap.Admin!.ClientId);
    }

    [Fact]
    public async Task No_Log_Of_A_Bootstrapped_Start_Or_Of_Its_Keys_At_Work_Holds_A_Key_Or_A_Hash()
    {
        var wrong = ClearKey.New("runner").Reveal();
        await TokenAsync(_app, "runner", _runnerKey);
        using (var refused = await ExchangeAsync(_app, "runner", wrong)) Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        using (var pasted = await ExchangeAsync(_app, wrong, wrong)) Assert.Equal(HttpStatusCode.BadRequest, pasted.StatusCode);
        using var browser = Browser(_app);
        using (await SignInAsync(browser, _adminKey)) { }
        using (await SignInAsync(browser, wrong)) { }
        using var admin = Client(_app, await TokenAsync(_app, "boss", _adminKey));
        using var made = await admin.PostAsJsonAsync("/api/rights/identities/runner/keys", new { }, Web, Ct);
        var rotated = (await made.Content.ReadFromJsonAsync<JsonElement>(Web, Ct)).GetProperty("key").GetString()!;
        await TokenAsync(_app, "runner", rotated);

        var log = string.Join("\n", _log.Entries);
        Assert.Contains("The rights gained boss, runner from Coordinator:Bootstrap", log);
        Assert.DoesNotContain(ClearKey.Prefix, log);
        foreach (var key in new[] { _adminKey, _runnerKey, rotated, wrong })
        {
            var secret = key[^ClearKey.SecretLength..];
            for (var at = 0; at + 8 <= secret.Length; at++) Assert.DoesNotContain(secret.Substring(at, 8), log);
        }
        foreach (var hash in _app.Services.GetRequiredService<RightsStore>().Snapshot().Identities.SelectMany(i => i.Auth.Keys).Select(k => k.Hash))
            Assert.DoesNotContain(hash[..16], log, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task The_Last_Administrator_Stays_Whatever_The_Route()
    {
        using var boss = Client(_app, await TokenAsync(_app, "boss", _adminKey));
        var refusals = new Func<Task<HttpResponseMessage>>[]
        {
            () => boss.PostAsync("/api/rights/identities/boss/keys/1/revoke", null, Ct),
            () => boss.PutAsJsonAsync("/api/rights/identities/boss", new { enabled = false }, Web, Ct),
            () => boss.PutAsJsonAsync("/api/rights/identities/boss", new { role = CoordinatorRoles.Designer }, Web, Ct),
            () => boss.DeleteAsync("/api/rights/identities/boss", Ct),
        };
        foreach (var refusal in refusals)
        {
            using var refused = await refusal();
            Assert.Equal(HttpStatusCode.Conflict, refused.StatusCode);
            Assert.Contains("'boss' is the last administrator", await refused.Content.ReadAsStringAsync(Ct));
        }
        Assert.Equal(CoordinatorRoles.All, await RolesAsync(boss));

        // A second administrator lets the first go, and is then the last one.
        (await boss.PostAsJsonAsync("/api/rights/identities", new { clientId = "ann", kind = IdentityKinds.Person, role = CoordinatorRoles.Admin, auth = AuthModes.Key }, Web, Ct))
            .EnsureSuccessStatusCode();
        using var made = await boss.PostAsJsonAsync("/api/rights/identities/ann/keys", new { }, Web, Ct);
        using var ann = Client(_app, await TokenAsync(_app, "ann", (await made.Content.ReadFromJsonAsync<JsonElement>(Web, Ct)).GetProperty("key").GetString()!));
        (await ann.PutAsJsonAsync("/api/rights/identities/boss", new { role = CoordinatorRoles.Reader }, Web, Ct)).EnsureSuccessStatusCode();
        using var last = await ann.PostAsync("/api/rights/identities/ann/keys/1/revoke", null, Ct);
        Assert.Equal(HttpStatusCode.Conflict, last.StatusCode);
        Assert.Contains("'ann' is the last administrator", await last.Content.ReadAsStringAsync(Ct));
    }
}
