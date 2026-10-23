using System.Net;
using System.Net.Http.Json;
using System.Security.Claims;
using System.Text.Json;
using System.Text.RegularExpressions;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.JsonWebTokens;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A person verified by a key: an identity of the rights with a role, signed in by the form into the session cookie, or by the
/// studio's token the key issuer makes for it. Both are judged against the rights on every request, reach <c>/api</c> with the role
/// the rights give, and never the data plane, <c>/health</c> or <c>/service</c>. <c>Coordinator:Auth:PersonKeys=false</c> turns all of
/// it off by name.
/// </summary>
public sealed partial class PersonKeyTests : IDisposable
{
    private const string PublicUrl = "https://coordinator.test";
    private const string Negotiate = "/node-channel/negotiate?negotiateVersion=1";

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly CoordinatorHost _host = new(publicUrl: PublicUrl);
    private readonly CapturedLog _log = new();
    private readonly WebApplicationFactory<Program> _app;

    public PersonKeyTests() => _app = _host.WithWebHostBuilder(b => b.ConfigureLogging(l => l.AddProvider(_log).AddFilter<CapturedLog>(null, LogLevel.Trace)));

    public void Dispose()
    {
        _app.Dispose();
        _host.Dispose();
    }

    [GeneratedRegex("name=\"__RequestVerificationToken\" value=\"([^\"]+)\"")]
    private static partial Regex AntiforgeryField();

    [GeneratedRegex("<p role=\"alert\"><strong>([^<]*)</strong></p>")]
    private static partial Regex Alert();

    private static CoordinatorServerOptions Options(params (string Key, string Value)[] values) => CoordinatorServerOptions.FromConfiguration(
        new ConfigurationBuilder().AddInMemoryCollection(values.ToDictionary(v => "Coordinator:" + v.Key, v => (string?)v.Value)).Build());

    private static HttpClient Client(WebApplicationFactory<Program> app, string? bearer = null)
    {
        var client = app.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });
        if (bearer is not null) client.DefaultRequestHeaders.Authorization = new("Bearer", bearer);
        client.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
        return client;
    }

    /// <summary>A browser: it keeps cookies, follows no redirect, and speaks HTTPS as the public address does, so a secure cookie comes back.</summary>
    private static HttpClient Browser(WebApplicationFactory<Program> app)
    {
        var client = app.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false, HandleCookies = true, BaseAddress = new Uri("https://localhost") });
        client.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
        return client;
    }

    private static HttpClient Admin(WebApplicationFactory<Program> app) => Client(app, CoordinatorHost.Person(CoordinatorHost.Admin));

    private static async Task<HttpResponseMessage> AddAsync(WebApplicationFactory<Program> app, string id, string? role, string kind = IdentityKinds.Person, string auth = AuthModes.Key)
    {
        using var admin = Admin(app);
        return await admin.PostAsJsonAsync("/api/rights/identities", new { clientId = id, name = id, kind, role, groups = new[] { "crm" }, auth }, Web, Ct);
    }

    private static async Task<string> KeyAsync(WebApplicationFactory<Program> app, string id)
    {
        using var admin = Admin(app);
        using var made = await admin.PostAsJsonAsync($"/api/rights/identities/{id}/keys", new { }, Web, Ct);
        made.EnsureSuccessStatusCode();
        return (await made.Content.ReadFromJsonAsync<JsonElement>(Web, Ct)).GetProperty("key").GetString()!;
    }

    /// <summary>Adds the person <paramref name="id"/> of <paramref name="role"/>, verified by a key, and answers the key made for it.</summary>
    private static async Task<string> PersonAsync(WebApplicationFactory<Program> app, string id, string role, string kind = IdentityKinds.Person)
    {
        using (var added = await AddAsync(app, id, role, kind)) Assert.True(added.IsSuccessStatusCode, await added.Content.ReadAsStringAsync(Ct));
        return await KeyAsync(app, id);
    }

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

    private static async Task<HttpStatusCode> StatusAsync(HttpClient client, string path, bool post = false)
    {
        using var response = post ? await client.PostAsync(path, null, Ct) : await client.GetAsync(path, Ct);
        return response.StatusCode;
    }

    private static async Task<HttpStatusCode> StatusAsync(WebApplicationFactory<Program> app, string token, string path, bool post = false)
    {
        using var client = Client(app, token);
        return await StatusAsync(client, path, post);
    }

    private static async Task<string[]> RolesAsync(HttpClient client) =>
        (await client.GetFromJsonAsync<CoordinatorMe>(CoordinatorApi.Me, Web, Ct))!.Roles.ToArray();

    private static async Task RevokeAsync(WebApplicationFactory<Program> app, string id, string keyId = "1")
    {
        using var admin = Admin(app);
        (await admin.PostAsync($"/api/rights/identities/{id}/keys/{keyId}/revoke", null, Ct)).EnsureSuccessStatusCode();
    }

    private static async Task EditAsync(WebApplicationFactory<Program> app, string id, object edit)
    {
        using var admin = Admin(app);
        (await admin.PutAsJsonAsync($"/api/rights/identities/{id}", edit, Web, Ct)).EnsureSuccessStatusCode();
    }

    /// <summary>Opens the form in <paramref name="browser"/> and answers its antiforgery token.</summary>
    private static async Task<string> FormTokenAsync(HttpClient browser)
    {
        var form = await browser.GetStringAsync(AccountPages.LoginPath + "?returnUrl=/library", Ct);
        return WebUtility.HtmlDecode(AntiforgeryField().Match(form).Groups[1].Value);
    }

    private static async Task<HttpResponseMessage> SignInAsync(HttpClient browser, string key, string? token, string returnUrl = "/library")
    {
        var fields = new Dictionary<string, string> { ["key"] = key, ["returnUrl"] = returnUrl };
        if (token is not null) fields["__RequestVerificationToken"] = token;
        return await browser.PostAsync(AccountPages.LoginPath, new FormUrlEncodedContent(fields), Ct);
    }

    private ClaimsPrincipal SessionOf(HttpResponseMessage signedIn)
    {
        var cookie = signedIn.Headers.GetValues("Set-Cookie").Single(c => c.StartsWith("dtpipe-coordinator=", StringComparison.Ordinal));
        var value = cookie["dtpipe-coordinator=".Length..cookie.IndexOf(';')];
        var format = _app.Services.GetRequiredService<IOptionsMonitor<CookieAuthenticationOptions>>().Get(TrustedIdp.CookieScheme).TicketDataFormat;
        return format.Unprotect(value)!.Principal;
    }

    private void AssertNoLogCarries(string key) =>
        Assert.DoesNotContain(_log.Entries, e => e.Contains(ClearKey.Prefix) || e.Contains(key[^ClearKey.SecretLength..][..8]) || e.Contains(key[^8..]));

    [Fact]
    public async Task A_Persons_Key_Is_Exchanged_For_A_Studio_Token_Of_An_Hour_Naming_Its_Key_And_Never_The_Key()
    {
        var key = await PersonAsync(_app, "alice", CoordinatorRoles.Designer);

        using var answer = await ExchangeAsync(_app, "alice", key);

        Assert.Equal(HttpStatusCode.OK, answer.StatusCode);
        Assert.Equal("no-store", answer.Headers.CacheControl?.ToString());
        var body = await answer.Content.ReadFromJsonAsync<JsonElement>(Web, Ct);
        Assert.Equal(3600, body.GetProperty("expires_in").GetInt32());
        var token = new JsonWebToken(body.GetProperty("access_token").GetString());
        Assert.Equal(PublicUrl, token.Issuer);
        Assert.Equal(new[] { CoordinatorHost.Audience }, token.Audiences);
        Assert.Equal("alice", token.Subject);
        Assert.Equal(CoordinatorHost.StudioClient, token.GetClaim("client_id").Value);
        Assert.Equal("1", token.GetClaim(TrustedIdp.KeyIdClaim).Value);
        Assert.Equal(TimeSpan.FromHours(1), token.ValidTo - token.IssuedAt);
        var hash = _app.Services.GetRequiredService<RightsStore>().Find("alice")!.Auth.Keys.Single().Hash;
        Assert.DoesNotContain(token.Claims, c => c.Value.Contains(key[^ClearKey.SecretLength..]) || c.Value.Contains(hash));
        // A person's token is no service's, on its own claims: the service schemes refuse it before they read the rights.
        Assert.False(TrustedIdp.IsService(new ClaimsPrincipal(new ClaimsIdentity(token.Claims))));
        Assert.True(TrustedIdp.IsStudioPerson(new ClaimsPrincipal(new ClaimsIdentity(token.Claims)), CoordinatorHost.StudioClient));
        AssertNoLogCarries(key);
    }

    [Fact]
    public async Task Without_A_Studio_Client_A_Person_Gets_No_Token_And_The_Refusal_Reads_As_Any_Other()
    {
        using var host = new CoordinatorHost(studioClientId: null, publicUrl: PublicUrl);
        // No bearer reaches /api without a studio's client, so the rights are written here.
        var rights = host.Services.GetRequiredService<RightsStore>();
        rights.AddIdentity(new Identity("alice", "alice", "", [], Kind: IdentityKinds.Person) { Auth = new IdentityAuth(AuthModes.Key, []), Role = CoordinatorRoles.Reader });
        var key = rights.AddKey("alice", null, null).Key.Reveal();

        using var refused = await ExchangeAsync(host, "alice", key);
        using var unknown = await ExchangeAsync(host, "ghost", ClearKey.New("ghost").Reveal());

        Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        Assert.Equal(await unknown.Content.ReadAsStringAsync(Ct), await refused.Content.ReadAsStringAsync(Ct));
    }

    [Fact]
    public async Task A_Persons_Token_Reaches_Api_With_The_Role_Its_Identity_Holds_Read_At_Every_Request()
    {
        var reader = Client(_app, await TokenAsync(_app, "rae-key", await PersonAsync(_app, "rae-key", CoordinatorRoles.Reader)));
        var admin = Client(_app, await TokenAsync(_app, "root-key", await PersonAsync(_app, "root-key", CoordinatorRoles.Admin)));
        var matrix = new { matrix = new Dictionary<string, string[]> { ["crm"] = ["crm"] } };

        var me = await reader.GetFromJsonAsync<CoordinatorMe>(CoordinatorApi.Me, Web, Ct);
        Assert.Equal(("rae-key", null), (me!.Name, me.Email));
        Assert.Equal([CoordinatorRoles.Reader], me.Roles);
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(reader, "/api/rights"));
        using (var refused = await reader.PutAsJsonAsync("/api/rights/matrix", matrix, Web, Ct))
            Assert.Equal(HttpStatusCode.Forbidden, refused.StatusCode);
        using (var kept = await admin.PutAsJsonAsync("/api/rights/matrix", matrix, Web, Ct))
            Assert.Equal(HttpStatusCode.OK, kept.StatusCode);
        Assert.Equal(CoordinatorRoles.All, await RolesAsync(admin));

        await EditAsync(_app, "rae-key", new { clientId = "rae-key", role = CoordinatorRoles.Designer });

        Assert.Equal([CoordinatorRoles.Designer, CoordinatorRoles.Reader], await RolesAsync(reader));
    }

    [Fact]
    public async Task A_Persons_Token_Is_Refused_At_The_Next_Request_Once_Its_Key_Is_Revoked_Or_Its_Identity_Disabled_Or_Removed()
    {
        var revoked = Client(_app, await TokenAsync(_app, "ann", await PersonAsync(_app, "ann", CoordinatorRoles.Reader)));
        var rotated = Client(_app, await TokenAsync(_app, "ann", await KeyAsync(_app, "ann")));
        var disabled = Client(_app, await TokenAsync(_app, "bob", await PersonAsync(_app, "bob", CoordinatorRoles.Reader)));
        var removed = Client(_app, await TokenAsync(_app, "cid", await PersonAsync(_app, "cid", CoordinatorRoles.Reader)));
        foreach (var client in new[] { revoked, rotated, disabled, removed })
            Assert.Equal(HttpStatusCode.OK, await StatusAsync(client, CoordinatorApi.Me));

        await RevokeAsync(_app, "ann");
        await EditAsync(_app, "bob", new { clientId = "bob", enabled = false });
        using (var admin = Admin(_app)) (await admin.DeleteAsync("/api/rights/identities/cid", Ct)).EnsureSuccessStatusCode();

        // Each token was valid for an hour more: the rights refuse it now, and only the token of the key revoked.
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(revoked, CoordinatorApi.Me));
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(rotated, CoordinatorApi.Me));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(disabled, CoordinatorApi.Me));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(removed, "/api/rights"));
    }

    [Fact]
    public async Task A_Persons_Token_Never_Reaches_The_Data_Plane_Health_Or_Service()
    {
        var token = await TokenAsync(_app, "root-key", await PersonAsync(_app, "root-key", CoordinatorRoles.Admin));

        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, token, Negotiate, post: true));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, token, "/hub/command/negotiate?negotiateVersion=1", post: true));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, token, "/health/pipelines"));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(_app, token, "/service/runs/run-1"));
        var rights = _app.Services.GetRequiredService<RightsStore>();
        Assert.Null(rights.NodeIdentity("root-key"));
        Assert.Throws<ConflictException>(() => rights.SubjectOf("root-key"));
        Assert.Empty(_app.Services.GetRequiredService<EnrollmentQueue>().List());
    }

    [Fact]
    public async Task A_Providers_Token_That_Names_A_Persons_Id_And_Key_Holds_None_Of_Its_Roles()
    {
        await PersonAsync(_app, "alice", CoordinatorRoles.Admin);
        var forged = CoordinatorHost.Token("alice", CoordinatorHost.StudioClient, extra: new Dictionary<string, object> { [TrustedIdp.KeyIdClaim] = "1" });
        using var client = Client(_app, forged);

        Assert.Empty(await RolesAsync(client));
        Assert.Equal(HttpStatusCode.Forbidden, await StatusAsync(client, "/api/rights"));
    }

    [Fact]
    public async Task The_Form_Signs_A_Person_In_With_Its_Key_Into_A_Session_That_Holds_Its_Id_And_Never_The_Key()
    {
        var key = await PersonAsync(_app, "alice", CoordinatorRoles.Designer);
        using var browser = Browser(_app);

        using var form = await browser.GetAsync(AccountPages.LoginPath + "?returnUrl=/library", Ct);
        var page = await form.Content.ReadAsStringAsync(Ct);
        Assert.Equal(HttpStatusCode.OK, form.StatusCode);
        Assert.True(form.Headers.CacheControl?.NoStore);
        Assert.Contains("name=\"key\"", page);
        Assert.Contains($"href=\"{AccountPages.ProviderLoginPath}?returnUrl=%2Flibrary\"", page);

        using var signedIn = await SignInAsync(browser, key, WebUtility.HtmlDecode(AntiforgeryField().Match(page).Groups[1].Value));

        Assert.Equal(HttpStatusCode.Redirect, signedIn.StatusCode);
        Assert.Equal("/library", signedIn.Headers.Location?.ToString());
        var session = SessionOf(signedIn);
        Assert.Equal([("sub", "alice"), (TrustedIdp.KeyIdClaim, "1")], session.Claims.Select(c => (c.Type, c.Value)));
        var me = await browser.GetFromJsonAsync<CoordinatorMe>(CoordinatorApi.Me, Web, Ct);
        Assert.Equal("alice", me!.Name);
        Assert.Equal([CoordinatorRoles.Designer, CoordinatorRoles.Reader], me.Roles);
        AssertNoLogCarries(key);
    }

    [Fact]
    public async Task A_Session_Ends_At_The_Next_Request_Once_Its_Key_Is_Revoked_Or_Its_Identity_Disabled()
    {
        var ann = await PersonAsync(_app, "ann", CoordinatorRoles.Reader);
        var bob = await PersonAsync(_app, "bob", CoordinatorRoles.Reader);
        using var revoked = Browser(_app);
        using var disabled = Browser(_app);
        (await SignInAsync(revoked, ann, await FormTokenAsync(revoked))).Dispose();
        (await SignInAsync(disabled, bob, await FormTokenAsync(disabled))).Dispose();
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(revoked, CoordinatorApi.Me));
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(disabled, CoordinatorApi.Me));

        await RevokeAsync(_app, "ann");
        await EditAsync(_app, "bob", new { clientId = "bob", enabled = false });

        using var ended = await revoked.GetAsync(CoordinatorApi.Me, Ct);
        Assert.Equal(HttpStatusCode.Unauthorized, ended.StatusCode);
        Assert.Contains(ended.Headers.GetValues("Set-Cookie"), c => c.StartsWith("dtpipe-coordinator=;", StringComparison.Ordinal));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(disabled, CoordinatorApi.Me));
        // A page opened then is a page opened by nobody: the form again.
        using var page = await revoked.GetAsync("/", Ct);
        Assert.StartsWith(AccountPages.LoginPath + "?returnUrl=", page.Headers.Location?.ToString());
    }

    [Fact]
    public async Task A_Person_Of_A_Key_Signs_Out_Here_And_Never_At_The_Provider()
    {
        var key = await PersonAsync(_app, "alice", CoordinatorRoles.Reader);
        using var browser = Browser(_app);
        (await SignInAsync(browser, key, await FormTokenAsync(browser))).Dispose();

        using var signedOut = await browser.GetAsync("/account/logout", Ct);

        Assert.Equal(HttpStatusCode.Redirect, signedOut.StatusCode);
        Assert.Equal("/", signedOut.Headers.Location?.ToString());
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(browser, CoordinatorApi.Me));
    }

    [Fact]
    public async Task A_Form_Without_Its_Antiforgery_Token_Opens_Nothing()
    {
        var key = await PersonAsync(_app, "alice", CoordinatorRoles.Reader);
        using var browser = Browser(_app);
        await FormTokenAsync(browser);

        using var refused = await SignInAsync(browser, key, token: null);

        Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        Assert.Contains("expired", Alert().Match(await refused.Content.ReadAsStringAsync(Ct)).Groups[1].Value);
        Assert.DoesNotContain(refused.Headers.TryGetValues("Set-Cookie", out var set) ? set : [], c => c.StartsWith("dtpipe-coordinator=", StringComparison.Ordinal));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(browser, CoordinatorApi.Me));
    }

    [Fact]
    public async Task Every_Refusal_Of_A_Key_At_The_Form_Reads_The_Same_And_No_Log_Carries_It()
    {
        var revoked = await PersonAsync(_app, "gone", CoordinatorRoles.Reader);
        var disabled = await PersonAsync(_app, "off", CoordinatorRoles.Reader);
        var node = await PersonAsync(_app, "edge", role: null!, kind: IdentityKinds.Node);
        await PersonAsync(_app, "alice", CoordinatorRoles.Reader);
        await RevokeAsync(_app, "gone");
        await EditAsync(_app, "off", new { clientId = "off", enabled = false });
        using var browser = Browser(_app);
        var token = await FormTokenAsync(browser);

        var presented = new[] { ClearKey.New("ghost").Reveal(), ClearKey.New("alice").Reveal(), revoked, disabled, node, "not a key" };
        var answers = new List<(HttpStatusCode, string)>();
        foreach (var key in presented)
        {
            using var refused = await SignInAsync(browser, key, token);
            Assert.True(refused.Headers.CacheControl?.NoStore);
            answers.Add((refused.StatusCode, Alert().Match(await refused.Content.ReadAsStringAsync(Ct)).Groups[1].Value));
        }

        Assert.Equal((HttpStatusCode.BadRequest, "This key signs nobody in here."), Assert.Single(answers.Distinct()));
        Assert.Equal(HttpStatusCode.Unauthorized, await StatusAsync(browser, CoordinatorApi.Me));
        Assert.Contains(_log.Entries, e => e.Contains("A sign-in with a key for alice"));
        foreach (var key in presented.Where(k => k.StartsWith(ClearKey.Prefix, StringComparison.Ordinal))) AssertNoLogCarries(key);
    }

    [Fact]
    public async Task A_Page_Opened_By_Nobody_Is_Sent_To_The_Form_Where_People_Hold_Keys_And_To_The_Provider_Elsewhere()
    {
        using var keyed = Browser(_app);
        using var provider = new CoordinatorHost();
        using var plain = provider.Anonymous();

        using var toForm = await keyed.GetAsync("/?view=runs", Ct);
        using var toProvider = await plain.GetAsync("/", Ct);
        using var login = await plain.GetAsync(AccountPages.LoginPath, Ct);

        Assert.Equal($"{AccountPages.LoginPath}?returnUrl=%2F%3Fview%3Druns", toForm.Headers.Location?.ToString());
        Assert.StartsWith(CoordinatorHost.Issuer + "connect/authorize", toProvider.Headers.Location?.ToString());
        Assert.StartsWith(CoordinatorHost.Issuer + "connect/authorize", login.Headers.Location?.ToString());
    }

    [Theory]
    [InlineData("/library", "/library")]
    [InlineData("//evil.test/x", "/")]
    [InlineData("/\\evil.test", "/")]
    [InlineData("https://evil.test", "/")]
    [InlineData("", "/")]
    [InlineData(null, "/")]
    public void The_Form_Sends_A_Person_Back_To_A_Path_Of_This_Server_Only(string? returnUrl, string expected) =>
        Assert.Equal(expected, AccountPages.LocalPath(returnUrl));

    [Fact]
    public async Task Over_Http_No_Form_Offers_A_Key_Unless_The_Setting_Allows_It()
    {
        using var plain = new CoordinatorHost(publicUrl: "http://coordinator.test");
        using var allowed = new CoordinatorHost(publicUrl: "http://coordinator.test", settings: new Dictionary<string, string> { ["Auth:AllowHttpKeys"] = "true" });
        var key = await PersonAsync(allowed, "alice", CoordinatorRoles.Reader);
        using var refusing = plain.Anonymous();
        using var browser = allowed.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false, HandleCookies = true });

        using var toProvider = await refusing.GetAsync(AccountPages.LoginPath, Ct);
        var metadata = await refusing.GetFromJsonAsync<ResourceMetadata>(CoordinatorApi.Metadata, Ct);
        using var signedIn = await SignInAsync(browser, key, await FormTokenAsync(browser));

        Assert.StartsWith(CoordinatorHost.Issuer + "connect/authorize", toProvider.Headers.Location?.ToString());
        Assert.Equal([PersonAuth.Idp], metadata!.Auth);
        Assert.Equal(HttpStatusCode.Redirect, signedIn.StatusCode);
        Assert.Contains("Identity keys cross the network in clear", await browser.GetStringAsync(AccountPages.LoginPath, Ct));
    }

    [Fact]
    public async Task A_Person_Without_An_Email_Is_Named_By_Its_Id_In_The_Audit_And_On_What_It_Sets()
    {
        var admin = Client(_app, await TokenAsync(_app, "root-key", await PersonAsync(_app, "root-key", CoordinatorRoles.Admin)));

        using (var added = await admin.PostAsJsonAsync("/api/rights/identities", new { clientId = "edge", groups = new[] { "crm" } }, Web, Ct))
            added.EnsureSuccessStatusCode();
        using var cursor = await admin.PutAsJsonAsync(CoordinatorApi.Cursor("sales", "orders"), new { type = "integer", value = "42" }, Web, Ct);

        var audit = _app.Services.GetRequiredService<RightsStore>().Snapshot().Audit!;
        Assert.Equal("root-key", audit.First(a => a.Change.StartsWith("Identity edge added", StringComparison.Ordinal)).By);
        Assert.Equal(CoordinatorHost.Admin, audit.First(a => a.Change.StartsWith("Identity root-key added", StringComparison.Ordinal)).By);
        Assert.Equal("root-key", (await cursor.Content.ReadFromJsonAsync<JsonElement>(Web, Ct)).GetProperty("setBy").GetString());
    }

    [Fact]
    public async Task A_Person_Holds_A_Role_A_Key_And_Neither_Group_Nor_Pipeline()
    {
        using (var noRole = await AddAsync(_app, "ann", role: null)) Assert.Equal(HttpStatusCode.Conflict, noRole.StatusCode);
        using (var badRole = await AddAsync(_app, "ann", role: "owner")) Assert.Equal(HttpStatusCode.Conflict, badRole.StatusCode);
        using var byProvider = await AddAsync(_app, "ann", CoordinatorRoles.Reader, auth: AuthModes.Idp);
        Assert.Equal(HttpStatusCode.Conflict, byProvider.StatusCode);
        Assert.Contains("Coordinator:Roles", await byProvider.Content.ReadAsStringAsync(Ct));

        await PersonAsync(_app, "ann", CoordinatorRoles.Reader);

        var ann = _app.Services.GetRequiredService<RightsStore>().Find("ann")!;
        Assert.Equal((IdentityKinds.Person, CoordinatorRoles.Reader, AuthModes.Key), (ann.Kind, ann.Role, ann.Auth.Mode));
        Assert.Empty(ann.Groups);
        Assert.Empty(ann.Pipelines!);
        using var reader = Client(_app, CoordinatorHost.Person(CoordinatorHost.Reader));
        var listed = (await reader.GetFromJsonAsync<JsonElement>("/api/rights", Web, Ct)).GetProperty("identities").EnumerateArray().Single();
        Assert.Equal((IdentityKinds.Person, CoordinatorRoles.Reader), (listed.GetProperty("kind").GetString(), listed.GetProperty("role").GetString()));
        // An id is unique among the identities of every kind: no service may take a person's.
        using var taken = await AddAsync(_app, "ann", role: null, kind: IdentityKinds.Launcher);
        Assert.Equal(HttpStatusCode.Conflict, taken.StatusCode);
    }

    [Fact]
    public async Task Without_Person_Keys_A_Person_Is_Refused_By_Name_The_Form_Offers_No_Key_And_The_Metadata_Names_None()
    {
        using var host = new CoordinatorHost(publicUrl: PublicUrl, settings: new Dictionary<string, string> { ["Auth:PersonKeys"] = "false" });

        using var refused = await AddAsync(host, "ann", CoordinatorRoles.Reader);
        using var client = host.Anonymous();
        using var login = await client.GetAsync(AccountPages.LoginPath, Ct);
        using var post = await client.PostAsync(AccountPages.LoginPath, new FormUrlEncodedContent([]), Ct);
        var metadata = await client.GetFromJsonAsync<JsonElement>(CoordinatorApi.Metadata, Ct);

        Assert.Equal(HttpStatusCode.Conflict, refused.StatusCode);
        Assert.Contains("Coordinator:Auth:PersonKeys", await refused.Content.ReadAsStringAsync(Ct));
        Assert.StartsWith(CoordinatorHost.Issuer + "connect/authorize", login.Headers.Location?.ToString());
        Assert.False(post.IsSuccessStatusCode || post.StatusCode == HttpStatusCode.Redirect);
        Assert.Equal([PersonAuth.Idp], metadata.GetProperty("dtpipe_auth").EnumerateArray().Select(e => e.GetString()));
        // A service's key is untouched.
        var edge = await PersonAsync(host, "edge", role: null!, kind: IdentityKinds.Node);
        using var exchanged = await ExchangeAsync(host, "edge", edge);
        Assert.Equal(HttpStatusCode.OK, exchanged.StatusCode);
    }

    [Fact]
    public async Task The_Metadata_Names_A_Key_Beside_The_Provider_And_The_Contract_It_Speaks()
    {
        using var client = Client(_app);

        var metadata = await client.GetFromJsonAsync<ResourceMetadata>(CoordinatorApi.Metadata, Ct);

        Assert.Equal(CoordinatorApi.Contract, metadata!.DtpipeContract);
        Assert.Equal([PersonAuth.Idp, PersonAuth.Key], metadata.Auth);
        Assert.Equal([CoordinatorHost.Issuer.TrimEnd('/'), PublicUrl], metadata.AuthorizationServers);
    }

    [Fact]
    public void The_Settings_Say_How_A_Person_Signs_In_And_For_How_Long()
    {
        (string, string)[] external = [("Authority", "https://idp.example"), ("Ui:ClientSecret", "s")];

        var keyed = Options([.. external, ("PublicUrl", "https://c.example")]);
        Assert.Equal((true, TimeSpan.FromHours(1)), (keyed.PersonKeys, keyed.PersonTokenLifetime));
        Assert.Equal([PersonAuth.Idp, PersonAuth.Key], keyed.PersonAuth);
        Assert.Equal([PersonAuth.Idp], Options([.. external, ("PublicUrl", "https://c.example"), ("Auth:PersonKeys", "false")]).PersonAuth);
        Assert.Equal([PersonAuth.Idp], Options([.. external, ("PublicUrl", "http://c.example")]).PersonAuth);
        Assert.Equal([PersonAuth.Idp, PersonAuth.Key], Options([.. external, ("PublicUrl", "http://c.example"), ("Auth:AllowHttpKeys", "true")]).PersonAuth);
        Assert.Equal([PersonAuth.Idp], Options(external).PersonAuth);
        Assert.Equal(TimeSpan.FromMinutes(30), Options([.. external, ("Auth:PersonTokenLifetime", "00:30:00")]).PersonTokenLifetime);
        var tooShort = Assert.Throws<InvalidOperationException>(() => Options([.. external, ("Auth:PersonTokenLifetime", "00:04:00")]));
        Assert.Contains("Coordinator:Auth:PersonTokenLifetime", tooShort.Message);
    }

    [Fact]
    public void People_Verified_By_A_Key_Stop_A_Start_That_No_Longer_Lets_People_Hold_One()
    {
        (string, string)[] keyed = [("Authority", "https://idp.example"), ("Ui:ClientSecret", "s"), ("PublicUrl", "https://c.example")];
        var rights = new RightsStore(MemoryRightsStore.At(Guid.NewGuid().ToString("N")), Options(keyed));
        rights.AddIdentity(new Identity("ann", "ann", "", [], Kind: IdentityKinds.Person) { Auth = new IdentityAuth(AuthModes.Key, []), Role = CoordinatorRoles.Reader });
        KeyIssuer.CheckIdentities(rights, Options(keyed));

        var off = Assert.Throws<InvalidOperationException>(() => KeyIssuer.CheckIdentities(rights, Options([.. keyed, ("Auth:PersonKeys", "false")])));

        Assert.Contains("ann", off.Message);
        Assert.Contains("Coordinator:Auth:PersonKeys", off.Message);
    }
}
