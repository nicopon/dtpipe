using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Jobs.Coordinator;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// An identity's keys through the admin's routes: a key is answered once, by the call that makes it, and is then nowhere: not in the
/// store, which keeps its hash, not in a list, which names it by its id and last characters, and not in a log.
/// </summary>
public sealed class IdentityKeyRoutesTests : IDisposable
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // An identity verified by a key needs the issuer, which PublicUrl turns on.
    private readonly CoordinatorHost _host = new(publicUrl: "https://coordinator.test");
    private readonly CapturedLog _log = new();
    private readonly WebApplicationFactory<Program> _logged;
    private readonly HttpClient _admin;
    private int _aborts;

    public IdentityKeyRoutesTests()
    {
        _logged = _host.WithWebHostBuilder(b => b.ConfigureLogging(l => l.AddProvider(_log).AddFilter<CapturedLog>(null, LogLevel.Trace)));
        _admin = _logged.CreateClient(new WebApplicationFactoryClientOptions { AllowAutoRedirect = false });
        _admin.DefaultRequestHeaders.Authorization = new("Bearer", CoordinatorHost.Person(CoordinatorHost.Admin));
        _admin.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
    }

    public void Dispose()
    {
        _admin.Dispose();
        _logged.Dispose();
        _host.Dispose();
    }

    private RightsStore Rights => _logged.Services.GetRequiredService<RightsStore>();

    /// <summary>The identity <c>a</c>, verified as <paramref name="auth"/> says, for the node <c>node-a</c>, announced by one replica.</summary>
    private async Task AnIdentityAsync(string auth = AuthModes.Key)
    {
        using var created = await _admin.PostAsJsonAsync("/api/rights/identities",
            new { clientId = "a", name = "node-a", description = "A", groups = new[] { "crm" }, auth }, Web, Ct);
        created.EnsureSuccessStatusCode();
        _logged.Services.GetRequiredService<ReplicaRegistry>().Announce(
            "c1", "a", new NodeAnnouncement("node-a", "", [], NodeRole.Data, []), ["crm"], () => Interlocked.Increment(ref _aborts));
    }

    private Task<HttpResponseMessage> MakeKeyAsync(object? body = null) =>
        _admin.PostAsJsonAsync("/api/rights/identities/a/keys", body ?? new { }, Web, Ct);

    private async Task<JsonElement> MadeKeyAsync(object? body = null)
    {
        using var made = await MakeKeyAsync(body);
        Assert.Equal(HttpStatusCode.OK, made.StatusCode);
        return await made.Content.ReadFromJsonAsync<JsonElement>(Web, Ct);
    }

    private Task<HttpResponseMessage> RevokeAsync(string keyId) =>
        _admin.PostAsync($"/api/rights/identities/a/keys/{keyId}/revoke", null, Ct);

    private async Task<string> ErrorOf(HttpResponseMessage response) =>
        (await response.Content.ReadFromJsonAsync<JsonElement>(Web, Ct)).GetProperty("error").GetString()!;

    [Fact]
    public async Task Making_A_Key_Answers_It_Once_And_The_Store_Keeps_Its_Hash_Only()
    {
        await AnIdentityAsync();

        using var made = await MakeKeyAsync(new { label = "runner" });
        var body = await made.Content.ReadFromJsonAsync<JsonElement>(Web, Ct);
        var key = body.GetProperty("key").GetString()!;

        Assert.Equal(HttpStatusCode.OK, made.StatusCode);
        Assert.True(made.Headers.CacheControl?.NoStore);
        Assert.StartsWith("dtp_a_", key);
        Assert.Equal("dtp_a_".Length + ClearKey.SecretLength, key.Length);
        Assert.Equal("1", body.GetProperty("id").GetString());
        Assert.Equal(key[^4..], body.GetProperty("hint").GetString());
        Assert.Equal("runner", body.GetProperty("label").GetString());
        var stored = _host.Rights.Load()!.Document;
        Assert.DoesNotContain(key[6..], stored);
        Assert.Contains(Rights.Find("a")!.Auth.Keys.Single().Hash, stored);
        Assert.Equal("a", Rights.KeyHolder(key, DateTimeOffset.UtcNow)?.ClientId);
    }

    [Fact]
    public async Task No_Log_Carries_The_Key_A_Call_Made()
    {
        await AnIdentityAsync();

        var key = (await MadeKeyAsync()).GetProperty("key").GetString()!;

        var entries = _log.Entries;
        Assert.Contains(entries, e => e.Contains("/api/rights/identities/a/keys"));
        Assert.DoesNotContain(entries, e => e.Contains(key[6..]));
    }

    [Fact]
    public async Task No_List_Answers_A_Key_Or_Its_Hash()
    {
        await AnIdentityAsync();
        var key = (await MadeKeyAsync()).GetProperty("key").GetString()!;
        var hash = Rights.Find("a")!.Auth.Keys.Single().Hash;

        var keys = await _admin.GetStringAsync("/api/rights/identities/a/keys", Ct);
        var rights = await _admin.GetStringAsync("/api/rights", Ct);

        foreach (var answer in new[] { keys, rights })
        {
            Assert.DoesNotContain(key[6..], answer);
            Assert.DoesNotContain(hash, answer, StringComparison.OrdinalIgnoreCase);
        }
        var listed = JsonSerializer.Deserialize<JsonElement>(keys, Web);
        Assert.Equal("key", listed.GetProperty("auth").GetString());
        Assert.True(listed.GetProperty("hasKey").GetBoolean());
        var only = Assert.Single(listed.GetProperty("keys").EnumerateArray());
        Assert.Equal("1", only.GetProperty("id").GetString());
        Assert.Equal(key[^4..], only.GetProperty("hint").GetString());
        Assert.True(only.GetProperty("live").GetBoolean());
        var identity = JsonSerializer.Deserialize<JsonElement>(rights, Web).GetProperty("identities").EnumerateArray().Single();
        Assert.Equal("key", identity.GetProperty("auth").GetString());
        Assert.True(identity.GetProperty("hasKey").GetBoolean());
    }

    [Fact]
    public async Task The_Keys_Of_An_Identity_That_Does_Not_Exist_Are_Not_Found()
    {
        using var response = await _admin.GetAsync("/api/rights/identities/nobody/keys", Ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task A_Revoked_Key_Opens_Nothing_Stays_Listed_And_Its_Replicas_Are_Cut()
    {
        await AnIdentityAsync();
        var key = (await MadeKeyAsync()).GetProperty("key").GetString()!;

        using var revoked = await RevokeAsync("1");

        Assert.Equal(HttpStatusCode.OK, revoked.StatusCode);
        Assert.Equal(1, _aborts);
        Assert.Null(Rights.KeyHolder(key, DateTimeOffset.UtcNow));
        var listed = await _admin.GetFromJsonAsync<JsonElement>("/api/rights/identities/a/keys", Web, Ct);
        Assert.False(listed.GetProperty("hasKey").GetBoolean());
        var only = Assert.Single(listed.GetProperty("keys").EnumerateArray());
        Assert.False(only.GetProperty("live").GetBoolean());
        Assert.NotEqual(JsonValueKind.Null, only.GetProperty("revoked").ValueKind);
    }

    [Fact]
    public async Task A_Rotation_Keeps_Two_Live_Keys_And_Refuses_A_Third_Until_The_Old_One_Is_Revoked()
    {
        await AnIdentityAsync();
        var old = (await MadeKeyAsync()).GetProperty("key").GetString()!;
        var fresh = (await MadeKeyAsync()).GetProperty("key").GetString()!;

        using var third = await MakeKeyAsync();
        Assert.Equal(HttpStatusCode.Conflict, third.StatusCode);
        Assert.Contains("revoke the old one", await ErrorOf(third));
        Assert.Equal("a", Rights.KeyHolder(old, DateTimeOffset.UtcNow)?.ClientId);
        Assert.Equal("a", Rights.KeyHolder(fresh, DateTimeOffset.UtcNow)?.ClientId);

        (await RevokeAsync("1")).EnsureSuccessStatusCode();
        var next = await MadeKeyAsync();

        Assert.Equal("3", next.GetProperty("id").GetString());
        Assert.Null(Rights.KeyHolder(old, DateTimeOffset.UtcNow));
    }

    [Fact]
    public async Task An_Identity_The_IdP_Verifies_Has_No_Key()
    {
        await AnIdentityAsync(AuthModes.Idp);

        using var refused = await MakeKeyAsync();

        Assert.Equal(HttpStatusCode.Conflict, refused.StatusCode);
        Assert.Contains("verified by the identity provider", await ErrorOf(refused));
        Assert.Empty(Rights.Find("a")!.Auth.Keys);
    }

    [Fact]
    public async Task A_Verification_That_Is_Neither_Is_Refused_When_The_Identity_Is_Added()
    {
        using var refused = await _admin.PostAsJsonAsync("/api/rights/identities",
            new { clientId = "a", name = "node-a", groups = new[] { "crm" }, auth = "password" }, Web, Ct);

        Assert.Equal(HttpStatusCode.Conflict, refused.StatusCode);
        Assert.Contains("idp or key", await ErrorOf(refused));
        Assert.Null(Rights.Find("a"));
    }

    [Fact]
    public async Task An_Edit_Leaves_The_Verification_As_The_Identity_Was_Added()
    {
        await AnIdentityAsync(AuthModes.Idp);

        using var edited = await _admin.PutAsJsonAsync("/api/rights/identities/a", new { description = "again", auth = "key" }, Web, Ct);

        Assert.Equal(HttpStatusCode.OK, edited.StatusCode);
        Assert.Equal(AuthModes.Idp, Rights.Find("a")!.Auth.Mode);
    }

    [Fact]
    public async Task A_Key_Of_An_Identity_That_Does_Not_Exist_Is_Refused()
    {
        using var refused = await MakeKeyAsync();

        Assert.Equal(HttpStatusCode.Conflict, refused.StatusCode);
        Assert.Contains("does not exist", await ErrorOf(refused));
    }

    [Fact]
    public async Task A_Key_That_Would_Have_Expired_Already_Is_Refused()
    {
        await AnIdentityAsync();

        using var refused = await MakeKeyAsync(new { expires = DateTimeOffset.UtcNow.AddMinutes(-1) });

        Assert.Equal(HttpStatusCode.Conflict, refused.StatusCode);
        Assert.Contains("expired already", await ErrorOf(refused));
        Assert.Empty(Rights.Find("a")!.Auth.Keys);
    }

    [Fact]
    public async Task A_Key_Revoked_Twice_Or_That_Does_Not_Exist_Is_Refused_By_Name()
    {
        await AnIdentityAsync();
        await MadeKeyAsync();
        (await RevokeAsync("1")).EnsureSuccessStatusCode();

        using var twice = await RevokeAsync("1");
        using var none = await RevokeAsync("7");

        Assert.Equal(HttpStatusCode.Conflict, twice.StatusCode);
        Assert.Contains("revoked already", await ErrorOf(twice));
        Assert.Equal(HttpStatusCode.Conflict, none.StatusCode);
        Assert.Contains("has no key 7", await ErrorOf(none));
    }

    [Fact]
    public async Task Making_And_Revoking_A_Key_Name_Their_Author_And_Never_The_Key()
    {
        await AnIdentityAsync();
        var key = (await MadeKeyAsync()).GetProperty("key").GetString()!;
        (await RevokeAsync("1")).EnsureSuccessStatusCode();

        var audit = Rights.Snapshot().Audit!;

        Assert.Equal(("Identity a: key 1 revoked", CoordinatorHost.Admin), (audit[0].Change, audit[0].By));
        Assert.Equal(("Identity a: key 1 made", CoordinatorHost.Admin), (audit[1].Change, audit[1].By));
        Assert.DoesNotContain(audit, e => e.Change.Contains(key[6..]));
    }

    [Fact]
    public async Task A_Store_That_Is_Down_Answers_503_To_A_Key_And_None_Is_Kept()
    {
        await AnIdentityAsync();
        _host.Rights.Down = true;

        using var refused = await MakeKeyAsync();

        Assert.Equal(HttpStatusCode.ServiceUnavailable, refused.StatusCode);
        Assert.DoesNotContain("dtp_", await refused.Content.ReadAsStringAsync(Ct));
        _host.Rights.Down = false;
        Assert.Empty(Rights.Find("a")!.Auth.Keys);
    }
}
