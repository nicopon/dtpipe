using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// What editing an identity does to the hosts that carry it: the rights route drops the control connection of every
/// replica of the node when its identity can no longer be what they announced.
/// </summary>
public sealed class RightsRoutesTests : IDisposable
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly CoordinatorHost _host = new();
    private readonly HttpClient _admin;
    private int _aborts;

    public RightsRoutesTests()
    {
        _admin = _host.Bearer(CoordinatorHost.Person(CoordinatorHost.Admin));
        _admin.DefaultRequestHeaders.Add(CoordinatorApi.CsrfHeader, "1");
    }

    public void Dispose()
    {
        _admin.Dispose();
        _host.Dispose();
    }

    private sealed record IdentityBody(string ClientId, string Name, string Description, string[] Groups, bool Enabled);

    private Task<HttpResponseMessage> Put(IdentityBody body) =>
        _admin.PutAsJsonAsync($"/api/rights/identities/{body.ClientId}", body, Web, Ct);

    /// <summary>An identity <c>a</c> for the node <c>node-a</c>, announced by one replica whose connection counts how often it is dropped.</summary>
    private async Task<IdentityBody> AnIdentityWithAReplicaAsync(bool enabled = true)
    {
        var identity = new IdentityBody("a", "node-a", "A", ["crm"], enabled);
        using var created = await _admin.PostAsJsonAsync("/api/rights/identities", identity, Web, Ct);
        created.EnsureSuccessStatusCode();
        _host.Services.GetRequiredService<ReplicaRegistry>().Announce(
            "c1", "a", new NodeAnnouncement("node-a", "", [], NodeRole.Data, []), identity.Groups, () => Interlocked.Increment(ref _aborts));
        return identity;
    }

    [Fact]
    public async Task Disabling_An_Identity_Drops_The_Control_Connection_Of_Its_Replicas()
    {
        var identity = await AnIdentityWithAReplicaAsync();

        using var response = await Put(identity with { Enabled = false });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(1, _aborts);
    }

    [Fact]
    public async Task An_Edit_That_Leaves_The_Identity_Enabled_And_Its_Name_And_Groups_Alone_Drops_Nothing()
    {
        var identity = await AnIdentityWithAReplicaAsync();

        using var response = await Put(identity with { Description = "described again" });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(0, _aborts);
    }

    /// <summary>The identity <c>a</c> as the rights hold it now, read back through the route the page reads.</summary>
    private async Task<JsonElement> StoredAsync()
    {
        var rights = await _admin.GetFromJsonAsync<JsonElement>("/api/rights", Web, Ct);
        return rights.GetProperty("identities").EnumerateArray().Single(i => i.GetProperty("clientId").GetString() == "a");
    }

    /// <summary>The edit of <c>a</c> that sends every field of <paramref name="body"/> but <paramref name="omitted"/>.</summary>
    private Task<HttpResponseMessage> PutWithout(string omitted, IdentityBody body)
    {
        var fields = new Dictionary<string, object>
        {
            ["clientId"] = body.ClientId, ["name"] = body.Name, ["description"] = body.Description, ["groups"] = body.Groups, ["enabled"] = body.Enabled,
        };
        fields.Remove(omitted);
        return _admin.PutAsJsonAsync("/api/rights/identities/a", fields, Web, Ct);
    }

    /// <summary>An edit says what it changes: a field it leaves out stays as the identity has it, and the omission neither enables, renames, empties nor refuses.</summary>
    [Fact]
    public async Task An_Edit_That_Omits_Enabled_Leaves_A_Disabled_Identity_Disabled()
    {
        var identity = await AnIdentityWithAReplicaAsync(enabled: false);

        using var response = await PutWithout("enabled", identity with { Description = "described again" });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var stored = await StoredAsync();
        Assert.False(stored.GetProperty("enabled").GetBoolean());
        Assert.Equal("described again", stored.GetProperty("description").GetString());
    }

    [Fact]
    public async Task An_Edit_That_Omits_Enabled_Leaves_An_Enabled_Identity_Enabled_And_Drops_Nothing()
    {
        var identity = await AnIdentityWithAReplicaAsync();

        using var response = await PutWithout("enabled", identity with { Description = "described again" });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True((await StoredAsync()).GetProperty("enabled").GetBoolean());
        Assert.Equal(0, _aborts);
    }

    [Fact]
    public async Task An_Edit_That_Omits_The_Name_Keeps_It_And_Drops_Nothing()
    {
        var identity = await AnIdentityWithAReplicaAsync();

        using var response = await PutWithout("name", identity with { Description = "described again" });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("node-a", (await StoredAsync()).GetProperty("name").GetString());
        Assert.Equal(0, _aborts);
    }

    [Fact]
    public async Task An_Edit_That_Omits_The_Description_Keeps_It()
    {
        var identity = await AnIdentityWithAReplicaAsync();

        using var response = await PutWithout("description", identity);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("A", (await StoredAsync()).GetProperty("description").GetString());
    }

    [Fact]
    public async Task An_Edit_That_Omits_The_Groups_Keeps_Them_And_Drops_Nothing()
    {
        var identity = await AnIdentityWithAReplicaAsync();

        using var response = await PutWithout("groups", identity with { Description = "described again" });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(["crm"], (await StoredAsync()).GetProperty("groups").EnumerateArray().Select(g => g.GetString()));
        Assert.Equal(0, _aborts);
    }

    [Fact]
    public async Task Enabling_An_Identity_Drops_Nothing()
    {
        var identity = await AnIdentityWithAReplicaAsync(enabled: false);

        using var response = await Put(identity with { Enabled = true });

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(0, _aborts);
    }
}
