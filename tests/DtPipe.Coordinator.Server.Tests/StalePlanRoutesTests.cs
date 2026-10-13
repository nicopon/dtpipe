using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// A plan saved for a pipeline places each fragment on a node under the groups the rights gave it then. When a node's groups
/// change, the plan is no longer the one a new distribution would make: it is listed as stale and its deployment is refused,
/// with the cause.
/// </summary>
public sealed class StalePlanRoutesTests : IDisposable
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly CoordinatorHost _host = new();
    private readonly HttpClient _admin;

    public StalePlanRoutesTests()
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

    private sealed record Listed(string Id, bool Distributed, bool PlanCurrent, string? PlanStaleCause);

    private async Task<PipelineLibrary> APipelinePlacedOnNodeAAsync()
    {
        using var created = await _admin.PostAsJsonAsync("/api/rights/identities", new IdentityBody("a", "node-a", "A", ["crm"], true), Web, Ct);
        created.EnsureSuccessStatusCode();
        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        await library.SaveAsync("orders", "a:\n  input: x\n", "create", Ct);
        var fragment = new PlannedFragment("orders@node-a", "node-a", ["crm"], "", "v", [], [], FragmentForm.Bricks);
        await library.SavePlanAsync("orders", new DistributionPlan("plan-1", "orders", [], [], [fragment], [], [], [], null), "plan", Ct);
        return library;
    }

    private async Task<Listed> ListedAsync() =>
        Assert.Single((await _admin.GetFromJsonAsync<Listed[]>("/api/library", Web, Ct))!);

    private Task<HttpResponseMessage> ChangeGroupsOfNodeA(params string[] groups) =>
        _admin.PutAsJsonAsync("/api/rights/identities/a", new IdentityBody("a", "node-a", "A", groups, true), Web, Ct);

    [Fact]
    public async Task ASavedPlanIsListedCurrent_WhileTheGroupsOfItsNodesAreUnchanged()
    {
        await APipelinePlacedOnNodeAAsync();

        var listed = await ListedAsync();

        Assert.True(listed.Distributed);
        Assert.True(listed.PlanCurrent);
        Assert.Null(listed.PlanStaleCause);
    }

    [Fact]
    public async Task ASavedPlanIsListedStale_AndItsDeploymentRefused_WhenTheGroupsOfANodeChange()
    {
        await APipelinePlacedOnNodeAAsync();

        using var changed = await ChangeGroupsOfNodeA("crm", "audit");
        changed.EnsureSuccessStatusCode();

        var listed = await ListedAsync();
        Assert.False(listed.PlanCurrent);
        Assert.Equal("groups", listed.PlanStaleCause);
        using var deploy = await _admin.PostAsJsonAsync("/api/library/orders/deploy", new { }, Web, Ct);
        Assert.Equal(HttpStatusCode.Conflict, deploy.StatusCode);
        var error = (await deploy.Content.ReadFromJsonAsync<JsonElement>(Ct)).GetProperty("error").GetString()!;
        Assert.Contains("groups", error);
        Assert.Contains("distribute it again", error);
    }

    /// <summary>The cause of a plan made from another job stays its own: the job changed after its plan was saved.</summary>
    [Fact]
    public async Task ADeploymentOfAPlanWhoseJobChanged_SaysTheJobChanged()
    {
        var library = await APipelinePlacedOnNodeAAsync();
        await library.SaveAsync("orders", "a:\n  input: y\n", "edit", Ct);

        var listed = await ListedAsync();
        Assert.False(listed.PlanCurrent);
        Assert.Equal("job", listed.PlanStaleCause);
        using var deploy = await _admin.PostAsJsonAsync("/api/library/orders/deploy", new { }, Web, Ct);

        Assert.Equal(HttpStatusCode.Conflict, deploy.StatusCode);
        var error = (await deploy.Content.ReadFromJsonAsync<JsonElement>(Ct)).GetProperty("error").GetString()!;
        Assert.Contains("changed since its plan was saved", error);
        Assert.DoesNotContain("groups", error);
    }

    /// <summary>
    /// A node announced under one group and edited to another while it is offline: distributing again makes a plan that is
    /// current. The planner takes the groups the rights give the node now, not those of its last announcement.
    /// </summary>
    [Fact]
    public async Task DistributingAgain_RepairsAPlanWhoseNodeChangedGroupsWhileItWasOffline()
    {
        var source = new BrickInfo("customers", BrickKind.Source, "Customers", "", """{"input":"sqlite:${{LAB_DB}}"}""", null, null);
        foreach (var identity in new[] { new IdentityBody("a", "node-a", "A", ["crm"], true), new IdentityBody("r", "runner", "R", ["run"], true) })
        {
            using var created = await _admin.PostAsJsonAsync("/api/rights/identities", identity, Web, Ct);
            created.EnsureSuccessStatusCode();
        }
        using var matrix = await _admin.PutAsJsonAsync("/api/rights/matrix", new { matrix = new Dictionary<string, string[]> { ["crm"] = ["run"], ["audit"] = ["run"] } }, Web, Ct);
        matrix.EnsureSuccessStatusCode();
        var registry = _host.Services.GetRequiredService<ReplicaRegistry>();
        var inventory = _host.Services.GetRequiredService<NodeInventory>();
        void Announce(string connection, string subject, NodeAnnouncement announcement, string group)
        {
            registry.Announce(connection, subject, announcement, [group], () => { });
            inventory.Announced(announcement.Name, subject, announcement, [group]);
        }
        Announce("c-a", "a", new NodeAnnouncement("node-a", "", [new DatasetInfo("LAB_DB", "sqlite", "")], NodeRole.Data, [source], InstanceId: Guid.NewGuid()), "crm");
        Announce("c-r", "r", new NodeAnnouncement("runner", "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid()), "run");
        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        await library.SaveAsync("orders", "a:\n  input: sqlite:${{LAB_DB}}\nb:\n  from: a\n  output: csv:out.csv\n", "create", Ct);

        using var first = await _admin.PostAsJsonAsync("/api/library/orders/distribute", new { save = true }, Web, Ct);
        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.True((await ListedAsync()).PlanCurrent);

        using var changed = await _admin.PutAsJsonAsync("/api/rights/identities/a", new IdentityBody("a", "node-a", "A", ["audit"], true), Web, Ct);
        changed.EnsureSuccessStatusCode();
        registry.Disconnected("c-a");
        Assert.Equal("groups", (await ListedAsync()).PlanStaleCause);

        using var again = await _admin.PostAsJsonAsync("/api/library/orders/distribute", new { save = true }, Web, Ct);

        Assert.Equal(HttpStatusCode.OK, again.StatusCode);
        var listed = await ListedAsync();
        Assert.True(listed.PlanCurrent);
        Assert.Null(listed.PlanStaleCause);
    }
}
