using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The plan of a library pipeline is computed when it is asked for or launched, from the job, the bindings and the rights as they
/// are then: nothing is saved, so nothing goes stale. A deployment is refused for bindings that do not hold.
/// </summary>
public sealed class LaunchRoutesTests : IDisposable
{
    private const string Job = "a:\n  input: sqlite:${{LAB_DB}}\nb:\n  from: a\n  output: csv:out.csv\n";
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly CoordinatorHost _host = new(check: _ => new DesignCheck(true, null));
    private readonly HttpClient _admin;

    public LaunchRoutesTests()
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

    /// <summary>Node A offers the customers brick under <c>crm</c>, a runner is connected, and A may send to it.</summary>
    private async Task NodeAAndARunnerAsync()
    {
        var source = TestBricks.WithText("customers", BrickKind.Source, """{"input":"sqlite:${{LAB_DB}}"}""", "Customers");
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
        Announce("c-a", "a", new NodeAnnouncement("node-a", "", [new DatasetInfo("LAB_DB", "")], NodeRole.Data, [source], InstanceId: Guid.NewGuid()), "crm");
        Announce("c-r", "r", new NodeAnnouncement("runner", "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid()), "run");
    }

    private async Task<JsonElement> PlanAsync() =>
        await _admin.GetFromJsonAsync<JsonElement>("/api/library/orders/plan", Web, Ct);

    /// <summary>The preview names the nodes that offer each brick and the runner a launch would use, and writes nothing.</summary>
    [Fact]
    public async Task The_Plan_Is_A_Preview_Of_A_Launch_And_Writes_Nothing()
    {
        await NodeAAndARunnerAsync();
        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        await library.SaveAsync("orders", Job, "create", Ct);
        await library.SaveBindingsAsync("orders", new Dictionary<string, Binding> { ["a"] = new Binding("node-a") }, "bind", Ct);
        var history = (await library.HistoryAsync("orders", Ct)).Count;

        var answer = await PlanAsync();

        Assert.Equal("node-a", Assert.Single(answer.GetProperty("offers").GetProperty("a").EnumerateArray()).GetString());
        Assert.Equal(["orders@node-a", "orders@runner"], answer.GetProperty("plan").GetProperty("fragments").EnumerateArray().Select(f => f.GetProperty("name").GetString()).Order());
        Assert.Equal(history, (await library.HistoryAsync("orders", Ct)).Count);
        Assert.False(Directory.Exists(Path.Combine(_host.StateDirectory, "library", "pipelines", "orders", "distributed")));
    }

    /// <summary>A node edited to other groups is planned under them at the next preview: there is no saved plan to repair.</summary>
    [Fact]
    public async Task A_Change_Of_Groups_Is_Picked_Up_By_The_Next_Plan_With_No_Redistribution()
    {
        await NodeAAndARunnerAsync();
        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        await library.SaveAsync("orders", Job, "create", Ct);
        await library.SaveBindingsAsync("orders", new Dictionary<string, Binding> { ["a"] = new Binding("node-a") }, "bind", Ct);
        string[] GroupsOfA(JsonElement answer) => answer.GetProperty("plan").GetProperty("fragments").EnumerateArray()
            .Single(f => f.GetProperty("node").GetString() == "node-a").GetProperty("groups").EnumerateArray().Select(g => g.GetString()!).ToArray();
        Assert.Equal(["crm"], GroupsOfA(await PlanAsync()));

        using var changed = await _admin.PutAsJsonAsync("/api/rights/identities/a", new IdentityBody("a", "node-a", "A", ["audit"], true), Web, Ct);
        changed.EnsureSuccessStatusCode();

        Assert.Equal(["audit"], GroupsOfA(await PlanAsync()));
    }

    /// <summary>A brick branch with no node names the nodes that offer it; a binding on an absent node is a problem and refuses the deployment.</summary>
    [Fact]
    public async Task A_Brick_Needs_Its_Node_And_A_Binding_On_An_Absent_Node_Refuses_The_Deployment()
    {
        await NodeAAndARunnerAsync();
        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        await library.SaveAsync("orders", Job, "create", Ct);

        var unbound = await PlanAsync();
        Assert.Contains(unbound.GetProperty("plan").GetProperty("errors").EnumerateArray(), e => e.GetString()!.Contains("node-a"));

        using var moved = await _admin.PutAsJsonAsync("/api/library/orders/bindings", new { bindings = new Dictionary<string, Binding> { ["a"] = new Binding("node-gone") } }, Web, Ct);
        moved.EnsureSuccessStatusCode();
        var problems = (await moved.Content.ReadFromJsonAsync<JsonElement>(Ct)).GetProperty("problems");
        Assert.Equal("unknown-node", Assert.Single(problems.EnumerateArray()).GetProperty("cause").GetString());
        using var deploy = await _admin.PostAsync("/api/library/orders/deploy", null, Ct);
        Assert.Equal(HttpStatusCode.Conflict, deploy.StatusCode);
        Assert.Contains("do not hold", await deploy.Content.ReadAsStringAsync(Ct));
        Assert.Null(_host.Services.GetRequiredService<DeploymentManager>().ArmedOf("orders"));
    }

    [Fact]
    public async Task The_Plan_Of_A_Pipeline_The_Library_Lacks_Is_Not_Found()
    {
        using var response = await _admin.GetAsync("/api/library/nothing/plan", Ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }
}
