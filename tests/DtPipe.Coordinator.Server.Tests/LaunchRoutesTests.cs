using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Deployments;
using DtPipe.Coordinator.Server.Library;
using DtPipe.Coordinator.Server.Nodes;
using DtPipe.Jobs.Coordinator;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Hub;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The plan of a library pipeline is computed when it is asked for or launched, from the job, the bindings and the rights as they
/// are then: nothing is saved, so nothing goes stale. A deployment is refused for bindings that do not hold.
/// </summary>
public sealed class LaunchRoutesTests : IDisposable
{
    private const string Job = "a:\n  input: brick:node-a/customers\nb:\n  from: a\n";
    private const string Text = """{"input":"sqlite:${{LAB_DB}}"}""";

    /// <summary>What a check pinned for the reference to the brick: the version the node announces for it.</summary>
    private static Binding Checked(string mode = "strict") => new("node-a", "customers", mode, new BindingPin(TestBricks.VersionOf(BrickKind.Source, Text), mode == "strict" ? null : "{}"));
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

    /// <summary>
    /// Node A offers the customers brick under <c>crm</c>, a runner is connected, and A may send to it. Each protects the data plane as
    /// it is told to, in clear by default. When <paramref name="admitted"/>, each replica has a TransportR client registered under its
    /// subject with its groups, so that admission finds it eligible and the plan can be one a launch would run.
    /// </summary>
    private async Task NodeAAndARunnerAsync(ReplicaProtection? nodeA = null, ReplicaProtection? runner = null, bool admitted = false)
    {
        var source = TestBricks.Announced("customers", BrickKind.Source, Text, "Customers");
        foreach (var identity in new[] { new IdentityBody("a", "node-a", "A", ["crm"], true), new IdentityBody("r", "runner", "R", ["run"], true) })
        {
            using var created = await _admin.PostAsJsonAsync("/api/rights/identities", identity, Web, Ct);
            created.EnsureSuccessStatusCode();
        }
        using var matrix = await _admin.PutAsJsonAsync("/api/rights/matrix", new { matrix = new Dictionary<string, string[]> { ["crm"] = ["run"], ["audit"] = ["run"] } }, Web, Ct);
        matrix.EnsureSuccessStatusCode();
        var registry = _host.Services.GetRequiredService<ReplicaRegistry>();
        var inventory = _host.Services.GetRequiredService<NodeInventory>();
        var state = _host.Services.GetRequiredService<IStateStore>();
        async Task AnnounceAsync(string connection, string subject, NodeAnnouncement announcement, string group)
        {
            if (admitted)
            {
                var client = Guid.NewGuid();
                await state.SaveClientAsync(new ClientRegistration(client, "data-" + subject, [group], null, null, DateTime.UtcNow, Subject: subject));
                announcement = announcement with { DataPlaneClientId = client };
            }
            registry.Announce(connection, subject, announcement, [group], () => { });
            inventory.Announced(announcement.Name, subject, announcement, [group]);
        }
        await AnnounceAsync("c-a", "a", new NodeAnnouncement("node-a", "", [new DatasetInfo("LAB_DB", "")], NodeRole.Data, [source], InstanceId: Guid.NewGuid(),
            DataPlaneProtection: nodeA?.Level, KeyId: nodeA?.KeyId), "crm");
        await AnnounceAsync("c-r", "r", new NodeAnnouncement("runner", "", [], NodeRole.Runner, [], InstanceId: Guid.NewGuid(),
            DataPlaneProtection: runner?.Level, KeyId: runner?.KeyId), "run");
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
        await library.SaveBindingsAsync("orders", new Dictionary<string, Binding> { ["a"] = Checked() }, "bind", Ct);
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
        await library.SaveBindingsAsync("orders", new Dictionary<string, Binding> { ["a"] = Checked() }, "bind", Ct);
        string[] GroupsOfA(JsonElement answer) => answer.GetProperty("plan").GetProperty("fragments").EnumerateArray()
            .Single(f => f.GetProperty("node").GetString() == "node-a").GetProperty("groups").EnumerateArray().Select(g => g.GetString()!).ToArray();
        Assert.Equal(["crm"], GroupsOfA(await PlanAsync()));

        using var changed = await _admin.PutAsJsonAsync("/api/rights/identities/a", new IdentityBody("a", "node-a", "A", ["audit"], true), Web, Ct);
        changed.EnsureSuccessStatusCode();

        Assert.Equal(["audit"], GroupsOfA(await PlanAsync()));
    }

    /// <summary>
    /// A plan the matrix refuses names each refused edge with the groups of both its ends, which the page reads to propose what to
    /// allow, and a Deploy is refused naming them; once they are allowed beside the matrix, the next plan's edge passes it.
    /// </summary>
    [Fact]
    public async Task A_Refused_Edge_Names_The_Groups_Of_Its_Ends_And_Allowing_Them_Lets_The_Next_Plan_Through_The_Matrix()
    {
        await NodeAAndARunnerAsync();
        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        await library.SaveAsync("orders", Job, "create", Ct);
        await library.SaveBindingsAsync("orders", new Dictionary<string, Binding> { ["a"] = Checked() }, "bind", Ct);
        using var emptied = await _admin.PutAsJsonAsync("/api/rights/matrix", new { matrix = new Dictionary<string, string[]>() }, Web, Ct);
        emptied.EnsureSuccessStatusCode();

        var refused = (await PlanAsync()).GetProperty("plan");
        var edge = Assert.Single(refused.GetProperty("edges").EnumerateArray());
        using var deploy = await _admin.PostAsync("/api/library/orders/deploy", null, Ct);
        using var allowed = await _admin.PostAsJsonAsync("/api/rights/matrix/edges", new { edges = new[] { new { from = "crm", to = "run" } } }, Web, Ct);

        Assert.False(refused.GetProperty("deployable").GetBoolean());
        Assert.False(edge.GetProperty("allowed").GetBoolean());
        Assert.Equal(["crm"], edge.GetProperty("producerGroups").EnumerateArray().Select(g => g.GetString()));
        Assert.Equal(["run"], edge.GetProperty("consumerGroups").EnumerateArray().Select(g => g.GetString()));
        Assert.Equal(HttpStatusCode.Conflict, deploy.StatusCode);
        Assert.Contains("(crm -> run)", await deploy.Content.ReadAsStringAsync(Ct));
        allowed.EnsureSuccessStatusCode();
        var after = (await PlanAsync()).GetProperty("plan");
        Assert.True(Assert.Single(after.GetProperty("edges").EnumerateArray()).GetProperty("allowed").GetBoolean());
        Assert.Equal(JsonValueKind.Null, after.GetProperty("flowRejection").ValueKind);
    }

    /// <summary>A brick branch with no node names the nodes that offer it; a binding on an absent node is a problem and refuses the deployment.</summary>
    [Fact]
    public async Task A_Reference_Needs_Its_Version_Pinned_And_A_Reference_To_An_Absent_Node_Refuses_The_Deployment()
    {
        await NodeAAndARunnerAsync();
        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        await library.SaveAsync("orders", Job, "create", Ct);

        var unpinned = await PlanAsync();
        Assert.Contains(unpinned.GetProperty("plan").GetProperty("errors").EnumerateArray(), e => e.GetString()!.Contains("node-a"));

        await library.SaveAsync("orders", Job.Replace("node-a", "node-gone"), "move", Ct);
        using var moved = await _admin.PutAsJsonAsync("/api/library/orders/bindings", new { bindings = new Dictionary<string, Binding> { ["a"] = Checked() } }, Web, Ct);
        moved.EnsureSuccessStatusCode();
        var problems = (await moved.Content.ReadFromJsonAsync<JsonElement>(Ct)).GetProperty("problems");
        Assert.Equal("unknown-node", Assert.Single(problems.EnumerateArray()).GetProperty("cause").GetString());
        using var deploy = await _admin.PostAsync("/api/library/orders/deploy", null, Ct);
        Assert.Equal(HttpStatusCode.Conflict, deploy.StatusCode);
        Assert.Contains("do not hold", await deploy.Content.ReadAsStringAsync(Ct));
        Assert.Null(_host.Services.GetRequiredService<DeploymentManager>().ArmedOf("orders"));
    }

    private const string Key = "3f9a0c11d2e4b7a8";
    private const string OtherKey = "0123456789abcdef";
    private static readonly ReplicaProtection Keyed = new(DataPlaneProtections.EncryptedPsk, Key);
    private static readonly ReplicaProtection OtherKeyed = new(DataPlaneProtections.EncryptedPsk, OtherKey);

    private async Task BoundOrdersAsync()
    {
        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        await library.SaveAsync("orders", Job, "create", Ct);
        await library.SaveBindingsAsync("orders", new Dictionary<string, Binding> { ["a"] = Checked() }, "bind", Ct);
    }

    /// <summary>
    /// The plan of a deployable pipeline says, beside the plan and for each of its edges, the level the edge would be held at, with the
    /// property names a page reads. Two ends in clear are a level of none, a level that is known; a refused edge is held at no level
    /// and carries the sentence that names it, which arming gives too.
    /// </summary>
    [Theory]
    [InlineData(false, false, DataPlaneProtections.None, null)]
    [InlineData(true, true, DataPlaneProtections.EncryptedPsk, null)]
    [InlineData(true, false, null, $"edge orders@node-a -> orders@runner: orders@node-a seals with key {Key}, orders@runner sends in clear.")]
    public async Task The_Plan_Says_The_Protection_Of_Each_Edge_By_The_Rule_Arming_Applies(
        bool nodeSeals, bool runnerSeals, string? protection, string? refusal)
    {
        await NodeAAndARunnerAsync(nodeSeals ? Keyed : null, runnerSeals ? Keyed : null, admitted: true);
        await BoundOrdersAsync();

        var answer = await PlanAsync();

        Assert.Equal(refusal is null, answer.GetProperty("plan").GetProperty("deployable").GetBoolean());
        Assert.Equal(refusal is null ? "runner" : null, answer.GetProperty("runner").GetString());
        var edge = Assert.Single(answer.GetProperty("edgeProtections").EnumerateArray());
        Assert.Equal(["producerFragment", "consumerFragment", "protection", "refusal"], edge.EnumerateObject().Select(p => p.Name));
        var planned = Assert.Single(answer.GetProperty("plan").GetProperty("edges").EnumerateArray());
        Assert.Equal(planned.GetProperty("producerFragment").GetString(), edge.GetProperty("producerFragment").GetString());
        Assert.Equal(planned.GetProperty("consumerFragment").GetString(), edge.GetProperty("consumerFragment").GetString());
        Assert.Equal(protection, edge.GetProperty("protection").GetString());
        Assert.Equal(refusal, edge.GetProperty("refusal").GetString());
    }

    /// <summary>The runner and node A hold keys that differ: the edge is named, with the fingerprint of the key of each end.</summary>
    [Fact]
    public async Task The_Plan_Names_An_Edge_Whose_Ends_Hold_Different_Keys_By_Their_Fingerprints()
    {
        await NodeAAndARunnerAsync(Keyed, OtherKeyed, admitted: true);
        await BoundOrdersAsync();

        var answer = await PlanAsync();

        var edge = Assert.Single(answer.GetProperty("edgeProtections").EnumerateArray());
        Assert.Equal(JsonValueKind.Null, edge.GetProperty("protection").ValueKind);
        Assert.Equal(
            $"edge orders@node-a -> orders@runner: orders@node-a seals with key {Key}, orders@runner seals with key {OtherKey}.",
            edge.GetProperty("refusal").GetString());
    }

    /// <summary>
    /// An edge of a plan the planner refuses is not judged, whatever its ends do, and a plan with no edge has none to judge. The planner
    /// refuses a plan whose fragments exchange nothing, so the plan with no edge is one that is refused for another reason.
    /// </summary>
    [Fact]
    public async Task The_Plan_Judges_No_Edge_Of_A_Plan_It_Refuses_Nor_Of_One_With_No_Edge()
    {
        await NodeAAndARunnerAsync(Keyed, null, admitted: true);
        await BoundOrdersAsync();
        Assert.Single((await PlanAsync()).GetProperty("edgeProtections").EnumerateArray());
        using var emptied = await _admin.PutAsJsonAsync("/api/rights/matrix", new { matrix = new Dictionary<string, string[]>() }, Web, Ct);
        emptied.EnsureSuccessStatusCode();

        var refused = await PlanAsync();

        Assert.False(refused.GetProperty("plan").GetProperty("deployable").GetBoolean());
        Assert.Single(refused.GetProperty("plan").GetProperty("edges").EnumerateArray());
        Assert.Empty(refused.GetProperty("edgeProtections").EnumerateArray());

        await _host.Services.GetRequiredService<PipelineLibrary>().SaveAsync("alone", "a:\n  input: generate:10\n", "create", Ct);
        var alone = await _admin.GetFromJsonAsync<JsonElement>("/api/library/alone/plan", Web, Ct);

        Assert.Empty(alone.GetProperty("plan").GetProperty("edges").EnumerateArray());
        Assert.Empty(alone.GetProperty("edgeProtections").EnumerateArray());
    }

    private async Task<JsonElement> JudgeAsync(string job)
    {
        using var response = await _admin.PostAsJsonAsync(CoordinatorApi.EdgeProtections, new EdgeProtectionsRequest(job), Web, Ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<JsonElement>(Ct);
    }

    /// <summary>
    /// The edges of a job text no library entry holds are judged by the rule the plan of a library pipeline applies, each reference pinned
    /// at its brick's version now, and nothing is saved, bound or armed: the same job saved afterwards is judged the same way.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    public async Task The_Edges_Of_A_Job_The_Library_Lacks_Are_Judged_As_The_Plan_Of_A_Library_Pipeline_Judges_Them(bool nodeSeals, bool runnerSeals)
    {
        await NodeAAndARunnerAsync(nodeSeals ? Keyed : null, runnerSeals ? Keyed : null, admitted: true);

        var answer = await JudgeAsync(Job);

        var library = _host.Services.GetRequiredService<PipelineLibrary>();
        Assert.Empty(await library.ListAsync(Ct));
        Assert.Null(_host.Services.GetRequiredService<DeploymentManager>().ArmedOf("draft"));
        var edge = Assert.Single(answer.GetProperty("edgeProtections").EnumerateArray());
        Assert.Equal(["producerFragment", "consumerFragment", "protection", "refusal"], edge.EnumerateObject().Select(p => p.Name));
        Assert.Equal("draft@node-a", edge.GetProperty("producerFragment").GetString());
        Assert.Equal("draft@runner", edge.GetProperty("consumerFragment").GetString());
        var joined = nodeSeals == runnerSeals;
        Assert.Equal(joined, answer.GetProperty("deployable").GetBoolean());
        Assert.Equal(joined, answer.GetProperty("refusal").ValueKind == JsonValueKind.Null);
        Assert.Equal(joined, edge.GetProperty("refusal").ValueKind == JsonValueKind.Null);
        if (!joined) Assert.Contains(edge.GetProperty("refusal").GetString()!, answer.GetProperty("refusal").GetString());

        await BoundOrdersAsync();
        var judged = Assert.Single((await PlanAsync()).GetProperty("edgeProtections").EnumerateArray());
        Assert.Equal(judged.GetProperty("protection").GetString(), edge.GetProperty("protection").GetString());
        Assert.Equal(judged.GetProperty("refusal").GetString()?.Replace("orders@", "draft@"), edge.GetProperty("refusal").GetString());
    }

    /// <summary>
    /// A brick of a node with no connected replica, or one its node does not offer, is given no pin: the plan is refused naming it and
    /// no edge is judged, so the answer says neither a level nor a refusal for an edge it cannot know.
    /// </summary>
    [Theory]
    [InlineData("a:\n  input: brick:node-a/customers\nb:\n  from: a\n", true)]
    [InlineData("a:\n  input: brick:node-a/ghost\nb:\n  from: a\n", false)]
    public async Task A_Brick_That_Is_Offline_Or_Not_Offered_Is_Given_No_Pin_And_Its_Edges_Are_Not_Judged(string job, bool nodeAway)
    {
        await NodeAAndARunnerAsync(Keyed, Keyed, admitted: true);
        if (nodeAway) _host.Services.GetRequiredService<ReplicaRegistry>().Disconnected("c-a");

        var answer = await JudgeAsync(job);

        Assert.False(answer.GetProperty("deployable").GetBoolean());
        Assert.Contains("node-a", answer.GetProperty("refusal").GetString());
        Assert.Empty(answer.GetProperty("edgeProtections").EnumerateArray());
    }

    [Fact]
    public async Task The_Plan_Of_A_Pipeline_The_Library_Lacks_Is_Not_Found()
    {
        using var response = await _admin.GetAsync("/api/library/nothing/plan", Ct);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }
}
