using System.Text.Json;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using TransportR.FlowControl;
using TransportR.Services;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// What each way of planning may do: only the Lab view's manual planning cuts a branch and sends a data node a fragment
/// written as a job. The planner is real, over a hub that answers a node's requests and notes each one.
/// </summary>
public sealed class PlanModeTests : IDisposable
{
    /// <summary>Notes every message sent to a node and answers a <c>Request</c> with what the test set.</summary>
    private sealed class RecordingHub : IHubContext<NodeChannelHub>
    {
        public List<string> Asked { get; } = [];
        public Func<string, NodeResponse> Answer { get; set; } = name => new NodeResponse(null, $"no answer to '{name}'");

        public IHubClients Clients => new RecordingClients(this);
        public IGroupManager Groups => throw new NotSupportedException();
    }

    private sealed class RecordingClients(RecordingHub hub) : IHubClients
    {
        public ISingleClientProxy Client(string connectionId) => new RecordingProxy(hub);
        IClientProxy IHubClients<IClientProxy>.Client(string connectionId) => Client(connectionId);

        public IClientProxy All => throw new NotSupportedException();
        public IClientProxy AllExcept(IReadOnlyList<string> excludedConnectionIds) => throw new NotSupportedException();
        public IClientProxy Clients(IReadOnlyList<string> connectionIds) => throw new NotSupportedException();
        public IClientProxy Group(string groupName) => throw new NotSupportedException();
        public IClientProxy GroupExcept(string groupName, IReadOnlyList<string> excludedConnectionIds) => throw new NotSupportedException();
        public IClientProxy Groups(IReadOnlyList<string> groupNames) => throw new NotSupportedException();
        public IClientProxy User(string userId) => throw new NotSupportedException();
        public IClientProxy Users(IReadOnlyList<string> userIds) => throw new NotSupportedException();
    }

    private sealed class RecordingProxy(RecordingHub hub) : ISingleClientProxy
    {
        public Task SendCoreAsync(string method, object?[] args, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<T> InvokeCoreAsync<T>(string method, object?[] args, CancellationToken cancellationToken = default)
        {
            Assert.Equal(NodeChannel.Request, method);
            var name = (string)args[0]!;
            hub.Asked.Add(name);
            return Task.FromResult((T)(object)hub.Answer(name));
        }
    }

    private const string Pair = "a:\n  input: sqlite:${{LAB_DB}}\nb:\n  from: a\n  output: csv:out.csv\n";

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private readonly FlowControlOptions _matrix = new();
    private readonly ReplicaRegistry _registry = new();
    private readonly NodeInventory _inventory;
    private readonly RecordingHub _hub = new();
    private readonly PlanBuilder _planner;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "plan-mode-" + Guid.NewGuid().ToString("N"));

    public PlanModeTests()
    {
        Directory.CreateDirectory(_dir);
        var seed = Path.Combine(_dir, "seed.json");
        File.WriteAllText(seed, """
            { "identities": [
              { "clientId": "node-r", "name": "r", "description": "", "groups": ["run"] },
              { "clientId": "node-d", "name": "d", "description": "", "groups": ["data"] } ], "matrix": {} }
            """);
        var rights = new RightsStore(new CoordinatorServerOptions
        {
            StateDir = Path.Combine(_dir, "state"), Authority = "http://idp", Audience = "test", UiClientId = "ui", UiClientSecret = "secret", RightsSeed = seed,
        });
        _inventory = new NodeInventory(_registry);
        _planner = new PlanBuilder(
            _inventory, new NodeRequests(_inventory, _hub), new BrickCatalog(_inventory),
            new PlanRegistry(new DefaultFlowControlService(_matrix)), rights);
        _matrix.Groups["data"] = new GroupAccess { CanSendTo = ["run"] };

        Announce("r", "run", NodeRole.Runner, [], []);
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private void Announce(string name, string group, NodeRole role, DatasetInfo[] datasets, string[] capabilities)
    {
        var announcement = new NodeAnnouncement(name, "", datasets, role, [], Capabilities: capabilities, InstanceId: Guid.NewGuid());
        _registry.Announce("connection-" + name, "node-" + name, announcement, [group], () => { });
        _inventory.Announced(name, "node-" + name, announcement, [group]);
    }

    /// <summary>A data node that hosts the database <c>LAB_DB</c>, with no brick: nothing it runs is a brick.</summary>
    private void DataNode(string name, bool acceptsRaw) =>
        Announce(name, "data", NodeRole.Data, [new DatasetInfo("LAB_DB", "")], acceptsRaw ? [NodeAnnouncement.RawFragments] : []);

    private Task<DistributionPlan> PlanAsync(bool manual, IReadOnlyList<CutRequest>? cuts, params (string Unit, string Node)[] placement) =>
        _planner.BuildAsync(
            new PlanRequest("p", Pair, cuts, placement.ToDictionary(p => p.Unit, p => p.Node)), manual ? PlanMode.Manual : PlanMode.Library, TestContext.Current.CancellationToken);

    [Fact]
    public async Task A_Fragment_That_Is_No_Brick_Goes_As_A_Job_When_The_Plan_Is_Manual_And_The_Node_Accepts_It()
    {
        DataNode("d", acceptsRaw: true);

        var plan = await PlanAsync(manual: true, null, ("a", "d"), ("b", "r"));

        Assert.Empty(plan.Errors);
        Assert.Equal(FragmentForm.Job, plan.Fragments.Single(f => f.Node == "d").Form);
        Assert.True(plan.Deployable);
    }

    [Fact]
    public async Task A_Plan_That_Is_Not_Manual_Refuses_A_Fragment_That_Is_No_Brick_Even_On_A_Node_That_Accepts_It()
    {
        DataNode("d", acceptsRaw: true);

        var plan = await PlanAsync(manual: false, null, ("a", "d"), ("b", "r"));

        Assert.Contains(plan.Errors, e => e.StartsWith("p@d:", StringComparison.Ordinal));
        Assert.DoesNotContain(plan.Fragments, f => f.Node == "d");
        Assert.False(plan.Deployable);
    }

    [Fact]
    public async Task A_Manual_Plan_Still_Needs_The_Node_To_Accept_A_Fragment_That_Is_No_Brick()
    {
        DataNode("d", acceptsRaw: false);

        var plan = await PlanAsync(manual: true, null, ("a", "d"), ("b", "r"));

        Assert.Contains(plan.Errors, e => e.StartsWith("p@d:", StringComparison.Ordinal));
        Assert.False(plan.Deployable);
    }

    [Fact]
    public async Task A_Manual_Plan_Cuts_A_Branch_On_The_Node_That_Hosts_Its_Data()
    {
        DataNode("d", acceptsRaw: true);
        var split = new BranchSplit("a:\n  input: sqlite:${{LAB_DB}}\n  output: arrow:-\n", "a:\n  input: arrow:-\n", null);
        _hub.Answer = _ => new NodeResponse(JsonSerializer.Serialize(split, Web), null);

        var plan = await PlanAsync(manual: true, [new CutRequest("a", 1)], ("a^", "d"), ("a", "r"), ("b", "r"));

        Assert.Equal(["lab.split"], _hub.Asked);
        Assert.Empty(plan.Errors);
        Assert.Contains(plan.Units, u => u.Id == "a^" && u.IsHead);
        Assert.True(plan.Deployable);
    }

    [Fact]
    public async Task A_Plan_That_Is_Not_Manual_Is_Never_Cut_And_No_Node_Is_Asked()
    {
        DataNode("d", acceptsRaw: true);
        var split = new BranchSplit("a:\n  input: sqlite:${{LAB_DB}}\n  output: arrow:-\n", "a:\n  input: arrow:-\n", null);
        _hub.Answer = _ => new NodeResponse(JsonSerializer.Serialize(split, Web), null);

        var plan = await PlanAsync(manual: false, [new CutRequest("a", 1)], ("a^", "d"), ("a", "r"), ("b", "r"));

        Assert.Empty(_hub.Asked);
        Assert.Equal(["Cuts belong to the Lab view's planning by hand: this plan is not one, so it takes none."], plan.Errors);
        Assert.False(plan.Deployable);
        Assert.DoesNotContain(plan.Units, u => u.IsHead);
    }
}
