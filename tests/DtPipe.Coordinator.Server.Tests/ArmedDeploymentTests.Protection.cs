using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Deployments;
using DtPipe.Jobs.Coordinator;
using Xunit;
using DtPipe.Tests.Shared;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// The protection of the data plane at arming and at the registration of a fixed plan: an edge is refused, by name and with what each
/// end does, when its two nodes each have a connected replica and no pair of them joins. A node with none is left to admission, and
/// replicas that differ within a node pass while a pair of them joins.
/// </summary>
public sealed partial class ArmedDeploymentTests
{
    private const string Key = "3f9a0c11d2e4b7a8";

    [Fact]
    public async Task An_Edge_No_Pair_Of_Connected_Replicas_Joins_Is_Refused_By_Name_At_Arming_And_At_Registration()
    {
        var clear = await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var fixedPlan = (await _deployments.PlanLaunchAsync("p", Job, Armed().Bindings, CancellationToken.None)).Plan;
        Assert.True(fixedPlan.Deployable);

        await NodeAsync("n", "crm", NodeRole.Data, protection: DataPlaneProtections.EncryptedPsk, keyId: Key);
        _registry.Disconnected(clear);

        const string refusal = $"edge p@n -> p@r1: p@n seals with key {Key}, p@r1 sends in clear.";
        var arming = await Assert.ThrowsAsync<ConflictException>(() => _deployments.ArmAsync(Armed(), Job, CancellationToken.None));
        Assert.Contains($"Runner r1: {refusal}", arming.Message);
        var registration = await Assert.ThrowsAsync<ConflictException>(() => _deployments.DeployAsync(fixedPlan, "lab", CancellationToken.None));
        Assert.Equal(refusal, registration.Message);
        Assert.Null(_deployments.ArmedOf("p"));
        Assert.Empty(await _deployments.DeploymentsAsync());
    }

    /// <summary>
    /// The only replica <c>n</c> has left seals and is in its grace: no replica of it is connected, so the edge is not judged and the
    /// refusal is the node's absence.
    /// </summary>
    [Fact]
    public async Task A_Node_With_No_Connected_Replica_Is_Not_Judged_At_Registration()
    {
        var clear = await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);
        var fixedPlan = (await _deployments.PlanLaunchAsync("p", Job, Armed().Bindings, CancellationToken.None)).Plan;
        _registry.Leave(clear);
        _registry.Disconnected(await NodeAsync("n", "crm", NodeRole.Data, protection: DataPlaneProtections.EncryptedPsk, keyId: Key));

        var ex = await Assert.ThrowsAsync<ConflictException>(() => _deployments.DeployAsync(fixedPlan, "lab", CancellationToken.None));

        Assert.Equal("Offline: n.", ex.Message);
    }

    /// <summary>A rolling update leaves <c>n</c> with a replica that seals and one that sends in clear: the second joins the runner, so the arming holds.</summary>
    [Fact]
    public async Task Replicas_That_Differ_Within_A_Node_But_Offer_A_Pair_That_Joins_Do_Not_Block_Arming()
    {
        await NodeAsync("n", "crm", NodeRole.Data, protection: DataPlaneProtections.EncryptedPsk, keyId: Key);
        await NodeAsync("n", "crm", NodeRole.Data);
        await NodeAsync("r1", "crm", NodeRole.Runner);

        await _deployments.ArmAsync(Armed(), Job, CancellationToken.None);

        Assert.NotNull(_deployments.ArmedOf("p"));
    }

    /// <summary>
    /// The edge <c>n -> r1</c> where <c>n</c> sends in clear and the runner seals with a key. Judged as it is, no pair joins and the cause is the
    /// edge's. Under a required protection <c>n</c>'s only replica is eligible for nothing, so it is no end of a pair: the edge is not judged,
    /// and <c>n</c> is a node with no eligible replica, by the cause that already names it.
    /// </summary>
    [Fact]
    public async Task A_Node_Whose_Connected_Replicas_Are_All_Under_The_Required_Protection_Is_Not_An_End_Of_An_Edge()
    {
        async Task<Launch> LaunchAsync()
        {
            await NodeAsync("n", "crm", NodeRole.Data);
            await NodeAsync("r1", "crm", NodeRole.Runner, protection: DataPlaneProtections.EncryptedPsk, keyId: Key);
            return await _deployments.PlanLaunchAsync("p", Job, Armed().Bindings, CancellationToken.None);
        }

        var judged = await LaunchAsync();
        Assert.Contains(judged.Causes, c => c.Code == LaunchCauses.ProtectionMismatch);

        _required = DataPlaneProtections.EncryptedPsk;
        Restart();
        var required = await LaunchAsync();

        Assert.DoesNotContain(required.Causes, c => c.Code == LaunchCauses.ProtectionMismatch);
        var cause = Assert.Single(required.Causes, c => c.Code == LaunchCauses.NoEligibleReplica);
        Assert.Equal(("n", "p@n"), (cause.Node, cause.Fragment));
        Assert.Contains("it sends in clear and this deployment requires encrypted-psk.", cause.Message);
        Assert.True(Assert.Single(_registry.Replicas("node-n")).Connected);
    }

    private const string ChainJob = "src:\n  input: brick:n/src\nmid:\n  from: src\ndst:\n  from: mid\n  output: brick:m/dst\n";

    private static BrickInfo SinkBrick() => TestBricks.Announced("dst", BrickKind.Sink, """{"output":"sqlite:b"}""", "sink");

    /// <summary>
    /// The chain n -> r1 -> m where the runner seals, m sends in clear and n does either: a run of it would fail before its first batch,
    /// so arming refuses it, naming each edge no pair joins, before any replica is asked anything, any run is sent or any lease is taken.
    /// </summary>
    [Theory]
    [InlineData(false, "edge p@n -> p@r1: p@n sends in clear, p@r1 seals without a key.", "edge p@r1 -> p@m: p@r1 seals without a key, p@m sends in clear.")]
    [InlineData(true, "edge p@r1 -> p@m: p@r1 seals without a key, p@m sends in clear.")]
    public async Task A_Chain_Whose_Ends_Protect_The_Data_Plane_Differently_Is_Refused_At_Arming_Before_Anything_Is_Launched(
        bool sourceSeals, params string[] refused)
    {
        await NodeAsync("n", "crm", NodeRole.Data, protection: sourceSeals ? DataPlaneProtections.Encrypted : null);
        await NodeAsync("r1", "crm", NodeRole.Runner, protection: DataPlaneProtections.Encrypted);
        await NodeAsync("m", "crm", NodeRole.Data, SinkBrick());
        var bindings = new Dictionary<string, Binding>
        {
            ["src"] = new Binding("n", "src", "strict", new BindingPin(Brick().Version)),
            ["dst"] = new Binding("m", "dst", "strict", new BindingPin(SinkBrick().Version)),
        };
        var armed = Armed(job: ChainJob) with { Bindings = bindings };

        var ex = await Assert.ThrowsAsync<ConflictException>(() => _deployments.ArmAsync(armed, ChainJob, CancellationToken.None));

        foreach (var edge in refused) Assert.Contains(edge, ex.Message);
        Assert.Equal(refused.Length, ex.Message.Split("edge p@").Length - 1);
        Assert.Null(_deployments.ArmedOf("p"));
        Assert.False(_link.Asked.Task.IsCompleted);
        Assert.Empty(_runs.Specs);
        Assert.All(_registry.Replicas(), r => Assert.Empty(r.Leases));
    }
}
