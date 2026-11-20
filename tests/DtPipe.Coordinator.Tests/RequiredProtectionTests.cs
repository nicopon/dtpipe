using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Hub;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// A deployment may require a protection of the data plane. A replica announcing a level below it stays connected and is eligible for no
/// fragment, a runner's included, saying what it does and what is required; the levels are ordered, so the required one is a floor; and
/// the weakest level, which is the default, requires nothing.
/// </summary>
public class RequiredProtectionTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);
    private const string Key = "3f9a0c11d2e4b7a8";
    private const string Clear = DataPlaneProtections.None;
    private const string Sealed = DataPlaneProtections.Encrypted;
    private const string Keyed = DataPlaneProtections.EncryptedPsk;

    private readonly ReplicaRegistry _registry = new();
    private readonly InMemoryStateStore _state = new();

    private static RunFragment Fragment(string name, string node) =>
        new(new FragmentSpec(name, "v1", [], Yaml: "x:\n  input: generate:1\n"), node, ["test"]);

    private ReplicaAdmission Admission(string? required) =>
        new(_registry, _state, required is null ? null : new AdmissionOptions { RequiredProtection = required });

    /// <summary>A replica of <paramref name="node"/> that announces <paramref name="level"/> and is eligible for any fragment otherwise.</summary>
    private async Task<ReplicaKey> ReplicaAsync(string node, int instance, string level)
    {
        var client = Guid.NewGuid();
        await _state.SaveClientAsync(new ClientRegistration(client, "data-" + node, ["test"], null, null, DateTime.UtcNow, Subject: node));
        var announcement = new NodeAnnouncement(
            node, "", [], NodeRole.Data, [], Capabilities: [NodeAnnouncement.RawFragments],
            InstanceId: Guid.Parse($"00000000-0000-0000-0000-{instance:000000000000}"), DataPlaneClientId: client,
            DataPlaneProtection: level, KeyId: level == Keyed ? Key : null);
        return _registry.Announce($"c{instance}", node, announcement, ["test"], () => { });
    }

    [Fact]
    public async Task AReplicaBelowTheRequiredProtection_IsEligibleForNothing_AndSaysWhatItDoesAndWhatIsRequired()
    {
        var clear = await ReplicaAsync("n", 1, Clear);
        var keyed = await ReplicaAsync("n", 2, Keyed);
        var admission = Admission(Keyed);

        var eligibility = (await admission.EvaluateAsync([Fragment("f", "n")]))["f"];
        var (chosen, _) = await admission.TryReserveAsync("run-1", [Fragment("f", "n")]);

        Assert.Equal(new[] { keyed }, eligibility.Eligible);
        Assert.Equal(keyed, chosen!["f"].Key);

        _registry.Leave("c2");
        const string says = "it sends in clear and this deployment requires encrypted-psk.";
        var alone = (await admission.EvaluateAsync([Fragment("f", "n")]))["f"];
        var (none, reasons) = await admission.TryReserveAsync("run-2", [Fragment("f", "n")]);

        Assert.Empty(alone.Eligible);
        Assert.Equal($"replica {clear.Short}: {says}", alone.Reason);
        Assert.Null(none);
        Assert.Equal($"replica {clear.Short}: {says}", reasons["f"]);
        Assert.True(Assert.Single(_registry.Replicas()).Connected);
    }

    /// <summary>The levels are ordered, so the required one is a floor; the weakest, the default, leaves every level eligible.</summary>
    [Theory]
    [InlineData(Clear, new[] { Clear, Sealed, Keyed })]
    [InlineData(Sealed, new[] { Sealed, Keyed })]
    [InlineData(Keyed, new[] { Keyed })]
    [InlineData(null, new[] { Clear, Sealed, Keyed })]
    public async Task TheRequiredProtection_IsAFloor_AndTheDefaultRequiresNothing(string? required, string[] eligibleLevels)
    {
        var keys = new Dictionary<string, ReplicaKey>
        {
            [Clear] = await ReplicaAsync("n", 1, Clear),
            [Sealed] = await ReplicaAsync("n", 2, Sealed),
            [Keyed] = await ReplicaAsync("n", 3, Keyed),
        };

        var eligibility = (await Admission(required).EvaluateAsync([Fragment("f", "n")]))["f"];

        Assert.Equal(eligibleLevels.Select(level => keys[level]).ToHashSet(), eligibility.Eligible.ToHashSet());
    }

    [Fact]
    public void ARequiredProtection_IsOneOfTheWords_NeverAnyOtherText()
    {
        Assert.Equal(Sealed, new AdmissionOptions { RequiredProtection = "ENCRYPTED" }.RequiredProtection);
        Assert.Equal(Clear, new AdmissionOptions().RequiredProtection);
        Assert.Throws<ArgumentException>(() => new AdmissionOptions { RequiredProtection = "aes" });
    }

    private static Task<CoordinatorTestHost> StartAsync(TestJwt jwt, string required) =>
        CoordinatorTestHost.StartAsync(_ => { }, services =>
        {
            services.AddSingleton(new AdmissionOptions { RequiredProtection = required });
            services.AddSingleton(new RunOrchestratorOptions
            {
                AdmissionTimeout = TimeSpan.FromSeconds(1),
                ReadyTimeout = TimeSpan.FromSeconds(2),
                ExitTimeout = TimeSpan.FromSeconds(1),
            });
        }, jwt);

    private static Task<FakeNode> RunnerAsync(CoordinatorTestHost host, TestJwt jwt, string level) =>
        FakeNode.StartAsync(host, "runner-1", new FakeNodeOptions
        {
            Token = jwt.Mint("runner-1", "test", TimeSpan.FromHours(1)), DataPlaneProtection = level, InstanceId = Guid.NewGuid(),
        });

    /// <summary>A runner is judged as any node: the run is refused while its only replica is below the requirement, and placed on one that meets it.</summary>
    [Fact]
    public async Task ARunnerIsJudgedAsAnyNode_AReplicaBelowTheRequirementIsNeverChosen()
    {
        var jwt = new TestJwt();
        await using var host = await StartAsync(jwt, Sealed);
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        await using var below = await RunnerAsync(host, jwt, Clear);

        var refused = await Assert.ThrowsAsync<NoEligibleReplicaException>(
            () => orchestrator.RunAsync(new RunSpec("run-1", [Fragment("f", "runner-1")], [])).WaitAsync(Wait));

        Assert.Contains("it sends in clear and this deployment requires encrypted.", refused.Reasons["f"]);
        Assert.Empty(below.Launches);
        Assert.True(Assert.Single(host.Host.Services.GetRequiredService<ReplicaRegistry>().Replicas()).Connected);

        await using var meeting = await RunnerAsync(host, jwt, Keyed);
        var run = orchestrator.RunAsync(new RunSpec("run-2", [Fragment("f", "runner-1")], []));
        await meeting.Launched.Task.WaitAsync(Wait);
        await meeting.ExitAsync(0);

        Assert.Equal(RunOutcome.Succeeded, (await run.WaitAsync(Wait)).Outcome);
        Assert.Empty(below.Launches);
    }
}
