using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// A replica that does not announce <see cref="NodeAnnouncement.LaunchToken"/> returns no token in its report, which could not
/// be told from a stale one: it is never placed on a run. A run is refused for it only when no replica of the node is left.
/// </summary>
public class LaunchTokenEligibilityTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private static Task<CoordinatorTestHost> StartAsync() =>
        CoordinatorTestHost.StartAsync(_ => { }, services => services.AddSingleton(new RunOrchestratorOptions
        {
            AdmissionTimeout = TimeSpan.FromSeconds(1),
            ReadyTimeout = TimeSpan.FromSeconds(2),
            ExitTimeout = TimeSpan.FromSeconds(1),
        }));

    private static RunSpec Run(string runId) => new(runId, [TestFragments.Edgeless("frag-a", "node-a")], []);

    [Fact]
    public async Task AReplicaThatAnnouncesNoLaunchToken_IsNotEligible_AndTheRefusalSaysWhy()
    {
        await using var host = await StartAsync();
        await using var old = await FakeNode.StartAsync(host, "node-a", new FakeNodeOptions { AnnounceLaunchToken = false });
        old.OnLaunch = _ => Task.FromResult(new LaunchResult(false, "placed on a replica that could not report"));
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var refused = await Assert.ThrowsAsync<NoEligibleReplicaException>(() => orchestrator.RunAsync(Run("run-1")).WaitAsync(Wait));

        Assert.Contains("launch", refused.Reasons["frag-a"]);
        Assert.Contains("update", refused.Reasons["frag-a"]);
        Assert.Empty(old.Launches);
    }

    /// <summary>A rolling update: the replica of the older version is skipped while another of the node can run the fragment.</summary>
    [Fact]
    public async Task AReplicaThatAnnouncesIt_RunsTheFragment_BesideOneThatDoesNot()
    {
        await using var host = await StartAsync();
        // The tie between two idle replicas goes to the lowest instance id: the older replica would be the one placed.
        await using var old = await FakeNode.StartAsync(host, "node-a", new FakeNodeOptions
        {
            AnnounceLaunchToken = false, InstanceId = Guid.Parse("00000000-0000-0000-0000-000000000001"),
        });
        await using var current = await FakeNode.StartAsync(host, "node-a", new FakeNodeOptions
        {
            InstanceId = Guid.Parse("00000000-0000-0000-0000-000000000002"),
        });
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        old.OnLaunch = _ => Task.FromResult(new LaunchResult(false, "placed on a replica that could not report"));

        var run = orchestrator.RunAsync(Run("run-1"));
        await Task.WhenAny(current.Launched.Task, run).WaitAsync(Wait);
        Assert.Empty(old.Launches);

        Assert.Equal(ExitedAck.Accepted, await current.ExitAsync(0));
        Assert.Equal(RunOutcome.Succeeded, (await run.WaitAsync(Wait)).Outcome);
    }
}
