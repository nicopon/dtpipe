using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// Several runs in flight on one <see cref="RunOrchestrator"/>, against the real hub, each fragment run by a
/// <see cref="FakeNode"/>: it answers <c>Launch</c> as started, reports <c>Exited</c> when the test says so, and
/// when told to <c>Cancel</c>. Every run is edgeless, so no transfer opens. Each test waits until both runs
/// have launched before it ends one, so the two are in flight together.
/// </summary>
public class ConcurrentRunsTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private static Task<CoordinatorTestHost> StartHostAsync() =>
        CoordinatorTestHost.StartAsync(_ => { }, services => services.AddSingleton(new RunOrchestratorOptions
        {
            AdmissionTimeout = TimeSpan.FromSeconds(5),
            ReadyTimeout = TimeSpan.FromSeconds(5),
            ExitTimeout = TimeSpan.FromSeconds(2),
        }));

    private static RunSpec Spec(string runId, params string[] fragments) =>
        new(runId, fragments.Select(f => TestFragments.Edgeless(f)).ToList(), []);

    /// <summary>Lets anything the first verdict could wrongly set off in the other run arrive before the test looks.</summary>
    private static Task Settle() => Task.Delay(300);

    [Fact]
    public async Task TwoDisjointRuns_AreInFlightTogether_EachWithItsOwnVerdict()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        await using var b = await FakeNode.StartAsync(host, "b");
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var first = orchestrator.RunAsync(Spec("run-1", "a"));
        var second = orchestrator.RunAsync(Spec("run-2", "b"));
        await Task.WhenAll(a.Launched.Task, b.Launched.Task).WaitAsync(Wait);

        await b.ExitAsync(1);
        var failed = await second.WaitAsync(Wait);
        await Settle();

        Assert.Equal(RunOutcome.Failed, failed.Outcome);
        Assert.Equal("b", failed.Cause);
        Assert.False(first.IsCompleted);
        Assert.False(a.Cancelled.Task.IsCompleted);

        await a.ExitAsync(0);
        var succeeded = await first.WaitAsync(Wait);
        Assert.Equal(RunOutcome.Succeeded, succeeded.Outcome);
    }

    [Fact]
    public async Task CancellingOneRun_LeavesTheOtherInFlight()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        await using var b = await FakeNode.StartAsync(host, "b");
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        using var cancel = new CancellationTokenSource();
        var first = orchestrator.RunAsync(Spec("run-1", "a"), cancel.Token);
        var second = orchestrator.RunAsync(Spec("run-2", "b"));
        await Task.WhenAll(a.Launched.Task, b.Launched.Task).WaitAsync(Wait);

        cancel.Cancel();
        var cancelled = await first.WaitAsync(Wait);
        await Settle();

        Assert.Equal(RunOutcome.Cancelled, cancelled.Outcome);
        Assert.True(a.Cancelled.Task.IsCompleted);
        Assert.False(second.IsCompleted);
        Assert.False(b.Cancelled.Task.IsCompleted);

        await b.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await second.WaitAsync(Wait)).Outcome);
    }

    [Fact]
    public async Task ALostFragment_EndsItsOwnRunOnly()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        await using var b = await FakeNode.StartAsync(host, "b");
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var first = orchestrator.RunAsync(Spec("run-1", "a"));
        var second = orchestrator.RunAsync(Spec("run-2", "b"));
        await Task.WhenAll(a.Launched.Task, b.Launched.Task).WaitAsync(Wait);

        await a.LeaveAsync();
        var lost = await first.WaitAsync(Wait);
        await Settle();

        Assert.Equal(RunOutcome.Failed, lost.Outcome);
        Assert.Equal("a", lost.Cause);
        Assert.True(lost.CauseIsUnresponsive);
        Assert.False(second.IsCompleted);
        Assert.False(b.Cancelled.Task.IsCompleted);

        await b.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await second.WaitAsync(Wait)).Outcome);
    }

    /// <summary>A replica serves several runs: two runs of one fragment on one node are in flight together, each reported by its own run id.</summary>
    [Fact]
    public async Task TwoRunsOfOneFragment_AreInFlightTogether_EachReportedByItsRunId()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        var registry = host.Host.Services.GetRequiredService<ReplicaRegistry>();

        var first = orchestrator.RunAsync(Spec("run-1", "a"));
        var second = orchestrator.RunAsync(Spec("run-2", "a"));
        var deadline = DateTime.UtcNow + Wait;
        while (a.Launches.Count < 2)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("both runs were never launched");
            await Task.Delay(10);
        }
        Assert.Equal(2, Assert.Single(registry.Replicas("a")).Leases.Count);

        await a.ExitAsync(1, runId: "run-2");
        var failed = await second.WaitAsync(Wait);
        await Settle();
        Assert.Equal(RunOutcome.Failed, failed.Outcome);
        Assert.False(first.IsCompleted);

        await a.ExitAsync(0, runId: "run-1");
        Assert.Equal(RunOutcome.Succeeded, (await first.WaitAsync(Wait)).Outcome);
        Assert.Empty(registry.Replicas("a").Single().Leases);
    }

    [Fact]
    public async Task AReportForARunNotInFlight_IsIgnored()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var run = orchestrator.RunAsync(Spec("run-1", "a"));
        await a.Launched.Task.WaitAsync(Wait);

        // The token is the one of a real launch of this node, so that only the rule on the run can refuse it.
        var realToken = (await a.Launched.Task).LaunchToken;
        Assert.Equal(ExitedAck.NotAdmitted, await a.ExitAsync(1, runId: "run-0", fragment: a.FragmentName, launchToken: realToken));
        await Settle();
        Assert.False(run.IsCompleted);

        await a.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await run.WaitAsync(Wait)).Outcome);
    }
}
