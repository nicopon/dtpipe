using System.Diagnostics;
using System.Reflection;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// What each phase of a run does that no other test pins, against the real hub with <see cref="FakeNode"/> replicas: who is
/// told to <c>Cancel</c> and who is not, which fragment a refusal or a silence names, what the teardown waits for, and
/// the table of runs. Every run is edgeless, so the wiring phase has nothing to open.
/// </summary>
public class RunPhasesTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private static Task<CoordinatorTestHost> StartHostAsync(TimeSpan? readyTimeout = null) =>
        CoordinatorTestHost.StartAsync(_ => { }, services => services.AddSingleton(new RunOrchestratorOptions
        {
            AdmissionTimeout = TimeSpan.FromSeconds(5),
            ReadyTimeout = readyTimeout ?? TimeSpan.FromSeconds(5),
            ExitTimeout = TimeSpan.FromSeconds(8),
        }));

    private static RunSpec Spec(string runId, params string[] fragments) =>
        new(runId, fragments.Select(f => TestFragments.Edgeless(f)).ToList(), []);

    private static Func<LaunchRequest, Task<LaunchResult>> Refuses(string reason) =>
        _ => Task.FromResult(new LaunchResult(false, reason));

    /// <summary>A launch answered long after the <c>ReadyTimeout</c> of <see cref="StartHostAsync"/> (one second): the fragment has gone quiet by then.</summary>
    private static Task<LaunchResult> AnsweredTooLate(LaunchRequest _) =>
        Task.Delay(TimeSpan.FromSeconds(4)).ContinueWith(_ => new LaunchResult(true));

    private static IRunOrchestrator OrchestratorOf(CoordinatorTestHost host) => host.Host.Services.GetRequiredService<IRunOrchestrator>();

    /// <summary>The fragment that did not answer its launch in time is the cause, gone quiet, and is the one fragment never told to cancel.</summary>
    [Fact]
    public async Task AFragmentThatDoesNotAnswerItsLaunchInTime_IsTheCauseAsUnresponsive_AndIsNeverToldToCancel()
    {
        await using var host = await StartHostAsync(readyTimeout: TimeSpan.FromSeconds(1));
        await using var a = await FakeNode.StartAsync(host, "a");
        await using var b = await FakeNode.StartAsync(host, "b");
        b.OnLaunch = AnsweredTooLate;

        var result = await OrchestratorOf(host).RunAsync(Spec("run-1", "a", "b")).WaitAsync(Wait);

        Assert.Equal(RunOutcome.Failed, result.Outcome);
        Assert.Equal("b", result.Cause);
        Assert.True(result.CauseIsUnresponsive);
        Assert.True(a.Cancelled.Task.IsCompleted);
        Assert.False(b.Cancelled.Task.IsCompleted);
    }

    /// <summary>A replica that refused the launch ran nothing: it is not told to cancel, while its healthy peer is.</summary>
    [Fact]
    public async Task AFragmentWhoseLaunchWasRefused_IsNotToldToCancel_ItsHealthyPeerIs()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        await using var b = await FakeNode.StartAsync(host, "b");
        b.OnLaunch = Refuses("not this one");

        var result = await OrchestratorOf(host).RunAsync(Spec("run-1", "a", "b")).WaitAsync(Wait);

        Assert.Contains("b could not start", result.Cause);
        Assert.True(a.Cancelled.Task.IsCompleted);
        Assert.False(b.Cancelled.Task.IsCompleted);
    }

    /// <summary>Several refusals: the cause names the first fragment of the run's own order, never the last.</summary>
    [Fact]
    public async Task OfSeveralRefusedLaunches_TheCauseNamesTheFirstFragmentOfTheRun()
    {
        await using var host = await StartHostAsync();
        await using var x = await FakeNode.StartAsync(host, "x");
        await using var y = await FakeNode.StartAsync(host, "y");
        x.OnLaunch = Refuses("x says no");
        y.OnLaunch = Refuses("y says no");

        var result = await OrchestratorOf(host).RunAsync(Spec("run-1", "x", "y")).WaitAsync(Wait);

        Assert.StartsWith("x could not start on replica", result.Cause);
        Assert.EndsWith("x says no", result.Cause);
    }

    /// <summary>A replica that answers a <c>Cancel</c> with "I know no such run" has nothing to report: the teardown does not wait for it.</summary>
    [Fact]
    public async Task AReplicaThatKnowsNoSuchRun_IsNotWaitedForAfterACancel()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        await using var b = await FakeNode.StartAsync(host, "b", new FakeNodeOptions { KnowsTheRun = false, ExitOnCancel = false });
        using var cancel = new CancellationTokenSource();
        var run = OrchestratorOf(host).RunAsync(Spec("run-1", "a", "b"), cancel.Token);
        await Task.WhenAll(a.Launched.Task, b.Launched.Task).WaitAsync(Wait);

        var clock = Stopwatch.StartNew();
        cancel.Cancel();
        var result = await run.WaitAsync(Wait);

        Assert.Equal(RunOutcome.Cancelled, result.Outcome);
        Assert.True(b.Cancelled.Task.IsCompleted);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(4), $"the teardown waited {clock.Elapsed} for a replica that knew nothing of the run");
    }

    /// <summary>A cancelled fragment that reports a moment after the cancel is waited for, and its report is the run's.</summary>
    [Fact]
    public async Task ACancelledFragmentsLateReport_IsWaitedFor_AndKept()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a", new FakeNodeOptions { ExitOnCancelAfter = TimeSpan.FromMilliseconds(300) });
        using var cancel = new CancellationTokenSource();
        var run = OrchestratorOf(host).RunAsync(Spec("run-1", "a"), cancel.Token);
        await a.Launched.Task.WaitAsync(Wait);

        cancel.Cancel();
        var result = await run.WaitAsync(Wait);

        Assert.Equal(RunOutcome.Cancelled, result.Outcome);
        Assert.Contains("a", result.Reports.Keys);
    }

    /// <summary>A replica that leaves after its fragment reported ends nothing: the run goes on, and its other fragment is not cancelled.</summary>
    [Fact]
    public async Task AReplicaLeavingAfterItsFragmentReported_DoesNotEndTheRun()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        await using var b = await FakeNode.StartAsync(host, "b");
        var run = OrchestratorOf(host).RunAsync(Spec("run-1", "a", "b"));
        await Task.WhenAll(a.Launched.Task, b.Launched.Task).WaitAsync(Wait);

        await a.ExitAsync(0);
        await a.LeaveAsync();
        await Task.Delay(300);

        Assert.False(run.IsCompleted);
        Assert.False(b.Cancelled.Task.IsCompleted);
        await b.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await run.WaitAsync(Wait)).Outcome);
    }

    /// <summary>A run id is free again once its run ended, and refused while the run is in flight.</summary>
    [Fact]
    public async Task ARunIdIsRefusedWhileItsRunIsInFlight_AndFreeAgainOnceItEnded()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        var orchestrator = OrchestratorOf(host);

        var first = orchestrator.RunAsync(Spec("run-1", "a"));
        await a.Launched.Task.WaitAsync(Wait);
        await Assert.ThrowsAsync<ArgumentException>(() => orchestrator.RunAsync(Spec("run-1", "a")));
        await a.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await first.WaitAsync(Wait)).Outcome);

        var again = orchestrator.RunAsync(Spec("run-1", "a"));
        var deadline = DateTime.UtcNow + Wait;
        while (a.Launches.Count < 2)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("the second run was never launched");
            await Task.Delay(10);
        }
        await a.ExitAsync(0, launchToken: a.Launches.Last().LaunchToken);
        Assert.Equal(RunOutcome.Succeeded, (await again.WaitAsync(Wait)).Outcome);
    }

    /// <summary>A run listens to lost replicas only while it is placed: nothing stays subscribed once it ended.</summary>
    [Fact]
    public async Task ARunUnsubscribesFromLostReplicas_WhenItEnds()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        var registry = host.Host.Services.GetRequiredService<ReplicaRegistry>();
        int Subscribers() =>
            typeof(ReplicaRegistry).GetField(nameof(ReplicaRegistry.ReplicaLost), BindingFlags.Instance | BindingFlags.NonPublic)?.GetValue(registry) is Delegate d
                ? d.GetInvocationList().Length : 0;
        var before = Subscribers();

        var run = OrchestratorOf(host).RunAsync(Spec("run-1", "a"));
        await a.Launched.Task.WaitAsync(Wait);
        Assert.Equal(before + 1, Subscribers());
        await a.ExitAsync(0);
        await run.WaitAsync(Wait);

        Assert.Equal(before, Subscribers());
    }

    /// <summary>Each launch of each fragment carries a token of its own.</summary>
    [Fact]
    public async Task EachLaunchOfEachFragment_CarriesATokenOfItsOwn()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeNode.StartAsync(host, "a");
        await using var b = await FakeNode.StartAsync(host, "b");
        var orchestrator = OrchestratorOf(host);

        var first = orchestrator.RunAsync(Spec("run-1", "a", "b"));
        var second = orchestrator.RunAsync(Spec("run-2", "a", "b"));
        await Task.WhenAll(a.Launched.Task, b.Launched.Task).WaitAsync(Wait);
        var deadline = DateTime.UtcNow + Wait;
        while (a.Launches.Count < 2 || b.Launches.Count < 2)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("the runs were never launched");
            await Task.Delay(10);
        }

        var tokens = a.Launches.Concat(b.Launches).Select(l => l.LaunchToken).ToList();
        Assert.Equal(4, tokens.Distinct().Count());
        Assert.All(tokens, token => Assert.False(string.IsNullOrEmpty(token)));

        foreach (var node in new[] { a, b })
            foreach (var launch in node.Launches)
                await node.ExitAsync(0, runId: launch.RunId, fragment: launch.Spec.Fragment, launchToken: launch.LaunchToken);
        Assert.Equal(RunOutcome.Succeeded, (await first.WaitAsync(Wait)).Outcome);
        Assert.Equal(RunOutcome.Succeeded, (await second.WaitAsync(Wait)).Outcome);
    }
}
