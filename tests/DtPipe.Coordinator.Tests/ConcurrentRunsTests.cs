using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using TransportR.Client.SignalR.Services;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// Several runs in flight on one <see cref="RunOrchestrator"/>, against the real hub, each fragment a
/// fake node: it answers <c>Launch</c> with <c>Ready</c>, reports <c>Exited</c> when the test says so,
/// and when told to <c>Cancel</c>. Every run is edgeless, so no transfer opens. Each test waits until
/// both runs have launched before it ends one, so the two are in flight together.
/// </summary>
public class ConcurrentRunsTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private sealed class FakeFragment : IAsyncDisposable
    {
        private readonly SignalRDataClient<string> _client;

        private FakeFragment(SignalRDataClient<string> client, string name)
        {
            _client = client;
            Name = name;
        }

        public string Name { get; }
        public TaskCompletionSource<string> Launched { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Cancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public static async Task<FakeFragment> StartAsync(CoordinatorTestHost host, string name)
        {
            var client = host.CreateClient();
            await client.ConnectAsync();
            var fragment = new FakeFragment(client, name);
            client.ControlConnection.On<string>("Launch", runId =>
            {
                fragment.Launched.TrySetResult(runId);
                return client.ControlConnection.SendAsync("Ready", runId);
            });
            // A real node answers a Cancel before it is fully wired with a local exit of its killed child.
            client.ControlConnection.On<string>("Cancel", runId =>
            {
                fragment.Cancelled.TrySetResult();
                return fragment.ExitAsync(1, runId: runId);
            });
            await client.ControlConnection.InvokeAsync("Register", name, "v1");
            return fragment;
        }

        public Task ExitAsync(int exitCode, string origin = "Local", string? runId = null) =>
            _client.ControlConnection.SendAsync(
                "Exited", runId ?? Launched.Task.Result, exitCode, origin, exitCode == 0 ? null : $"{Name} failed",
                new Dictionary<string, long>());

        public Task WithdrawAsync() => _client.ControlConnection.InvokeAsync("Unregister");

        public ValueTask DisposeAsync() => _client.DisposeAsync();
    }

    private static Task<CoordinatorTestHost> StartHostAsync() =>
        CoordinatorTestHost.StartAsync(_ => { }, services => services.AddSingleton(new RunOrchestratorOptions
        {
            AdmissionTimeout = TimeSpan.FromSeconds(5),
            ReadyTimeout = TimeSpan.FromSeconds(5),
            ExitTimeout = TimeSpan.FromSeconds(2),
        }));

    private static RunSpec Spec(string runId, params string[] fragments) =>
        new(runId, fragments.Select(f => new FragmentPin(f)).ToList(), []);

    /// <summary>Lets anything the first verdict could wrongly set off in the other run arrive before the test looks.</summary>
    private static Task Settle() => Task.Delay(300);

    [Fact]
    public async Task TwoDisjointRuns_AreInFlightTogether_EachWithItsOwnVerdict()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeFragment.StartAsync(host, "a");
        await using var b = await FakeFragment.StartAsync(host, "b");
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
        await using var a = await FakeFragment.StartAsync(host, "a");
        await using var b = await FakeFragment.StartAsync(host, "b");
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
        await using var a = await FakeFragment.StartAsync(host, "a");
        await using var b = await FakeFragment.StartAsync(host, "b");
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var first = orchestrator.RunAsync(Spec("run-1", "a"));
        var second = orchestrator.RunAsync(Spec("run-2", "b"));
        await Task.WhenAll(a.Launched.Task, b.Launched.Task).WaitAsync(Wait);

        await a.WithdrawAsync();
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

    /// <summary>A pipeline node serves one run: a run naming a fragment another run holds is refused before anything launches.</summary>
    [Fact]
    public async Task AFragmentAlreadyInARun_IsRefused_NamingTheRunThatHoldsIt()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeFragment.StartAsync(host, "a");
        await using var b = await FakeFragment.StartAsync(host, "b");
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var first = orchestrator.RunAsync(Spec("run-1", "a"));
        await a.Launched.Task.WaitAsync(Wait);

        var refused = await Assert.ThrowsAsync<FragmentInFlightException>(() => orchestrator.RunAsync(Spec("run-2", "b", "a")));
        await Settle();

        Assert.Equal("a", refused.FragmentName);
        Assert.Equal("run-1", refused.RunId);
        Assert.False(b.Launched.Task.IsCompleted);

        await a.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await first.WaitAsync(Wait)).Outcome);

        // The refusal claimed nothing: once run-1 is over, b runs.
        var later = orchestrator.RunAsync(Spec("run-3", "b"));
        Assert.Equal("run-3", await b.Launched.Task.WaitAsync(Wait));
        await b.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await later.WaitAsync(Wait)).Outcome);
    }

    [Fact]
    public async Task AReportForARunNotInFlight_IsIgnored()
    {
        await using var host = await StartHostAsync();
        await using var a = await FakeFragment.StartAsync(host, "a");
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();

        var run = orchestrator.RunAsync(Spec("run-1", "a"));
        await a.Launched.Task.WaitAsync(Wait);

        await a.ExitAsync(1, runId: "run-0");
        await Settle();
        Assert.False(run.IsCompleted);

        await a.ExitAsync(0);
        Assert.Equal(RunOutcome.Succeeded, (await run.WaitAsync(Wait)).Outcome);
    }
}
