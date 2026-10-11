using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using TransportR.FlowControl;
using Xunit;

namespace DtPipe.Coordinator.Tests;

/// <summary>
/// A run is admitted, its fragment placed on a replica, and the replica has not answered the <c>Launch</c> yet. A report
/// of that fragment's exit that arrives now is the report of this launch only if it carries the launch's token: an earlier
/// child that held the same run id, fragment and replica reports without it. The order of the two messages, the answer to
/// the launch and the report, decides nothing.
/// </summary>
public class ExitBeforeLaunchAnswerTests : IAsyncLifetime
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private CoordinatorTestHost _host = null!;
    private FakeNode _node = null!;
    private TaskCompletionSource<LaunchResult> _answer = null!;
    private IRunOrchestrator _orchestrator = null!;
    private ReplicaKey _replica;
    private Task<RunResult> _run = null!;
    private LaunchRequest _launch = null!;

    public async ValueTask InitializeAsync()
    {
        _host = await CoordinatorTestHost.StartAsync(o => o.Groups["test"] = new GroupAccess { CanSendTo = ["test"] });
        _node = await FakeNode.StartAsync(_host, "node-a");
        _answer = new TaskCompletionSource<LaunchResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        _node.OnLaunch = _ => _answer.Task;
        _orchestrator = _host.Host.Services.GetRequiredService<IRunOrchestrator>();
        _replica = _host.Host.Services.GetRequiredService<ReplicaRegistry>().Replicas("node-a").Single().Key;

        // The run is admitted and the replica has the launch; the answer is still pending.
        _run = _orchestrator.RunAsync(new RunSpec("run-1", [TestFragments.Edgeless("frag-a", "node-a")], []));
        var deadline = DateTime.UtcNow + Wait;
        while (_node.Launches.IsEmpty)
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("the launch never reached the replica");
            await Task.Delay(10);
        }
        _launch = _node.Launches.First();
    }

    public async ValueTask DisposeAsync()
    {
        _answer.TrySetResult(new LaunchResult(false, "released by the test"));
        try { await _run.WaitAsync(Wait); } catch (Exception) { /* the run's own verdict is not this test's */ }
        await _node.DisposeAsync();
        await _host.DisposeAsync();
    }

    private static FragmentExit Exit(string? token) =>
        new("run-1", "frag-a", 0, "Local", null, new Dictionary<string, long>(), token);

    [Fact]
    public async Task AnExitThatCarriesNoToken_IsNotAdmitted()
    {
        Assert.Equal(ExitedAck.NotAdmitted, await _orchestrator.OnExitedAsync(_replica, Exit(token: null)));
    }

    [Fact]
    public async Task AnExitThatCarriesAnotherLaunchsToken_IsNotAdmitted()
    {
        Assert.Equal(ExitedAck.NotAdmitted, await _orchestrator.OnExitedAsync(_replica, Exit("the-token-of-an-earlier-child")));
    }

    [Fact]
    public async Task AnExitThatCarriesTheLaunchsToken_IsAdmitted_WhateverTheOrderOfTheAnswer()
    {
        Assert.False(string.IsNullOrEmpty(_launch.LaunchToken));

        Assert.Equal(ExitedAck.Accepted, await _orchestrator.OnExitedAsync(_replica, Exit(_launch.LaunchToken)));

        _answer.SetResult(new LaunchResult(true));
        Assert.Equal(RunOutcome.Succeeded, (await _run.WaitAsync(Wait)).Outcome);
    }
}
