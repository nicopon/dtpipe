using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Tests.Infrastructure;
using DtPipe.PipelineNode.Hosting;
using DtPipe.PipelineNode.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Identity;
using TransportR.Interfaces;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// The coordinator's <c>Kill</c>, which ends the child of a run outright, as it reaches a replica over the node channel: a host
/// ends the child only when its own configuration enabled fault injection, and says so when it declines.
/// </summary>
[Collection(ChildExitTimingCollection.Name)]
public class FaultInjectionTests : IDisposable
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);
    private const string Node = "node-x";

    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("fault-injection-");

    public void Dispose() => _dir.Delete(recursive: true);

    /// <summary>A source that emits ten rows a second, so the child lives for <paramref name="rows"/> / 10 seconds.</summary>
    private string SlowJob(int rows)
    {
        var job = Path.Combine(_dir.FullName, $"job-{rows}.yaml");
        File.WriteAllText(job, $"a:\n  input: generate:{rows}\n  provider-options:\n    generate:\n      rows-per-second: 10\n  output: csv:{Path.Combine(_dir.FullName, $"out-{rows}.csv")}\n");
        return job;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + Bound;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException(what);
            await Task.Delay(50);
        }
    }

    [Fact]
    public async Task AHostThatEnabledFaultInjection_EndsTheChildOfTheFragment_AndSaysHowManyItEnded()
    {
        await using var host = await CoordinatorTestHost.StartAsync(o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        await using var node = await TestNode.StartAsync(host, "A", SlowJob(100), [], faultInjection: true);
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        var link = host.Host.Services.GetRequiredService<INodeLink>();

        var run = orchestrator.RunAsync(new RunSpec("run-killed", [node.Fragment], []));
        await WaitUntilAsync(() => node.IsLaunched, "the child never started");
        var killed = await link.KillAsync(node.Key, "A", TestContext.Current.CancellationToken);

        Assert.Equal(1, killed);
        var result = await run.WaitAsync(Bound);
        Assert.Equal(RunOutcome.Failed, result.Outcome);
        await node.Completion.WaitAsync(Bound);
    }

    [Fact]
    public async Task AHostThatDidNotEnableFaultInjection_EndsNothing_AndSaysNoneWasEnded()
    {
        await using var host = await CoordinatorTestHost.StartAsync(o => o.Groups["test"] = new TransportR.FlowControl.GroupAccess { CanSendTo = ["test"] });
        await using var node = await TestNode.StartAsync(host, "A", SlowJob(30), []);
        var orchestrator = host.Host.Services.GetRequiredService<IRunOrchestrator>();
        var link = host.Host.Services.GetRequiredService<INodeLink>();

        var run = orchestrator.RunAsync(new RunSpec("run-spared", [node.Fragment], []));
        await WaitUntilAsync(() => node.IsLaunched, "the child never started");
        var killed = await link.KillAsync(node.Key, "A", TestContext.Current.CancellationToken);

        Assert.Equal(0, killed);
        var result = await run.WaitAsync(Bound);
        Assert.Equal(RunOutcome.Succeeded, result.Outcome);
        await node.Completion.WaitAsync(Bound);
    }

    /// <summary>The warnings a host's log carries, by level and text.</summary>
    private sealed class CapturedLog : ILoggerProvider
    {
        private readonly List<(LogLevel Level, string Text)> _entries = [];

        public IReadOnlyList<(LogLevel Level, string Text)> Entries
        {
            get { lock (_entries) return [.. _entries]; }
        }

        public ILogger CreateLogger(string categoryName) => new Capture(this);

        public void Dispose() { }

        private sealed class Capture(CapturedLog owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                lock (owner._entries) owner._entries.Add((logLevel, formatter(state, exception)));
            }
        }
    }

    /// <summary>A real agent against a real coordinator hub, as a host runs it: the replica it announces, and the log it keeps.</summary>
    private async Task<(CoordinatorTestHost Host, FakeIdp Idp, NodeAgent Agent, ReplicaKey Replica, CapturedLog Log)> StartAgentAsync(
        Action<DtPipeNodeOptions>? configure = null)
    {
        var jwt = new TestJwt();
        var rights = new MutableGroups();
        rights.BySubject[Node] = ["g"];
        var host = await CoordinatorTestHost.StartAsync(
            _ => { }, services => services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider>(rights)), jwt);
        var idp = await FakeIdp.StartAsync(jwt);
        var options = new DtPipeNodeOptions
        {
            Name = Node,
            CoordinatorUrl = host.Url,
            DtPipe = Path.Combine(_dir.FullName, "no-such-dtpipe"),
            StateDirectory = Path.Combine(_dir.FullName, "state"),
            Oidc = new NodeOidcOptions { Authority = idp.Authority, ClientId = Node, ClientSecret = "secret" },
        };
        configure?.Invoke(options);
        var log = new CapturedLog();
        var agent = new NodeAgent(Options.Create(options), new NodeDefinition(), LoggerFactory.Create(b => b.AddProvider(log)));
        await agent.StartAsync(CancellationToken.None);

        // The run protocol answers once the replica announced a data plane client: until then there is nothing to kill.
        var registry = host.Host.Services.GetRequiredService<ReplicaRegistry>();
        await WaitUntilAsync(() => registry.Replicas(Node) is [{ Connected: true, DataPlaneClientId: not null }], "the replica never announced a data plane client");
        return (host, idp, agent, registry.Replicas(Node)[0].Key, log);
    }

    /// <summary>
    /// What a host's configuration does reaches the run protocol: a host started without the setting declines the coordinator's
    /// <c>Kill</c> and says why in its log, even when no run of the fragment is in flight.
    /// </summary>
    [Fact]
    public async Task AnAgentStartedWithoutFaultInjection_DeclinesTheKill_AndLogsTheSetting()
    {
        var (host, idp, agent, replica, log) = await StartAgentAsync();
        await using var _host = host;
        await using var _idp = idp;
        try
        {
            var killed = await host.Host.Services.GetRequiredService<INodeLink>().KillAsync(replica, "no-such-fragment", TestContext.Current.CancellationToken);

            Assert.Equal(0, killed);
            Assert.Contains(log.Entries, e => e.Level == LogLevel.Warning && e.Text.Contains("FaultInjection", StringComparison.Ordinal));
        }
        finally
        {
            await agent.StopAsync(CancellationToken.None);
            agent.Dispose();
        }
    }

    /// <summary>The same host with the setting on goes through the kill: it logs that it killed none (no run), and no refusal.</summary>
    [Fact]
    public async Task AnAgentStartedWithFaultInjection_AnswersTheKill_AndLogsNoRefusal()
    {
        var (host, idp, agent, replica, log) = await StartAgentAsync(o => o.FaultInjection = true);
        await using var _host = host;
        await using var _idp = idp;
        try
        {
            var killed = await host.Host.Services.GetRequiredService<INodeLink>().KillAsync(replica, "no-such-fragment", TestContext.Current.CancellationToken);

            Assert.Equal(0, killed);
            Assert.Contains(log.Entries, e => e.Level == LogLevel.Warning && e.Text.StartsWith("Killed 0 ", StringComparison.Ordinal));
            Assert.DoesNotContain(log.Entries, e => e.Text.Contains("FaultInjection", StringComparison.Ordinal));
        }
        finally
        {
            await agent.StopAsync(CancellationToken.None);
            agent.Dispose();
        }
    }
}
