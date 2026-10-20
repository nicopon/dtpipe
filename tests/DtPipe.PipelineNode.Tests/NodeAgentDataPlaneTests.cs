using DtPipe.Coordinator;
using DtPipe.Coordinator.Tests.Infrastructure;
using DtPipe.PipelineNode.Hosting;
using DtPipe.PipelineNode.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using TransportR.Identity;
using TransportR.Interfaces;
using TransportR.Models;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// A node host's agent against a real coordinator hub: the data plane client it registers follows what its identity may do.
/// The host's dtpipe names no binary, so no child is started and the test needs no timing collection.
/// </summary>
public class NodeAgentDataPlaneTests : IDisposable
{
    private const string Node = "node-x";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("agent-data-plane-");

    /// <summary>The agent's own log, written to the test's output so a failure shows what the host did.</summary>
    private sealed class OutputLoggerProvider(ITestOutputHelper? output) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new OutputLogger(output, categoryName);

        public void Dispose() { }

        private sealed class OutputLogger(ITestOutputHelper? output, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (IsEnabled(logLevel)) output?.WriteLine($"{DateTime.UtcNow:HH:mm:ss.fff} {logLevel,-11} {category.Split('.').Last()}: {formatter(state, exception)}");
            }
        }
    }

    public void Dispose() => _dir.Delete(recursive: true);

    private async Task<(CoordinatorTestHost Host, FakeIdp Idp, NodeAgent Agent, MutableGroups Rights)> StartAsync()
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
        var agent = new NodeAgent(Options.Create(options), new NodeDefinition(), LoggerFactory.Create(b => b.AddProvider(new OutputLoggerProvider(TestContext.Current.TestOutputHelper))));
        await agent.StartAsync(CancellationToken.None);
        return (host, idp, agent, rights);
    }

    private static async Task<bool> EventuallyAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline) return false;
            await Task.Delay(100);
        }
        return true;
    }

    /// <summary>The data plane client the node's replica announces, once it announces one.</summary>
    private static async Task<Guid> AnnouncedClientAsync(ReplicaRegistry registry)
    {
        Guid? client = null;
        Assert.True(await EventuallyAsync(() =>
        {
            client = registry.Replicas(Node) is [{ Connected: true, DataPlaneClientId: { } id }] ? id : null;
            return Task.FromResult(client is not null);
        }), "the replica never announced a data plane client");
        return client!.Value;
    }

    private static string GroupsOf(ClientRegistration? registration) => string.Join(",", (registration?.Groups ?? []).Order());

    /// <summary>
    /// The identity loses every group and the coordinator closes the channel: the host takes the groups of the moment, is
    /// refused, and holds no client any more, so the hub's registration is disconnected and the replica announces none.
    /// When the groups return, a client registers again and is announced.
    /// </summary>
    [Fact]
    public async Task AnIdentityThatLosesItsGroups_LosesItsClient_AndANewOneRegistersWhenTheyReturn()
    {
        var (host, idp, agent, rights) = await StartAsync();
        await using var _host = host;
        await using var _idp = idp;
        var registry = host.Host.Services.GetRequiredService<ReplicaRegistry>();
        var store = host.Host.Services.GetRequiredService<IStateStore>();
        try
        {
            var first = await AnnouncedClientAsync(registry);
            Assert.Equal("g", GroupsOf(await store.GetClientAsync(first)));
            Assert.Null((await store.GetClientAsync(first))!.DisconnectedAtUtc);

            rights.BySubject[Node] = [];
            registry.Disconnect(Node);

            Assert.True(await EventuallyAsync(async () => (await store.GetClientAsync(first))!.DisconnectedAtUtc is not null),
                "the hub still holds the client of an identity with no group as connected");
            Assert.True(await EventuallyAsync(() => Task.FromResult(registry.Replicas(Node) is [{ Connected: true, DataPlaneClientId: null }])),
                "the replica did not announce itself without a data plane client");

            rights.BySubject[Node] = ["g"];
            var second = await AnnouncedClientAsync(registry);
            Assert.NotEqual(first, second);
            Assert.Equal("g", GroupsOf(await store.GetClientAsync(second)));
            Assert.Null((await store.GetClientAsync(second))!.DisconnectedAtUtc);
        }
        finally
        {
            await agent.StopAsync(CancellationToken.None);
            agent.Dispose();
        }
    }
}
