using DtPipe.Coordinator;
using DtPipe.Coordinator.Tests.Infrastructure;
using DtPipe.PipelineNode.Hosting;
using DtPipe.PipelineNode.Tests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using TransportR.Identity;
using TransportR.Interfaces;
using Xunit;

namespace DtPipe.PipelineNode.Tests;

/// <summary>
/// When a host asks for its data plane client: only once the coordinator has accepted its announcement. A host whose identity
/// is unknown or disabled is refused at the announcement and is not an identity that could register a client, so it asks the
/// IdP for no token and connects no client while it is refused.
/// </summary>
public class DataPlaneAfterAnnouncementTests : IDisposable
{
    private const string Node = "node-x";
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly DirectoryInfo _dir = Directory.CreateTempSubdirectory("data-plane-after-announcement-");

    public void Dispose() => _dir.Delete(recursive: true);

    private sealed record Harness(CoordinatorTestHost Host, FakeIdp Idp, NodeAgent Agent, MutableGroups Rights, AnnouncementGate Gate);

    /// <summary>A real agent against a real coordinator hub: the rights are the ones the data plane reads, the gate is what the node channel does with an announcement.</summary>
    private async Task<Harness> StartAsync(string[] groups, bool refusing)
    {
        var jwt = new TestJwt();
        var rights = new MutableGroups();
        rights.BySubject[Node] = groups;
        var host = await CoordinatorTestHost.StartAsync(
            _ => { }, services => services.Replace(ServiceDescriptor.Singleton<IClientIdentityProvider>(rights)), jwt);
        var gate = host.Host.Services.GetRequiredService<AnnouncementGate>();
        gate.Refusing = refusing;
        var idp = await FakeIdp.StartAsync(jwt);
        var options = new DtPipeNodeOptions
        {
            Name = Node,
            CoordinatorUrl = host.Url,
            DtPipe = Path.Combine(_dir.FullName, "no-such-dtpipe"),
            StateDirectory = Path.Combine(_dir.FullName, "state"),
            Oidc = new NodeOidcOptions { Authority = idp.Authority, ClientId = Node, ClientSecret = "secret" },
        };
        var agent = new NodeAgent(Options.Create(options), new NodeDefinition(), NullLoggerFactory.Instance);
        await agent.StartAsync(CancellationToken.None);
        return new Harness(host, idp, agent, rights, gate);
    }

    private static async Task StopAsync(Harness h)
    {
        await h.Agent.StopAsync(CancellationToken.None);
        h.Agent.Dispose();
        await h.Idp.DisposeAsync();
        await h.Host.DisposeAsync();
    }

    private static async Task<bool> EventuallyAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + Wait;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) return false;
            await Task.Delay(50);
        }
        return true;
    }

    /// <summary>
    /// The identity holds no group, as a disabled identity does, and the coordinator refuses the announcement again and again: the
    /// host asks the IdP for no token beyond the first and connects no client, however long it is refused. Once the coordinator
    /// accepts it, the client registers and a second announcement carries it.
    /// </summary>
    [Fact]
    public async Task AHostWhoseAnnouncementIsRefused_AsksForNoNewTokenAndConnectsNoDataPlaneClient_UntilItIsAccepted()
    {
        var h = await StartAsync(groups: [], refusing: true);
        try
        {
            Assert.True(await EventuallyAsync(() => h.Gate.Announcements >= 1), "the host never announced");
            var tokens = h.Idp.TokenRequests;
            Assert.True(await EventuallyAsync(() => h.Gate.Announcements >= 4), "the host did not keep announcing");

            Assert.Equal(0, h.Rights.Resolutions);
            Assert.Equal(tokens, h.Idp.TokenRequests);

            h.Rights.BySubject[Node] = ["g"];
            h.Gate.Refusing = false;

            var registry = h.Host.Host.Services.GetRequiredService<ReplicaRegistry>();
            Assert.True(await EventuallyAsync(() => registry.Replicas(Node) is [{ Connected: true, DataPlaneClientId: not null }]),
                "the replica never announced a data plane client once it was accepted");
        }
        finally
        {
            await StopAsync(h);
        }
    }

    /// <summary>
    /// The host's watch of its data plane runs every few seconds while its channel is up, and the channel is up between the
    /// connection and the coordinator's refusal. A refusal that takes longer than a tick of the watch must not make the watch
    /// connect a client: the host asks for one only once it is announced.
    /// </summary>
    [Fact]
    public async Task AWatchTickInsideARefusedAnnouncement_ConnectsNoDataPlaneClient()
    {
        var h = await StartAsync(groups: ["g"], refusing: false);
        try
        {
            var registry = h.Host.Host.Services.GetRequiredService<ReplicaRegistry>();
            Assert.True(await EventuallyAsync(() => registry.Replicas(Node) is [{ Connected: true, DataPlaneClientId: not null }]),
                "the replica never announced a data plane client");

            // The identity loses its groups and the coordinator holds the next refusal for longer than a tick of the watch.
            var announced = h.Gate.Announcements;
            h.Rights.BySubject[Node] = [];
            h.Gate.RefuseAfter = TimeSpan.FromSeconds(4.5);
            h.Gate.Refusing = true;
            registry.Disconnect(Node);

            Assert.True(await EventuallyAsync(() => h.Gate.Announcements > announced), "the host never announced again");
            var resolutions = h.Rights.Resolutions;
            await Task.Delay(TimeSpan.FromSeconds(3.8));

            Assert.Equal(resolutions, h.Rights.Resolutions);
        }
        finally
        {
            await StopAsync(h);
        }
    }
}
