using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Runs;
using Microsoft.Extensions.Logging;
using TransportR;
using TransportR.Hub;
using Xunit;

namespace DtPipe.Coordinator.Server.Tests;

/// <summary>
/// What the coordinator makes of the two announcements the hub makes about the write path: a line in its log
/// and a line in the pages' log, the replica's name in place of a bare client identifier when the registry knows the client.
/// </summary>
public sealed class EventProgressMonitorTests
{
    private static readonly Guid Client = Guid.Parse("11111111-1111-1111-1111-111111111111");

    private static (EventProgressMonitor Monitor, EventBus Bus, CapturedLog Log, ReplicaRegistry Replicas) Make()
    {
        var log = new CapturedLog();
        var bus = new EventBus();
        var replicas = new ReplicaRegistry();
        var inner = new LoggingHubProgressMonitor(new LoggerFactory([log]).CreateLogger<LoggingHubProgressMonitor>());
        return (new EventProgressMonitor(inner, bus, replicas), bus, log, replicas);
    }

    private static NodeLogLine PagesLine(EventBus bus)
    {
        var (backlog, _, unsubscribe) = bus.Subscribe();
        unsubscribe();
        var evt = Assert.Single(backlog);
        Assert.Equal("log", evt.Type);
        return Assert.IsType<NodeLogLine>(evt.Data);
    }

    [Fact]
    public void An_Opened_Write_Stream_Reaches_The_Log_And_The_Pages_Log()
    {
        var (monitor, bus, log, _) = Make();

        monitor.OnWriteStreamOpened(new WriteStreamOpened("t-1", Client, ConnectionType.Data, WriteTransport.Segments));

        var line = PagesLine(bus);
        Assert.Equal(("coordinator", "Information"), (line.Node, line.Level));
        Assert.Contains("t-1", line.Message);
        Assert.Contains(Client.ToString(), line.Message);
        Assert.Contains("Data", line.Message);
        Assert.Contains("Segments", line.Message);
        var logged = Assert.Single(log.Entries);
        Assert.Contains("t-1", logged);
        Assert.Contains("Segments", logged);
    }

    [Fact]
    public void A_Write_Transport_Probe_Reaches_The_Log_And_The_Pages_Log()
    {
        var (monitor, bus, log, _) = Make();

        monitor.OnWriteTransportProbed(new WriteTransportProbed(Client, WriteTransport.WebSocket));

        var line = PagesLine(bus);
        Assert.Equal(("coordinator", "Information"), (line.Node, line.Level));
        Assert.Contains(Client.ToString(), line.Message);
        Assert.Contains("WebSocket", line.Message);
        var logged = Assert.Single(log.Entries);
        Assert.Contains(Client.ToString(), logged);
        Assert.Contains("WebSocket", logged);
    }

    [Fact]
    public void The_Pages_Name_The_Replica_That_Owns_The_Client()
    {
        var (monitor, bus, _, replicas) = Make();
        var instance = Guid.Parse("22222222-2222-2222-2222-222222222222");
        replicas.Announce("c1", "node-a", new NodeAnnouncement("node-a", "", [], NodeRole.Data, [], InstanceId: instance, DataPlaneClientId: Client), [], () => { });

        monitor.OnWriteTransportProbed(new WriteTransportProbed(Client, WriteTransport.WebSocket));

        Assert.StartsWith("node-a/", PagesLine(bus).Message);
    }
}
