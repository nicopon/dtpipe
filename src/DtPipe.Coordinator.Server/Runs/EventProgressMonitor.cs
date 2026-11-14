using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using TransportR.Hub;

namespace DtPipe.Coordinator.Server.Runs;

/// <summary>
/// Takes the place of the coordinator's progress monitor, keeps its logging by delegating to it first,
/// and publishes what the pages read: the bytes crossing the hub at most four times a second (TransportR
/// reports bytes without a transfer id, so the figure is the hub's total, not a per-edge one), and one
/// line of the pages' log per write stream the hub takes on and per write-transport probe it admits,
/// naming the replica that owns the client when the registry knows it. Both announcements are
/// implemented here: the interface's default for them does nothing, and a missing override is silent.
/// </summary>
public sealed class EventProgressMonitor(LoggingHubProgressMonitor inner, EventBus bus, ReplicaRegistry replicas) : IHubProgressMonitor
{
    private long _bytes;
    private long _lastPublishTicks;

    public void OnBytesTransferred(long bytes)
    {
        inner.OnBytesTransferred(bytes);
        Interlocked.Add(ref _bytes, bytes);
        Publish();
    }

    public void OnWriteStreamOpened(WriteStreamOpened opened)
    {
        inner.OnWriteStreamOpened(opened);
        Log($"transfer {opened.TransferId}: {Who(opened.ClientId)} writes its {opened.Stream} stream over {opened.Transport}");
    }

    public void OnWriteTransportProbed(WriteTransportProbed probed)
    {
        inner.OnWriteTransportProbed(probed);
        Log($"{Who(probed.ClientId)} probed its write path: {probed.Transport} gets through");
    }

    private void Log(string message) => bus.Publish("log", new NodeLogLine("coordinator", null, "Information", message));

    /// <summary>The replica that registered this data-plane client, else the client's identifier.</summary>
    private string Who(Guid clientId) =>
        replicas.Replicas().FirstOrDefault(r => r.DataPlaneClientId == clientId) is { } replica
            ? $"{replica.Key} ({clientId})"
            : $"client {clientId}";

    private void Publish()
    {
        var now = Environment.TickCount64;
        var last = Interlocked.Read(ref _lastPublishTicks);
        if (now - last < 250 || Interlocked.CompareExchange(ref _lastPublishTicks, now, last) != last) return;
        bus.Publish("bytes", new { total = Interlocked.Read(ref _bytes) });
    }
}
