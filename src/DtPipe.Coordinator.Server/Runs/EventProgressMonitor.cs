using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using TransportR.Hub;

namespace DtPipe.Coordinator.Server.Runs;

/// <summary>
/// Takes the place of the coordinator's progress monitor, keeps its logging by delegating to it first,
/// and publishes one line of the pages' log per write stream the hub takes on and per write-transport
/// probe it admits, naming the replica that owns the client when the registry knows it. Both
/// announcements are implemented here: the interface's default for them does nothing, and a missing
/// override is silent.
/// </summary>
public sealed class EventProgressMonitor(LoggingHubProgressMonitor inner, EventBus bus, ReplicaRegistry replicas) : IHubProgressMonitor
{
    public void OnBytesTransferred(long bytes) => inner.OnBytesTransferred(bytes);

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
}
