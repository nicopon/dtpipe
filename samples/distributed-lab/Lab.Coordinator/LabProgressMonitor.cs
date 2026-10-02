using DtPipe.Coordinator;
using TransportR.Interfaces;

namespace DtPipe.Lab.Coordinator;

/// <summary>
/// Takes the place of the coordinator's progress monitor, keeps its logging by delegating to it,
/// and publishes the bytes crossing the hub at most four times a second. TransportR reports bytes
/// without a transfer id, so the figure is the hub's total, not a per-edge one.
/// </summary>
public sealed class LabProgressMonitor(LoggingHubProgressMonitor inner, EventBus bus) : IHubProgressMonitor
{
    private long _bytes;
    private int _activeTransfers;
    private long _lastPublishTicks;

    public void InitializeTransfer(string transferId)
    {
        inner.InitializeTransfer(transferId);
        Interlocked.Increment(ref _activeTransfers);
        Publish(force: true);
    }

    public void OnBytesTransferred(long bytes)
    {
        inner.OnBytesTransferred(bytes);
        Interlocked.Add(ref _bytes, bytes);
        Publish(force: false);
    }

    public void CompleteTransfer(string transferId)
    {
        inner.CompleteTransfer(transferId);
        Interlocked.Decrement(ref _activeTransfers);
        Publish(force: true);
    }

    private void Publish(bool force)
    {
        var now = Environment.TickCount64;
        if (force)
        {
            Interlocked.Exchange(ref _lastPublishTicks, now);
        }
        else
        {
            var last = Interlocked.Read(ref _lastPublishTicks);
            if (now - last < 250 || Interlocked.CompareExchange(ref _lastPublishTicks, now, last) != last) return;
        }
        bus.Publish("bytes", new { total = Interlocked.Read(ref _bytes), activeTransfers = Volatile.Read(ref _activeTransfers) });
    }
}
