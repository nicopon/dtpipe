using System.Diagnostics;

namespace DtPipe.Cli.Studio;

/// <summary>
/// The requests that use what a slot holds, counted under the slot's lock (<see cref="Gate"/>, which the slot takes for its own
/// fields too). Each takes a lease and disposes it at its end; a replacement runs under that lock only while no lease is held,
/// and is refused with 409 otherwise, so nothing is replaced under a request that uses it. A replacement may wait a while for
/// the leases held to end; a request that arrives meanwhile is refused with 409 (<c>changing</c>): it was sent for what is being
/// replaced, and must not run against what replaces it.
/// </summary>
internal sealed class SlotHolds(string busy, string changing = "")
{
    private int _holds;
    private bool _draining;

    public object Gate { get; } = new();

    /// <summary>A lease on what <paramref name="read"/> answers under the lock: it stays in place until the lease is disposed.</summary>
    public Lease<T> Hold<T>(Func<T> read)
    {
        lock (Gate)
        {
            if (_draining) throw new StudioRefusal(409, changing);
            _holds++;
            return new Lease<T>(this, read());
        }
    }

    /// <summary>
    /// Runs <paramref name="replace"/> under the lock once no lease is held, waiting up to <paramref name="drain"/> for those held
    /// to end, or refuses with 409.
    /// </summary>
    public void Replace(Action replace, TimeSpan drain = default)
    {
        lock (Gate)
        {
            if (_holds > 0 && drain > TimeSpan.Zero)
            {
                _draining = true;
                var waited = Stopwatch.StartNew();
                try
                {
                    while (_holds > 0)
                    {
                        var left = drain - waited.Elapsed;
                        if (left <= TimeSpan.Zero || !Monitor.Wait(Gate, left)) break;
                    }
                }
                finally
                {
                    _draining = false;
                }
            }
            if (_holds > 0) throw new StudioRefusal(409, busy);
            replace();
        }
    }

    internal sealed class Lease<T>(SlotHolds holds, T value) : IDisposable
    {
        private int _released;

        public T Value => value;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) != 0) return;
            lock (holds.Gate)
            {
                holds._holds--;
                Monitor.PulseAll(holds.Gate);
            }
        }
    }
}
