namespace DtPipe.Cli.Studio;

/// <summary>
/// The requests that use what a slot holds, counted under the slot's lock (<see cref="Gate"/>, which the slot takes for its own
/// fields too). Each takes a lease and disposes it at its end; a replacement runs under that lock only while no lease is held,
/// and is refused with 409 otherwise, so nothing is replaced under a request that uses it.
/// </summary>
internal sealed class SlotHolds(string busy)
{
    private int _holds;

    public object Gate { get; } = new();

    /// <summary>A lease on what <paramref name="read"/> answers under the lock: it stays in place until the lease is disposed.</summary>
    public Lease<T> Hold<T>(Func<T> read)
    {
        lock (Gate)
        {
            _holds++;
            return new Lease<T>(this, read());
        }
    }

    /// <summary>Runs <paramref name="replace"/> under the lock, or refuses with 409 while a lease is held.</summary>
    public void Replace(Action replace)
    {
        lock (Gate)
        {
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
            if (Interlocked.Exchange(ref _released, 1) == 0)
                lock (holds.Gate) holds._holds--;
        }
    }
}
