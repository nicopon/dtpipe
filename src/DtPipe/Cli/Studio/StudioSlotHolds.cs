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
    private TaskCompletionSource? _drained;

    public object Gate { get; } = new();

    /// <summary>A lease on what <paramref name="read"/> answers under the lock: it stays in place until the lease is disposed.</summary>
    public Lease<T> Hold<T>(Func<T> read)
    {
        lock (Gate)
        {
            if (_draining) throw new StudioRefusal(409, changing);
            var value = read();
            _holds++;
            return new Lease<T>(this, value);
        }
    }

    /// <summary>
    /// Runs <paramref name="replace"/> under the lock once no lease is held, waiting asynchronously up to <paramref name="drain"/>
    /// for those held to end, or refuses with 409.
    /// </summary>
    public async Task ReplaceAsync(Action replace, TimeSpan drain = default, CancellationToken ct = default)
    {
        TaskCompletionSource drained;
        lock (Gate)
        {
            if (_draining) throw new StudioRefusal(409, string.IsNullOrEmpty(changing) ? busy : changing);
            if (_holds == 0)
            {
                replace();
                return;
            }
            if (drain <= TimeSpan.Zero) throw new StudioRefusal(409, busy);
            _draining = true;
            drained = _drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        try
        {
            try
            {
                await drained.Task.WaitAsync(drain, ct);
            }
            catch (TimeoutException)
            {
            }

            lock (Gate)
            {
                if (_holds > 0) throw new StudioRefusal(409, busy);
                replace();
            }
        }
        finally
        {
            lock (Gate)
            {
                _draining = false;
                _drained = null;
            }
        }
    }

    internal sealed class Lease<T>(SlotHolds holds, T value) : IDisposable
    {
        private int _released;

        public T Value => value;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) != 0) return;
            TaskCompletionSource? drained = null;
            lock (holds.Gate)
            {
                if (--holds._holds == 0) drained = holds._drained;
            }
            drained?.TrySetResult();
        }
    }
}
