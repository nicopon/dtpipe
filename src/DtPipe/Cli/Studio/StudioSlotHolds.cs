namespace DtPipe.Cli.Studio;

/// <summary>A single-use disposal token that invokes its release callback at most once.</summary>
internal class OneShotLease(Action release) : IDisposable
{
    private int _released;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _released, 1) == 0) release();
    }
}

/// <summary>A single-use disposal token carrying a captured value.</summary>
internal sealed class OneShotLease<T>(T value, Action release) : OneShotLease(release)
{
    public T Value => value;
}

/// <summary>
/// Serializes engine actions (samples, previews, runs, and slot-wide changes) behind a non-blocking gate that names
/// the active holder when busy, or an async wait for agent pipeline tools.
/// </summary>
internal sealed class EngineSlot : IDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private string _holder = "An action";

    /// <summary>Takes the engine slot immediately, or refuses naming who holds it.</summary>
    public OneShotLease Take(string who)
    {
        if (!_gate.Wait(0)) throw new StudioRefusal(409, $"{_holder} is already running: wait for it to finish.");
        _holder = who;
        return new OneShotLease(() => _gate.Release());
    }

    /// <summary>Waits asynchronously for the engine slot until <paramref name="ct"/> cancels.</summary>
    public async Task<OneShotLease> WaitAsync(string who, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        _holder = who;
        return new OneShotLease(() => _gate.Release());
    }

    public void Dispose() => _gate.Dispose();
}

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
    public OneShotLease<T> Hold<T>(Func<T> read)
    {
        lock (Gate)
        {
            if (_draining) throw new StudioRefusal(409, changing);
            var value = read();
            _holds++;
            return new OneShotLease<T>(value, Release);
        }
    }

    private void Release()
    {
        TaskCompletionSource? drained = null;
        lock (Gate)
        {
            if (--_holds == 0) drained = _drained;
        }
        drained?.TrySetResult();
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
}
