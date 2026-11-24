namespace DtPipe.Cli.Studio;

/// <summary>
/// Where the coordinator's connection stands, as the page reads it. <see cref="Auth"/> is <c>browser</c> or <c>key</c>;
/// <see cref="Source"/> is <c>flag</c>, <c>remembered</c> or <c>page</c>; <see cref="Key"/> is <c>none</c>, <c>stored</c> (in the
/// keyring), <c>flag</c> or <c>session</c> (held by this studio alone). <see cref="NeedsKey"/>: the connection was remembered with
/// a key that is not to be found, so no link exists yet.
/// </summary>
public sealed record CoordinatorConnection(string Origin, string Auth, string Source, string Key, bool NeedsKey = false);

/// <summary>What the page may do to the coordinator's link: the settings it remembers, and the connection the studio started with.</summary>
public sealed record StudioCoordinatorStart(StudioSettings Settings, CoordinatorConnection? Start = null);

/// <summary>
/// The studio's coordinator link, which the page may replace. Every request that reaches the coordinator holds the link for its
/// whole length (<see cref="Hold"/>), and a replacement is refused while one is held, so a publication or a library read never
/// finds its link closed under it. A replacement stops the sign-in the old link was making, then closes it with the tokens it
/// held.
/// </summary>
internal sealed class CoordinatorSlot(CoordinatorLink? link, CoordinatorConnection? connection) : IDisposable
{
    private readonly object _gate = new();
    private CoordinatorLink? _link = link;
    private CoordinatorConnection? _connection = connection;
    private int _holds;
    private CancellationTokenSource? _signIn;
    private Task _signingIn = Task.CompletedTask;

    public CoordinatorLink? Current
    {
        get { lock (_gate) return _link; }
    }

    public CoordinatorConnection? Connection
    {
        get { lock (_gate) return _connection; }
    }

    /// <summary>Another description of the connection in place, as it stands after a change of memory.</summary>
    public void Describe(CoordinatorConnection? updated)
    {
        lock (_gate) _connection = updated;
    }

    /// <summary>Holds the link in place until the lease is disposed; <see cref="Lease.Link"/> is null when none is connected.</summary>
    public Lease Hold()
    {
        lock (_gate)
        {
            _holds++;
            return new Lease(this, _link);
        }
    }

    internal sealed class Lease(CoordinatorSlot slot, CoordinatorLink? link) : IDisposable
    {
        private int _released;

        public CoordinatorLink? Link => link;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                lock (slot._gate) slot._holds--;
        }
    }

    /// <summary>
    /// Puts <paramref name="next"/> in place of the current link, or refuses with 409 while a request holds it. The sign-in the old
    /// link was making stops first, and the old link then forgets its tokens and closes. <paramref name="signIn"/> starts the key's
    /// sign-in on the new link, in the background: a coordinator that does not answer never delays the caller.
    /// </summary>
    public async Task ReplaceAsync(CoordinatorLink? next, CoordinatorConnection? described, bool signIn)
    {
        CoordinatorLink? old;
        CancellationTokenSource? stopped;
        Task stopping;
        lock (_gate)
        {
            if (_holds > 0) throw new StudioRefusal(409, "The coordinator is being used by another request: wait for it to finish.");
            old = _link;
            stopped = _signIn;
            stopping = _signingIn;
            _link = next;
            _connection = described;
            _signIn = null;
            _signingIn = Task.CompletedTask;
            if (signIn && next is { ByKey: true }) StartSignIn(next);
        }
        if (stopped is not null)
        {
            await stopped.CancelAsync();
            await stopping;
            stopped.Dispose();
        }
        if (old is not null)
        {
            old.SignOutNow();
            old.Dispose();
        }
    }

    /// <summary>Signs the link in with its key without waiting; a failure stays in the link's status for the page to show.</summary>
    public void StartSignIn(CoordinatorLink link)
    {
        var cts = new CancellationTokenSource();
        lock (_gate)
        {
            _signIn = cts;
            _signingIn = Run(link, cts.Token);
        }

        static async Task Run(CoordinatorLink link, CancellationToken ct)
        {
            try { await link.SignInWithKeyAsync(ct); }
            catch (Exception ex) when (ex is CoordinatorLinkException or HttpRequestException or OperationCanceledException or ObjectDisposedException) { }
        }
    }

    public void Dispose()
    {
        _signIn?.Cancel();
        try { _signingIn.Wait(TimeSpan.FromSeconds(5)); }
        catch (AggregateException) { }
        _signIn?.Dispose();
        _link?.Dispose();
    }
}
