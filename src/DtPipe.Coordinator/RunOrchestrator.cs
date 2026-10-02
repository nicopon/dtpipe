using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;

namespace DtPipe.Coordinator;

/// <summary>Mirrors <c>DtPipe.PipelineNode.FaultOrigin</c> on the wire - the two projects never share a type.</summary>
public enum FaultOrigin { Local, Remote }

/// <summary>One flow from a producer fragment's outbound alias to a consumer fragment's inbound alias.</summary>
public sealed record RunEdge(string ProducerFragment, string ProducerAlias, string ConsumerFragment, string ConsumerAlias);

/// <summary>The fragments a run needs present - each optionally pinned to a version, <see cref="FragmentPin"/> - and the edges to wire once they are.</summary>
public sealed record RunSpec(string RunId, IReadOnlyList<FragmentPin> Fragments, IReadOnlyList<RunEdge> Edges);

/// <summary>What one fragment reported when its own process finished.</summary>
public sealed record FragmentExitReport(
    string Fragment, int ExitCode, FaultOrigin? Origin, string? FirstFault, IReadOnlyDictionary<string, long> RowCounts);

/// <summary>
/// <see cref="Cancelled"/> mirrors the product's own 130 convention (root <c>CLAUDE.md</c>'s exit
/// codes): the requester asked for it, or the first uncommanded stop was itself a 130.
/// </summary>
public enum RunOutcome { Succeeded, Failed, Cancelled }

/// <summary>
/// One edge's two row counts, always both looked up - never defaulted to "agree" when a fragment
/// never reported a count for its side, which a chain of <c>&amp;&amp;</c> lookups would do by
/// short-circuiting past a missing key instead of flagging it.
/// </summary>
public sealed record EdgeCount(
    string ProducerFragment, string ProducerAlias, string ConsumerFragment, string ConsumerAlias,
    long? Sent, long? Received)
{
    public bool Agrees => Sent is not null && Received is not null && Sent == Received;
}

/// <summary>
/// <see cref="Cause"/> is null on <see cref="RunOutcome.Succeeded"/>, and on a requester-cancelled
/// <see cref="RunOutcome.Cancelled"/> run - the requester asked for it, nothing to name.
/// <see cref="Reports"/> is partial by construction: a fragment absent from it never reported
/// <c>Exited</c> at all, whether because it went silent or because the coordinator's own teardown
/// gave up waiting on it. <see cref="CauseIsUnresponsive"/> is set only when <see cref="Cause"/> is a
/// fragment the coordinator gave up on: one absent from <see cref="Reports"/>, or one that had not
/// reported when the grace after a remote failure ran out and reported only once told to cancel. It is
/// never true for the row-count-mismatch or refused-transfer forms of <see cref="Cause"/>, which are
/// messages, not fragment names.
/// </summary>
public sealed record RunResult(
    RunOutcome Outcome, string? Cause, IReadOnlyList<string> Consequences,
    IReadOnlyDictionary<string, FragmentExitReport> Reports, IReadOnlyList<EdgeCount> EdgeCounts,
    bool CauseIsUnresponsive = false)
{
    /// <summary>The human-readable run report the exit criterion asks for: cause, consequences, counts per edge.</summary>
    public string Describe()
    {
        var lines = new List<string> { $"Outcome: {Outcome}" };
        if (Cause is not null)
            lines.Add($"Cause: {Cause}" + (CauseIsUnresponsive ? " (unresponsive)" : ""));
        if (Consequences.Count > 0) lines.Add($"Consequences: {string.Join(", ", Consequences)}");
        foreach (var e in EdgeCounts)
        {
            var mismatch = e.Agrees ? "" : " (mismatch)";
            lines.Add($"  {e.ProducerFragment}.{e.ProducerAlias} -> {e.ConsumerFragment}.{e.ConsumerAlias}: " +
                      $"sent={e.Sent?.ToString() ?? "?"} received={e.Received?.ToString() ?? "?"}{mismatch}");
        }
        return string.Join(Environment.NewLine, lines);
    }
}

public sealed class RunOrchestratorOptions
{
    public TimeSpan AdmissionTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan ReadyTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Bounds only the teardown's wait for an already-cancelled or already-terminated fragment to
    /// finish reporting its own <c>Exited</c>, once the run has already been given up on.
    /// </summary>
    public TimeSpan ExitTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long a run goes on after a fragment failed <see cref="FaultOrigin.Remote"/> - its transfer
    /// failed under it. Its peers have this long to report their own <c>Exited</c>; when it runs out the
    /// run, already failed, is torn down, and the verdict names the peers that had not reported (see
    /// <see cref="RunOrchestrator.PeersOwingAReport"/>), never a fragment that is silent only because its
    /// own peer is. A <see cref="FaultOrigin.Local"/> fault, a lost node
    /// (<see cref="INodeRegistry.FragmentLost"/>) and the requester's cancellation each end the run on
    /// their own, so this is only the last bound on a run that nothing else ends: a peer that stopped
    /// answering without its node dropping, such as a consumer blocked on its sink. Keep it above
    /// <see cref="NodeRegistryOptions.DisconnectGracePeriod"/>, or a node that is merely reconnecting
    /// is named by its silence instead of by its loss. A run in which no fragment has failed
    /// <see cref="FaultOrigin.Remote"/> has no clock at all: a legitimately long transfer, or a slow
    /// consumer, is never reported failed for taking time. <see cref="Timeout.InfiniteTimeSpan"/>
    /// removes the bound.
    /// </summary>
    public TimeSpan RemoteFailureGrace { get; init; } = TimeSpan.FromSeconds(60);

    public int BatchSize { get; init; } = 8;
    public int TransferTimeoutMs { get; init; } = 20_000;
}

public interface IRunOrchestrator
{
    Task<RunResult> RunAsync(RunSpec spec, CancellationToken ct = default);
    Task OnReadyAsync(string runId, string fragment);
    Task OnExitedAsync(string runId, string fragment, int exitCode, string origin, string? firstFault, IReadOnlyDictionary<string, long> rowCounts);
}

/// <summary>
/// The barrier. Admits a run only once every fragment it names has registered (<see cref="AdmissionGate"/>),
/// launches all of them, opens each declared edge once both endpoints report ready, then waits out
/// the data-transfer phase and applies the outcome rule (<see cref="DetermineOutcome"/>). That phase ends
/// when every fragment has reported, or is given up on: a fragment lost (<see cref="INodeRegistry.FragmentLost"/>),
/// a fragment's own local failure, the caller's cancellation, or - once a fragment has failed
/// <see cref="FaultOrigin.Remote"/> - <see cref="RunOrchestratorOptions.RemoteFailureGrace"/> for its peers to
/// report. A run in which nothing has failed has no clock.
/// One run in flight at a time: a second call to <see cref="RunAsync"/> is refused outright, not
/// queued - concurrent runs are not supported yet.
/// </summary>
public sealed class RunOrchestrator : IRunOrchestrator
{
    private readonly AdmissionGate _admission;
    private readonly IHubContext<CoordinatorHub> _hub;
    private readonly ITransferInitiator _transferInitiator;
    private readonly ITransferTerminator _transferTerminator;
    private readonly INodeRegistry _nodeRegistry;
    private readonly IStateStore _stateStore;
    private readonly RunOrchestratorOptions _options;
    private readonly ILogger<RunOrchestrator> _logger;

    private readonly SemaphoreSlim _singleRun = new(1, 1);

    // Live only while a run is in flight; (re)built at the start of RunAsync, cleared in its finally.
    private string? _activeRunId;
    private Dictionary<string, TaskCompletionSource>? _ready;
    private Dictionary<string, TaskCompletionSource<FragmentExitReport>>? _exited;
    private CancellationTokenSource? _abort;
    private TaskCompletionSource? _remoteFailure;

    public RunOrchestrator(
        AdmissionGate admission,
        IHubContext<CoordinatorHub> hub,
        ITransferInitiator transferInitiator,
        ITransferTerminator transferTerminator,
        INodeRegistry nodeRegistry,
        IStateStore stateStore,
        ILogger<RunOrchestrator> logger,
        RunOrchestratorOptions? options = null)
    {
        _admission = admission;
        _hub = hub;
        _transferInitiator = transferInitiator;
        _transferTerminator = transferTerminator;
        _nodeRegistry = nodeRegistry;
        _stateStore = stateStore;
        _logger = logger;
        _options = options ?? new RunOrchestratorOptions();
    }

    public async Task<RunResult> RunAsync(RunSpec spec, CancellationToken ct = default)
    {
        if (!await _singleRun.WaitAsync(0, ct))
            throw new InvalidOperationException("A run is already in flight; concurrent runs are not supported.");

        try
        {
            _activeRunId = spec.RunId;
            _ready = spec.Fragments.ToDictionary(
                p => p.FragmentName, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
                StringComparer.Ordinal);
            _exited = spec.Fragments.ToDictionary(
                p => p.FragmentName, _ => new TaskCompletionSource<FragmentExitReport>(TaskCreationOptions.RunContinuationsAsynchronously),
                StringComparer.Ordinal);
            _remoteFailure = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var fragmentNames = spec.Fragments.Select(p => p.FragmentName).ToList();
            using var abort = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _abort = abort;
            string? lostFragment = null;
            string? wiringFailure = null;
            var launched = new HashSet<string>(StringComparer.Ordinal);
            var cancelledFragments = new HashSet<string>(StringComparer.Ordinal);
            var unresponsive = new HashSet<string>(StringComparer.Ordinal);
            var openTransfers = new List<string>();
            Dictionary<string, Guid>? clientIdByFragment = null;

            // Total edge participation per fragment (at most one inbound + one outbound -
            // PipelineNode.ValidateEdges), so a teardown can tell a fragment whose every declared
            // edge is already wired - it self-completes once its transfer is torn down, the same
            // path a mid-flow child death already takes - from one still waiting on a Wire it will
            // now never get, which needs an explicit Cancel to ever finish at all.
            var totalEdgesByFragment = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var edge in spec.Edges)
            {
                totalEdgesByFragment[edge.ProducerFragment] = totalEdgesByFragment.GetValueOrDefault(edge.ProducerFragment) + 1;
                totalEdgesByFragment[edge.ConsumerFragment] = totalEdgesByFragment.GetValueOrDefault(edge.ConsumerFragment) + 1;
            }
            var wiredEdgesByFragment = new Dictionary<string, int>(StringComparer.Ordinal);

            // Subscribed only once the run is admitted (below), so a fragment lost during admission
            // itself is never seen here: an unadmitted run is already AdmissionGate's own refusal,
            // naming the absentee, and must not also surface as a Failed run from this handler.
            void OnFragmentLost(string fragment, Guid clientId)
            {
                // Compared against the ClientId this run actually admitted, not just the fragment
                // name: a name reclaimed by a different node after admission (a restart) must not
                // be read as *this* run's own fragment going quiet, and must not be missed either -
                // NodeRegistry fires this same event for that case for exactly this reason.
                if (clientIdByFragment is null || !clientIdByFragment.TryGetValue(fragment, out var admittedClientId)
                    || admittedClientId != clientId)
                    return;
                if (_exited!.TryGetValue(fragment, out var tcs) && tcs.Task.IsCompleted) return;
                lostFragment ??= fragment;
                abort.Cancel();
            }

            IReadOnlyList<RegisteredNode> admitted;
            try
            {
                admitted = await _admission.AwaitAllAsync(spec.Fragments, _options.AdmissionTimeout, ct);
            }
            catch (OperationCanceledException)
            {
                return CancelledByRequester(spec.Edges);
            }
            // AdmissionRefusedException, MisalignedInstancesException and PinnedVersionUnavailableException
            // all propagate unchanged: nothing was launched yet.

            clientIdByFragment = admitted.ToDictionary(n => n.FragmentName, n => n.ClientId, StringComparer.Ordinal);
            _nodeRegistry.FragmentLost += OnFragmentLost;

            try
            {
                foreach (var pin in spec.Fragments)
                {
                    if (abort.IsCancellationRequested) break;
                    var fragment = pin.FragmentName;
                    var connectionId = await ResolveConnectionIdAsync(clientIdByFragment[fragment]);
                    if (connectionId is null) { lostFragment ??= fragment; abort.Cancel(); break; }
                    try { await _hub.Clients.Client(connectionId).SendAsync("Launch", spec.RunId, abort.Token); }
                    catch (OperationCanceledException) { break; }
                    launched.Add(fragment);
                }

                if (!abort.IsCancellationRequested)
                {
                    try
                    {
                        await Task.WhenAll(_ready.Values.Select(t => t.Task)).WaitAsync(_options.ReadyTimeout, abort.Token);
                    }
                    catch (TimeoutException)
                    {
                        lostFragment ??= spec.Fragments.Select(p => p.FragmentName).FirstOrDefault(f => !_ready[f].Task.IsCompleted);
                        abort.Cancel();
                    }
                    catch (OperationCanceledException) { /* ct, FragmentLost or a local failure already set abort */ }
                }

                if (!abort.IsCancellationRequested)
                {
                    foreach (var edge in spec.Edges)
                    {
                        if (abort.IsCancellationRequested) break;

                        var producerConnection = await ResolveConnectionIdAsync(clientIdByFragment[edge.ProducerFragment]);
                        var consumerConnection = await ResolveConnectionIdAsync(clientIdByFragment[edge.ConsumerFragment]);
                        if (producerConnection is null || consumerConnection is null)
                        {
                            lostFragment ??= producerConnection is null ? edge.ProducerFragment : edge.ConsumerFragment;
                            abort.Cancel();
                            break;
                        }

                        try
                        {
                            var transferId = await _transferInitiator.InitTransferAsync(
                                clientIdByFragment[edge.ProducerFragment], clientIdByFragment[edge.ConsumerFragment],
                                _options.BatchSize, _options.TransferTimeoutMs, abort.Token);
                            openTransfers.Add(transferId);

                            await _hub.Clients.Client(producerConnection).SendAsync("Wire", spec.RunId, edge.ProducerAlias, transferId, abort.Token);
                            await _hub.Clients.Client(consumerConnection).SendAsync("Wire", spec.RunId, edge.ConsumerAlias, transferId, abort.Token);

                            wiredEdgesByFragment[edge.ProducerFragment] = wiredEdgesByFragment.GetValueOrDefault(edge.ProducerFragment) + 1;
                            wiredEdgesByFragment[edge.ConsumerFragment] = wiredEdgesByFragment.GetValueOrDefault(edge.ConsumerFragment) + 1;
                        }
                        catch (OperationCanceledException) { break; }
                        catch (Exception ex)
                        {
                            // InitTransferAsync's documented failure modes (unknown client, expired
                            // token, unauthorized, unreachable, handshake timeout) name neither party
                            // in the exception itself, and neither fragment is known to have gone
                            // quiet: the edge is the cause. The verdict carries no reason beyond
                            // that (which side, which rule); the hub's log holds the detail.
                            _logger.LogWarning(ex, "Wiring edge {Producer}->{Consumer} failed", edge.ProducerFragment, edge.ConsumerFragment);
                            wiringFailure ??= $"transfer {edge.ProducerFragment} -> {edge.ConsumerFragment} could not be opened by the hub";
                            abort.Cancel();
                            break;
                        }
                    }
                }

                // Execution: nothing times a run in which no fragment has failed. It ends when every
                // fragment has reported, or by the abort a FragmentLost, a fragment's own local failure or
                // the requester's own ct raises - or, once a fragment has failed Remote, when its peers
                // have not reported within RemoteFailureGrace: the run is then torn down, and the verdict
                // names the fragments that had not reported, never the ones that failed because of them.
                if (!abort.IsCancellationRequested)
                {
                    try
                    {
                        var allExited = Task.WhenAll(_exited.Values.Select(t => t.Task));
                        var remoteFailure = _remoteFailure.Task;
                        if (await Task.WhenAny(allExited, remoteFailure).WaitAsync(abort.Token) == remoteFailure && !allExited.IsCompleted)
                        {
                            try { await allExited.WaitAsync(_options.RemoteFailureGrace, abort.Token); }
                            catch (TimeoutException)
                            {
                                var reported = fragmentNames.Where(f => _exited[f].Task.IsCompleted).ToHashSet(StringComparer.Ordinal);
                                var failedRemotely = fragmentNames
                                    .Where(f => _exited[f].Task.IsCompletedSuccessfully && _exited[f].Task.Result.Origin == FaultOrigin.Remote)
                                    .ToHashSet(StringComparer.Ordinal);
                                unresponsive.UnionWith(PeersOwingAReport(spec.Edges, failedRemotely, reported));
                                _logger.LogWarning(
                                    "Run {RunId}: {Fragments} did not report within {Grace} of a fragment failing remotely; tearing the run down",
                                    spec.RunId, string.Join(", ", fragmentNames.Where(f => !reported.Contains(f))), _options.RemoteFailureGrace);
                                abort.Cancel();
                            }
                        }
                        else
                        {
                            await allExited.WaitAsync(abort.Token);
                        }
                    }
                    catch (OperationCanceledException) { /* falls through to the teardown below */ }
                }

                if (abort.IsCancellationRequested)
                {
                    await TeardownAsync(
                        spec, lostFragment, clientIdByFragment, launched, openTransfers,
                        totalEdgesByFragment, wiredEdgesByFragment, cancelledFragments);
                }
            }
            finally
            {
                _nodeRegistry.FragmentLost -= OnFragmentLost;
            }

            var reports = fragmentNames
                .Where(f => _exited[f].Task.IsCompletedSuccessfully)
                .ToDictionary(f => f, f => _exited[f].Task.Result, StringComparer.Ordinal);

            return DetermineOutcome(reports, spec.Edges, fragmentNames, cancelledFragments, ct.IsCancellationRequested, wiringFailure, unresponsive);
        }
        finally
        {
            _activeRunId = null;
            _abort = null;
            _remoteFailure = null;
            _ready = null;
            _exited = null;
            _singleRun.Release();
        }
    }

    private static RunResult CancelledByRequester(IReadOnlyList<RunEdge> edges) =>
        new(RunOutcome.Cancelled, null, [], new Dictionary<string, FragmentExitReport>(), BuildEdgeCounts(new Dictionary<string, FragmentExitReport>(), edges));

    /// <summary>
    /// Terminates every transfer this run opened, tells every launched fragment still running to
    /// <c>Cancel</c>, then waits, bounded by <see cref="RunOrchestratorOptions.ExitTimeout"/>, for
    /// whichever of those it could actually reach to finish reporting. A fully wired fragment is told
    /// too - its torn-down transfer faults it only once a relay touches it, which a child with nothing
    /// to write never does - but it stays out of <paramref name="cancelledFragments"/>: it reports the
    /// <c>FaultOrigin.Remote</c> consequence a mid-flow peer death already produces.
    /// </summary>
    private async Task TeardownAsync(
        RunSpec spec, string? lostFragment, Dictionary<string, Guid> clientIdByFragment, HashSet<string> launched,
        List<string> openTransfers, Dictionary<string, int> totalEdgesByFragment,
        Dictionary<string, int> wiredEdgesByFragment, HashSet<string> cancelledFragments)
    {
        foreach (var transferId in openTransfers)
            await _transferTerminator.TerminateAsync(transferId);

        // A fragment resolved as unreachable here can never report Exited on its own - it is
        // unreachable precisely because nothing can tell it anything, itself included - checked
        // before the wiring test, since a fragment that was fully wired but has since gone silent
        // (the canonical pair muet case) is still unreachable and must be excluded the same way, not
        // just skipped for Cancel. The identified cause itself is excluded outright, even when its
        // own connection still resolves (a fragment that never became Ready is still connected): it
        // must stay silent for DetermineOutcome to name it, not be masked by a Cancel it happens to
        // still be reachable for.
        var unreachable = new HashSet<string>(StringComparer.Ordinal);
        if (lostFragment is not null) unreachable.Add(lostFragment);

        foreach (var fragment in launched)
        {
            if (fragment == lostFragment) continue;
            if (_exited![fragment].Task.IsCompleted) continue;

            var connectionId = await ResolveConnectionIdAsync(clientIdByFragment[fragment]);
            if (connectionId is null) { unreachable.Add(fragment); continue; }

            var isFullyWired = totalEdgesByFragment.TryGetValue(fragment, out var total) && total > 0
                && wiredEdgesByFragment.GetValueOrDefault(fragment) == total;
            if (!isFullyWired) cancelledFragments.Add(fragment);
            try { await _hub.Clients.Client(connectionId).SendAsync("Cancel", spec.RunId); }
            catch (Exception ex) { _logger.LogWarning(ex, "Cancel delivery failed for fragment {Fragment}", fragment); }
        }

        var pending = launched
            .Where(f => !unreachable.Contains(f) && !_exited![f].Task.IsCompleted)
            .Select(f => _exited![f].Task).ToArray();
        if (pending.Length > 0)
        {
            try { await Task.WhenAll(pending).WaitAsync(_options.ExitTimeout); }
            catch (TimeoutException) { /* still-missing fragments become the silent ones in DetermineOutcome */ }
        }
    }

    /// <summary>
    /// The connection id to send this run's control messages to, resolved fresh rather than carried
    /// from the admission snapshot: a reconnect gives a fragment a new SignalR ConnectionId, and
    /// TransportR's own state store already tracks the current one against the stable ClientId - the
    /// same lookup <c>InitTransferAsync</c> makes internally at every handshake. Null while the
    /// client is disconnected, whether or not it is still within its own grace period there: a
    /// connection marked disconnected is dead regardless of how long the record survives.
    /// </summary>
    private async Task<string?> ResolveConnectionIdAsync(Guid clientId)
    {
        var client = await _stateStore.GetClientAsync(clientId);
        return client is { DisconnectedAtUtc: null } ? client.ConnectionId : null;
    }

    public Task OnReadyAsync(string runId, string fragment)
    {
        if (runId != _activeRunId || _ready is null || !_ready.TryGetValue(fragment, out var tcs))
        {
            _logger.LogWarning("Ready from {Fragment} for run {RunId}, no such run in flight", fragment, runId);
            return Task.CompletedTask;
        }
        tcs.TrySetResult();
        return Task.CompletedTask;
    }

    public Task OnExitedAsync(
        string runId, string fragment, int exitCode, string origin, string? firstFault,
        IReadOnlyDictionary<string, long> rowCounts)
    {
        if (runId != _activeRunId || _exited is null || !_exited.TryGetValue(fragment, out var tcs))
        {
            _logger.LogWarning("Exited from {Fragment} for run {RunId}, no such run in flight", fragment, runId);
            return Task.CompletedTask;
        }

        // A fragment that exited 0 carries no origin: there was no fault to attribute.
        FaultOrigin? parsedOrigin = exitCode == 0 ? null : Enum.Parse<FaultOrigin>(origin);
        tcs.TrySetResult(new FragmentExitReport(fragment, exitCode, parsedOrigin, firstFault, rowCounts));

        // A fragment whose transfer failed under it is not the cause of anything yet: its peers owe the
        // run their own report, within RemoteFailureGrace.
        if (parsedOrigin == FaultOrigin.Remote) _remoteFailure?.TrySetResult();

        // A fragment whose own process failed has decided the run: the verdict is Failed whatever
        // its peers do next. Waiting for them to notice would leave a peer with nothing to write
        // (an aggregate over a slow source) holding the verdict until its source ends, since the
        // torn-down transfer never reaches a child that does not write. The teardown tells every
        // fragment still running to Cancel.
        if (parsedOrigin == FaultOrigin.Local)
        {
            try { _abort?.Cancel(); }
            catch (ObjectDisposedException) { /* the run already ended */ }
        }
        return Task.CompletedTask;
    }

    private static IReadOnlyList<EdgeCount> BuildEdgeCounts(
        IReadOnlyDictionary<string, FragmentExitReport> reports, IReadOnlyList<RunEdge> edges) =>
        edges.Select(e =>
        {
            long? sent = reports.TryGetValue(e.ProducerFragment, out var producer)
                && producer.RowCounts.TryGetValue(e.ProducerAlias, out var s) ? s : null;
            long? received = reports.TryGetValue(e.ConsumerFragment, out var consumer)
                && consumer.RowCounts.TryGetValue(e.ConsumerAlias, out var r) ? r : null;
            return new EdgeCount(e.ProducerFragment, e.ProducerAlias, e.ConsumerFragment, e.ConsumerAlias, sent, received);
        }).ToList();

    /// <summary>
    /// Pure. The fragments to name when the grace after a remote failure runs out: those that have not
    /// reported and share an edge with a fragment that failed <see cref="FaultOrigin.Remote"/>, the peers it
    /// was waiting on. A fragment silent only because its own peer is (the far end of a chain whose middle is
    /// stuck), or on a branch the failure never touched, is not named, though the teardown cancels it too.
    /// Empty when no such peer is left: the cause then follows the ordinary rule.
    /// </summary>
    internal static IReadOnlySet<string> PeersOwingAReport(
        IReadOnlyList<RunEdge> edges, IReadOnlySet<string> failedRemotely, IReadOnlySet<string> reported)
    {
        var owing = new HashSet<string>(StringComparer.Ordinal);
        foreach (var edge in edges)
        {
            if (failedRemotely.Contains(edge.ProducerFragment) && !reported.Contains(edge.ConsumerFragment))
                owing.Add(edge.ConsumerFragment);
            if (failedRemotely.Contains(edge.ConsumerFragment) && !reported.Contains(edge.ProducerFragment))
                owing.Add(edge.ProducerFragment);
        }
        return owing;
    }

    /// <summary>
    /// Pure. <paramref name="reports"/> is partial by construction - a fragment absent from it never
    /// reported <c>Exited</c>, whether it went silent or the coordinator gave up waiting on it after
    /// a teardown - so <paramref name="allFragments"/> is required to tell that apart from "every
    /// fragment reported 0". A fragment in <paramref name="coordinatorCancelled"/> is excluded from
    /// both cause and consequence: its own child was killed on the coordinator's own command, which
    /// otherwise reports as a plain <see cref="FaultOrigin.Local"/> failure indistinguishable from an
    /// organic one.
    ///
    /// Every fragment at 0 is not enough either: a fragment reporting 0 attests its own process, not
    /// that its peer received what it sent, so a run only succeeds when every declared edge's two row
    /// counts also agree. Otherwise the cause is the first fragment whose own failure has a
    /// <see cref="FaultOrigin.Local"/> origin, falling back to the first non-zero report when none
    /// does - never arrival order, since an aborting peer can fail a healthy fragment before the
    /// fragment that actually failed has finished reporting. An uncommanded first-local fault whose
    /// own exit code is 130 reports the whole run as cancelled (root <c>CLAUDE.md</c>'s convention),
    /// same as <paramref name="requesterCancelled"/> itself. A <paramref name="wiringFailure"/> - the hub
    /// could not open an edge's transfer - is the cause, as a message, unless a fragment went silent or
    /// one reports a local fault of its own. A fragment in <paramref name="unresponsive"/> - one that had not
    /// reported when <see cref="RunOrchestratorOptions.RemoteFailureGrace"/> ran out - counts as silent even
    /// when it reported afterwards, once told to cancel: that report is a consequence of the teardown, and
    /// the fragments that failed remotely because of it are its consequences, not the cause.
    /// </summary>
    internal static RunResult DetermineOutcome(
        IReadOnlyDictionary<string, FragmentExitReport> reports,
        IReadOnlyList<RunEdge> edges,
        IReadOnlyList<string> allFragments,
        IReadOnlySet<string> coordinatorCancelled,
        bool requesterCancelled,
        string? wiringFailure = null,
        IReadOnlySet<string>? unresponsive = null)
    {
        var edgeCounts = BuildEdgeCounts(reports, edges);

        var silent = allFragments
            .Where(f => (!reports.ContainsKey(f) || unresponsive?.Contains(f) == true) && !coordinatorCancelled.Contains(f))
            .ToList();
        var organicFaults = reports.Values.Where(r => r.ExitCode != 0 && !coordinatorCancelled.Contains(r.Fragment)).ToList();

        if (requesterCancelled)
        {
            var consequences = silent.Concat(organicFaults.Select(r => r.Fragment)).Distinct().ToList();
            return new RunResult(RunOutcome.Cancelled, null, consequences, reports, edgeCounts);
        }

        if (silent.Count > 0)
        {
            var cause = silent[0];
            var consequences = allFragments
                .Where(f => f != cause && (silent.Contains(f) || organicFaults.Any(r => r.Fragment == f)))
                .ToList();
            return new RunResult(RunOutcome.Failed, cause, consequences, reports, edgeCounts, CauseIsUnresponsive: true);
        }

        var localFault = organicFaults.FirstOrDefault(r => r.Origin == FaultOrigin.Local);

        if (wiringFailure is not null && localFault is null)
            return new RunResult(RunOutcome.Failed, wiringFailure, [], reports, edgeCounts);

        if (organicFaults.Count == 0)
        {
            var mismatchedEdges = edgeCounts.Where(e => !e.Agrees).ToList();
            if (mismatchedEdges.Count == 0)
                return new RunResult(RunOutcome.Succeeded, null, [], reports, edgeCounts);

            var edgeNames = mismatchedEdges.Select(e => $"{e.ProducerFragment}->{e.ConsumerFragment}");
            return new RunResult(RunOutcome.Failed, $"row count mismatch on edge(s): {string.Join(", ", edgeNames)}", [], reports, edgeCounts);
        }

        if (localFault is { ExitCode: 130 })
        {
            var consequences = organicFaults.Where(r => r.Fragment != localFault.Fragment).Select(r => r.Fragment).ToList();
            return new RunResult(RunOutcome.Cancelled, localFault.Fragment, consequences, reports, edgeCounts);
        }

        var faultCause = localFault ?? organicFaults[0];
        var faultConsequences = organicFaults.Where(r => r.Fragment != faultCause.Fragment).Select(r => r.Fragment).ToList();
        return new RunResult(RunOutcome.Failed, faultCause.Fragment, faultConsequences, reports, edgeCounts);
    }
}
