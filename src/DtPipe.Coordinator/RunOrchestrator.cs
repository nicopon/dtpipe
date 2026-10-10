using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;

namespace DtPipe.Coordinator;

/// <summary>Mirrors <c>DtPipe.PipelineNode.FaultOrigin</c> on the wire - the two projects never share a type.</summary>
public enum FaultOrigin { Local, Remote }

/// <summary>One flow from a producer fragment's outbound alias to a consumer fragment's inbound alias.</summary>
public sealed record RunEdge(string ProducerFragment, string ProducerAlias, string ConsumerFragment, string ConsumerAlias);

/// <summary>The fragments a run needs - each bound to the node that may host it, <see cref="RunFragment"/> - and the edges to wire once they are placed.</summary>
public sealed record RunSpec(string RunId, IReadOnlyList<RunFragment> Fragments, IReadOnlyList<RunEdge> Edges);

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
/// never true for the row-count-mismatch, refused-transfer or refused-launch forms of <see cref="Cause"/>,
/// which are messages, not fragment names. <see cref="Replicas"/> says which replica ran each fragment.
/// </summary>
public sealed record RunResult(
    RunOutcome Outcome, string? Cause, IReadOnlyList<string> Consequences,
    IReadOnlyDictionary<string, FragmentExitReport> Reports, IReadOnlyList<EdgeCount> EdgeCounts,
    bool CauseIsUnresponsive = false, IReadOnlyDictionary<string, RunReplica>? Replicas = null)
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

/// <summary>The replica a fragment ran on: the start of its instance id, and where it says it runs (untrusted text).</summary>
public sealed record RunReplica(string Id, string? Origin);

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
    /// (<see cref="ReplicaRegistry.ReplicaLost"/>) and the requester's cancellation each end the run on
    /// their own, so this is only the last bound on a run that nothing else ends: a peer that stopped
    /// answering without its node dropping, such as a consumer blocked on its sink. Keep it above
    /// <see cref="ReplicaRegistryOptions.DisconnectGracePeriod"/>, or a node that is merely reconnecting
    /// is named by its silence instead of by its loss. A run in which no fragment has failed
    /// <see cref="FaultOrigin.Remote"/> has no clock at all: a legitimately long transfer, or a slow
    /// consumer, is never reported failed for taking time. <see cref="Timeout.InfiniteTimeSpan"/>
    /// removes the bound.
    /// </summary>
    public TimeSpan RemoteFailureGrace { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>Bounds each <c>Wire</c> and <c>Cancel</c> the coordinator sends a replica: one that does not answer is unreachable.</summary>
    public TimeSpan LinkTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>How often admission looks again for a replica that can run every fragment, until <see cref="AdmissionTimeout"/>.</summary>
    public TimeSpan AdmissionPollInterval { get; init; } = TimeSpan.FromMilliseconds(50);

    public int BatchSize { get; init; } = 8;
    public int TransferTimeoutMs { get; init; } = 20_000;
}

public interface IRunOrchestrator
{
    Task<RunResult> RunAsync(RunSpec spec, CancellationToken ct = default);

    /// <summary>
    /// A replica reports how a fragment ended. Heard only from the replica the run placed that fragment on:
    /// anything else, including a replica of the same node, is <see cref="ExitedAck.NotAdmitted"/>.
    /// </summary>
    Task<ExitedAck> OnExitedAsync(ReplicaKey replica, FragmentExit exit);
}

/// <summary>
/// The barrier. Places every fragment of a run on a replica that can run it (<see cref="ReplicaAdmission"/>), launches
/// them all, opens each declared edge once the fragments are launched, then waits out the data-transfer phase and applies
/// the outcome rule (<see cref="DetermineOutcome"/>). That phase ends when every fragment has reported, or is given up
/// on: the replica a fragment runs on is lost (<see cref="ReplicaRegistry.ReplicaLost"/>), a fragment's own local
/// failure, the caller's cancellation, or - once a fragment has failed <see cref="FaultOrigin.Remote"/> -
/// <see cref="RunOrchestratorOptions.RemoteFailureGrace"/> for its peers to report. A run in which nothing has failed has
/// no clock. Several runs may be in flight at once, even of the same fragments: each has its own state, found by run id,
/// and everything that ends a run (a lost replica, a local failure, a cancellation, the grace) ends that run alone.
/// </summary>
public sealed class RunOrchestrator : IRunOrchestrator
{
    private readonly ReplicaAdmission _admission;
    private readonly ReplicaRegistry _registry;
    private readonly INodeLink _link;
    private readonly ITransferInitiator _transferInitiator;
    private readonly ITransferTerminator _transferTerminator;
    private readonly RunOrchestratorOptions _options;
    private readonly ILogger<RunOrchestrator> _logger;

    /// <summary>One run's state, built before its admission and dropped from the map before its abort is disposed.</summary>
    private sealed class ActiveRun
    {
        public required RunSpec Spec { get; init; }
        public required Dictionary<string, TaskCompletionSource<FragmentExitReport>> Exited { get; init; }
        public required CancellationTokenSource Abort { get; init; }
        public TaskCompletionSource RemoteFailure { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The replica each fragment was placed on, set once admission has chosen; null before.</summary>
        public volatile IReadOnlyDictionary<string, ReplicaKey>? Placed;
    }

    private readonly object _lock = new();
    private readonly Dictionary<string, ActiveRun> _runs = new(StringComparer.Ordinal);

    public RunOrchestrator(
        ReplicaAdmission admission,
        ReplicaRegistry registry,
        INodeLink link,
        ITransferInitiator transferInitiator,
        ITransferTerminator transferTerminator,
        ILogger<RunOrchestrator> logger,
        RunOrchestratorOptions? options = null)
    {
        _admission = admission;
        _registry = registry;
        _link = link;
        _transferInitiator = transferInitiator;
        _transferTerminator = transferTerminator;
        _logger = logger;
        _options = options ?? new RunOrchestratorOptions();
    }

    public async Task<RunResult> RunAsync(RunSpec spec, CancellationToken ct = default)
    {
        var run = Begin(spec, ct);
        try
        {
            var abort = run.Abort;
            var fragmentNames = spec.Fragments.Select(f => f.Name).ToList();
            string? lostFragment = null;
            string? wiringFailure = null;
            var launched = new HashSet<string>(StringComparer.Ordinal);
            var refused = new HashSet<string>(StringComparer.Ordinal);
            var cancelledFragments = new HashSet<string>(StringComparer.Ordinal);
            var unresponsive = new HashSet<string>(StringComparer.Ordinal);
            var openTransfers = new List<string>();

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

            IReadOnlyDictionary<string, ReplicaView> chosen;
            try
            {
                chosen = await AdmitAsync(spec, ct);
            }
            catch (OperationCanceledException)
            {
                return CancelledByRequester(spec.Edges);
            }
            // UnboundFragmentException and NoEligibleReplicaException propagate unchanged: nothing was launched yet.

            var placed = chosen.ToDictionary(c => c.Key, c => c.Value.Key, StringComparer.Ordinal);
            run.Placed = placed;

            // Subscribed only once the run is placed, so a replica lost during admission itself is never seen
            // here: an unplaced run is already admission's own refusal and must not also surface as a Failed
            // run from this handler. Compared against the replica each fragment was placed on, so a replica
            // of the same node dropping, or a replacement of this one, is never read as this run's own.
            void OnReplicaLost(ReplicaKey lost)
            {
                var gone = spec.Fragments.Select(f => f.Name)
                    .Where(f => placed[f] == lost && !run.Exited[f].Task.IsCompleted).ToList();
                if (gone.Count == 0) return;
                lostFragment ??= gone[0];
                // CancelAsync, not Cancel: the teardown this wakes must not run inline, in the hub method of
                // the replica that said it was leaving, which serialises everything that replica sends.
                try { _ = run.Abort.CancelAsync(); }
                catch (ObjectDisposedException) { /* the run already ended */ }
            }
            _registry.ReplicaLost += OnReplicaLost;

            try
            {
                if (!abort.IsCancellationRequested)
                {
                    using var launchBound = CancellationTokenSource.CreateLinkedTokenSource(abort.Token);
                    launchBound.CancelAfter(_options.ReadyTimeout);
                    var attempts = await Task.WhenAll(spec.Fragments.Select(f => LaunchOneAsync(run, f, placed[f.Name], launched, launchBound.Token)));
                    foreach (var attempt in attempts)
                    {
                        switch (attempt.Kind)
                        {
                            case LaunchKind.Started:
                                break;
                            case LaunchKind.Refused:
                                // The replica ran nothing: the coordinator's own teardown excludes the fragment from
                                // cause and consequence, and the reason is the cause.
                                refused.Add(attempt.Fragment);
                                cancelledFragments.Add(attempt.Fragment);
                                wiringFailure ??= $"{attempt.Fragment} could not start on replica {placed[attempt.Fragment].Short}: {attempt.Reason}";
                                abort.Cancel();
                                break;
                            case LaunchKind.Silent when !abort.IsCancellationRequested:
                                // No answer within ReadyTimeout, or no connection to ask on: the fragment went quiet.
                                lostFragment ??= attempt.Fragment;
                                abort.Cancel();
                                break;
                        }
                    }
                }

                if (!abort.IsCancellationRequested)
                {
                    foreach (var edge in spec.Edges)
                    {
                        if (abort.IsCancellationRequested) break;

                        try
                        {
                            var producer = chosen[edge.ProducerFragment];
                            var consumer = chosen[edge.ConsumerFragment];
                            var transferId = await _transferInitiator.InitTransferAsync(
                                producer.DataPlaneClientId!.Value, consumer.DataPlaneClientId!.Value,
                                _options.BatchSize, _options.TransferTimeoutMs, abort.Token);
                            openTransfers.Add(transferId);

                            await WireAsync(placed[edge.ProducerFragment], spec.RunId, edge.ProducerFragment, edge.ProducerAlias, transferId, abort.Token);
                            await WireAsync(placed[edge.ConsumerFragment], spec.RunId, edge.ConsumerFragment, edge.ConsumerAlias, transferId, abort.Token);

                            wiredEdgesByFragment[edge.ProducerFragment] = wiredEdgesByFragment.GetValueOrDefault(edge.ProducerFragment) + 1;
                            wiredEdgesByFragment[edge.ConsumerFragment] = wiredEdgesByFragment.GetValueOrDefault(edge.ConsumerFragment) + 1;
                        }
                        catch (OperationCanceledException) when (abort.IsCancellationRequested) { break; }
                        catch (ReplicaUnreachableException ex)
                        {
                            lostFragment ??= fragmentNames.First(f => placed[f] == ex.Replica);
                            abort.Cancel();
                            break;
                        }
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
                // fragment has reported, or by the abort a lost replica, a fragment's own local failure or
                // the requester's own ct raises - or, once a fragment has failed Remote, when its peers
                // have not reported within RemoteFailureGrace: the run is then torn down, and the verdict
                // names the fragments that had not reported, never the ones that failed because of them.
                if (!abort.IsCancellationRequested)
                {
                    try
                    {
                        var allExited = Task.WhenAll(run.Exited.Values.Select(t => t.Task));
                        var remoteFailure = run.RemoteFailure.Task;
                        if (await Task.WhenAny(allExited, remoteFailure).WaitAsync(abort.Token) == remoteFailure && !allExited.IsCompleted)
                        {
                            try { await allExited.WaitAsync(_options.RemoteFailureGrace, abort.Token); }
                            catch (TimeoutException)
                            {
                                var reported = fragmentNames.Where(f => run.Exited[f].Task.IsCompleted).ToHashSet(StringComparer.Ordinal);
                                var failedRemotely = fragmentNames
                                    .Where(f => run.Exited[f].Task.IsCompletedSuccessfully && run.Exited[f].Task.Result.Origin == FaultOrigin.Remote)
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
                        run, lostFragment, placed, launched, refused, openTransfers,
                        totalEdgesByFragment, wiredEdgesByFragment, cancelledFragments);
                }
            }
            finally
            {
                _registry.ReplicaLost -= OnReplicaLost;
            }

            var reports = fragmentNames
                .Where(f => run.Exited[f].Task.IsCompletedSuccessfully)
                .ToDictionary(f => f, f => run.Exited[f].Task.Result, StringComparer.Ordinal);

            return DetermineOutcome(reports, spec.Edges, fragmentNames, cancelledFragments, ct.IsCancellationRequested, wiringFailure, unresponsive)
                with { Replicas = chosen.ToDictionary(c => c.Key, c => new RunReplica(c.Value.Key.Short, c.Value.Announcement.Origin), StringComparer.Ordinal) };
        }
        finally
        {
            End(run);
        }
    }

    /// <summary>
    /// Waits, up to <see cref="RunOrchestratorOptions.AdmissionTimeout"/>, for a replica of every fragment's node that
    /// can run it, and places the whole run in one step. Gives up with the reason for each fragment that never had one.
    /// </summary>
    private async Task<IReadOnlyDictionary<string, ReplicaView>> AdmitAsync(RunSpec spec, CancellationToken ct)
    {
        _admission.ThrowIfUnbound(spec.Fragments);

        using var timeoutCts = new CancellationTokenSource(_options.AdmissionTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
        IReadOnlyDictionary<string, string> reasons;
        while (true)
        {
            var (chosen, why) = await _admission.TryReserveAsync(spec.RunId, spec.Fragments);
            if (chosen is not null) return chosen;
            reasons = why;

            ct.ThrowIfCancellationRequested();
            if (timeoutCts.IsCancellationRequested)
                throw new NoEligibleReplicaException(reasons.Count > 0 ? reasons
                    : spec.Fragments.ToDictionary(f => f.Name, _ => "no replica could be placed.", StringComparer.Ordinal));

            try { await Task.Delay(_options.AdmissionPollInterval, linked.Token); }
            catch (OperationCanceledException) { /* loop once more to tell which side fired above */ }
        }
    }

    private enum LaunchKind { Started, Refused, Silent }

    private sealed record LaunchAttempt(string Fragment, LaunchKind Kind, string? Reason = null);

    /// <summary>
    /// Sends one <c>Launch</c> and says how it was received. Never throws: a refusal is the replica's answer; an
    /// unreachable replica, or one that did not answer within the bound, is a fragment gone quiet. The fragment is in
    /// <paramref name="launched"/> from before the message is sent, so a teardown tells it to <c>Cancel</c> whatever happened.
    /// </summary>
    private async Task<LaunchAttempt> LaunchOneAsync(
        ActiveRun run, RunFragment fragment, ReplicaKey replica, HashSet<string> launched, CancellationToken bound)
    {
        lock (launched) launched.Add(fragment.Name);
        try
        {
            var result = await _link.LaunchAsync(replica, new LaunchRequest(run.Spec.RunId, fragment.Spec), bound);
            return result.Started ? new LaunchAttempt(fragment.Name, LaunchKind.Started)
                : new LaunchAttempt(fragment.Name, LaunchKind.Refused, result.Refusal ?? "the replica gave no reason.");
        }
        // Whatever the cancellation of the message looks like on the wire, it is not an answer of the replica.
        catch (Exception) when (bound.IsCancellationRequested) { return new LaunchAttempt(fragment.Name, LaunchKind.Silent); }
        catch (ReplicaUnreachableException) { return new LaunchAttempt(fragment.Name, LaunchKind.Silent); }
        catch (HubException ex) { return new LaunchAttempt(fragment.Name, LaunchKind.Refused, ex.Message); }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Launch of {Fragment} on {Replica} failed", fragment.Name, replica);
            return new LaunchAttempt(fragment.Name, LaunchKind.Silent);
        }
    }

    /// <summary>Hands one end of a transfer to the replica that runs the fragment; a replica that does not know the run or the edge is a failure of the edge.</summary>
    private async Task WireAsync(ReplicaKey replica, string runId, string fragment, string alias, string transferId, CancellationToken abort)
    {
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(abort);
        bound.CancelAfter(_options.LinkTimeout);
        if (!await _link.WireAsync(replica, runId, fragment, alias, transferId, bound.Token))
            throw new InvalidOperationException($"{replica} knows no edge '{alias}' of {fragment} in run {runId}.");
    }

    /// <summary>
    /// Claims the run's id in one step, so two runs of one id never both pass. Several runs of the same fragments
    /// may be in flight: which may overlap is the caller's to decide before it asks.
    /// </summary>
    private ActiveRun Begin(RunSpec spec, CancellationToken ct)
    {
        lock (_lock)
        {
            if (_runs.ContainsKey(spec.RunId))
                throw new ArgumentException($"A run named '{spec.RunId}' is already in flight.", nameof(spec));

            var run = new ActiveRun
            {
                Spec = spec,
                Exited = spec.Fragments.ToDictionary(
                    f => f.Name, _ => new TaskCompletionSource<FragmentExitReport>(TaskCreationOptions.RunContinuationsAsynchronously),
                    StringComparer.Ordinal),
                Abort = CancellationTokenSource.CreateLinkedTokenSource(ct),
            };
            _runs[spec.RunId] = run;
            return run;
        }
    }

    private void End(ActiveRun run)
    {
        lock (_lock) _runs.Remove(run.Spec.RunId);
        _registry.Release(run.Spec.RunId);
        run.Abort.Dispose();
    }

    private ActiveRun? Find(string runId)
    {
        lock (_lock) return _runs.GetValueOrDefault(runId);
    }

    private static RunResult CancelledByRequester(IReadOnlyList<RunEdge> edges) =>
        new(RunOutcome.Cancelled, null, [], new Dictionary<string, FragmentExitReport>(), BuildEdgeCounts(new Dictionary<string, FragmentExitReport>(), edges));

    /// <summary>
    /// Terminates every transfer this run opened, tells every launched fragment still running to
    /// <c>Cancel</c>, then waits, bounded by <see cref="RunOrchestratorOptions.ExitTimeout"/>, for
    /// whichever of those it could actually reach to finish reporting. A fully wired fragment is told
    /// too - its torn-down transfer faults it only once a relay touches it, which a child with nothing
    /// to write never does - but it stays out of <paramref name="cancelledFragments"/>: it reports the
    /// <c>FaultOrigin.Remote</c> consequence a mid-flow peer death already produces. A fragment whose replica
    /// refused the launch ran nothing, and a replica that knows no such run has nothing to report: neither is waited for.
    /// </summary>
    private async Task TeardownAsync(
        ActiveRun run, string? lostFragment, IReadOnlyDictionary<string, ReplicaKey> placed, HashSet<string> launched, HashSet<string> refused,
        List<string> openTransfers, Dictionary<string, int> totalEdgesByFragment,
        Dictionary<string, int> wiredEdgesByFragment, HashSet<string> cancelledFragments)
    {
        foreach (var transferId in openTransfers)
            await _transferTerminator.TerminateAsync(transferId);

        // A fragment unreachable here can never report Exited on its own - it is unreachable precisely
        // because nothing can tell it anything, itself included - and a fragment that was fully wired but
        // has since gone silent (the canonical pair muet case) is still unreachable and must be excluded the
        // same way, not just skipped for Cancel. The identified cause itself is excluded outright, even when
        // its own replica still answers (a fragment that never became ready is still connected): it must stay
        // silent for DetermineOutcome to name it, not be masked by a Cancel it happens to still receive.
        var unreachable = new HashSet<string>(StringComparer.Ordinal);
        if (lostFragment is not null) unreachable.Add(lostFragment);
        var knowsNothing = new HashSet<string>(StringComparer.Ordinal);

        async Task CancelOneAsync(string fragment)
        {
            var isFullyWired = totalEdgesByFragment.TryGetValue(fragment, out var total) && total > 0
                && wiredEdgesByFragment.GetValueOrDefault(fragment) == total;
            try
            {
                using var bound = new CancellationTokenSource(_options.LinkTimeout);
                var known = await _link.CancelAsync(placed[fragment], run.Spec.RunId, fragment, bound.Token);
                lock (cancelledFragments)
                {
                    if (!isFullyWired) cancelledFragments.Add(fragment);
                    if (!known) knowsNothing.Add(fragment);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Cancel delivery failed for fragment {Fragment}", fragment);
                lock (cancelledFragments) unreachable.Add(fragment);
            }
        }

        string[] toCancel;
        lock (launched)
            toCancel = launched.Where(f => f != lostFragment && !refused.Contains(f) && !run.Exited[f].Task.IsCompleted).ToArray();
        await Task.WhenAll(toCancel.Select(CancelOneAsync));

        var pending = toCancel
            .Where(f => !unreachable.Contains(f) && !knowsNothing.Contains(f) && !run.Exited[f].Task.IsCompleted)
            .Select(f => run.Exited[f].Task).ToArray();
        if (pending.Length > 0)
        {
            try { await Task.WhenAll(pending).WaitAsync(_options.ExitTimeout); }
            catch (TimeoutException) { /* still-missing fragments become the silent ones in DetermineOutcome */ }
        }
    }

    public Task<ExitedAck> OnExitedAsync(ReplicaKey replica, FragmentExit exit)
    {
        // The fragment name an exit cites finds the fragment; only the replica the run placed it on may report it.
        if (Find(exit.RunId) is not { } run || run.Placed is not { } placed
            || !placed.TryGetValue(exit.Fragment, out var admitted) || admitted != replica
            || !run.Exited.TryGetValue(exit.Fragment, out var tcs))
        {
            _logger.LogWarning("Exited from {Replica} for {Fragment} of run {RunId}, not a replica that run placed it on", replica, exit.Fragment, exit.RunId);
            return Task.FromResult(ExitedAck.NotAdmitted);
        }

        // A fragment that exited 0 carries no origin: there was no fault to attribute.
        FaultOrigin? parsedOrigin = exit.ExitCode == 0 ? null : Enum.TryParse<FaultOrigin>(exit.Origin, out var origin) ? origin : FaultOrigin.Local;
        tcs.TrySetResult(new FragmentExitReport(exit.Fragment, exit.ExitCode, parsedOrigin, exit.FirstFault, exit.RowCounts));

        // A fragment whose transfer failed under it is not the cause of anything yet: its peers owe the
        // run their own report, within RemoteFailureGrace.
        if (parsedOrigin == FaultOrigin.Remote) run.RemoteFailure.TrySetResult();

        // A fragment whose own process failed has decided the run: the verdict is Failed whatever
        // its peers do next. Waiting for them to notice would leave a peer with nothing to write
        // (an aggregate over a slow source) holding the verdict until its source ends, since the
        // torn-down transfer never reaches a child that does not write. The teardown tells every
        // fragment of this run still running to Cancel; another run in flight is untouched. CancelAsync,
        // so the teardown does not run inline in the hub method that carried this report.
        if (parsedOrigin == FaultOrigin.Local)
        {
            try { _ = run.Abort.CancelAsync(); }
            catch (ObjectDisposedException) { /* the run already ended */ }
        }
        return Task.FromResult(ExitedAck.Accepted);
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
