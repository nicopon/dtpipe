using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;

namespace DtPipe.Coordinator;

/// <summary>What every run of one orchestrator shares: the collaborators its phases ask.</summary>
internal sealed record RunServices(
    ReplicaAdmission Admission,
    ReplicaRegistry Registry,
    INodeLink Link,
    ITransferInitiator TransferInitiator,
    ITransferTerminator TransferTerminator,
    RunOrchestratorOptions Options,
    ILogger Logger);

/// <summary>
/// One run, from its admission to its verdict: the state its phases share, and the phases. The orchestrator builds it
/// before the admission and drops it from its table before <see cref="Abort"/> is disposed. Two entries run on the thread
/// of a hub method: the orchestrator's <c>OnExitedAsync</c>, which reaches <see cref="Exited"/>, <see cref="RemoteFailure"/>,
/// <see cref="Abort"/> and <see cref="Placed"/>, and the handler of <c>ReplicaLost</c>, subscribed once the run is placed, which
/// reads the placement and <see cref="Exited"/>, notes the lost fragment and raises <see cref="Abort"/>. <see cref="ExecuteAsync"/>
/// runs the phases in order, each only while the run has not been aborted: launch, wiring and the wait for every report;
/// an aborted run is torn down, and what the phases noted decides the verdict.
/// </summary>
internal sealed class RunSession
{
    private readonly RunServices _services;
    private readonly List<string> _fragmentNames;

    public RunSession(RunServices services, RunSpec spec, CancellationToken ct)
    {
        _services = services;
        Spec = spec;
        Exited = spec.Fragments.ToDictionary(
            f => f.Name, _ => new TaskCompletionSource<FragmentExitReport>(TaskCreationOptions.RunContinuationsAsynchronously),
            StringComparer.Ordinal);
        Abort = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _fragmentNames = spec.Fragments.Select(f => f.Name).ToList();
    }

    public RunSpec Spec { get; }
    public Dictionary<string, TaskCompletionSource<FragmentExitReport>> Exited { get; }
    public CancellationTokenSource Abort { get; }
    public TaskCompletionSource RemoteFailure { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Where each fragment was placed and under which launch token, set once admission has chosen; null before.</summary>
    public volatile RunOrchestrator.Placement? Placed;

    private IReadOnlyDictionary<string, ReplicaView> _chosen = null!;
    private Dictionary<string, ReplicaKey> _placed = null!;
    private Dictionary<string, string> _tokens = null!;

    private string? _lostFragment;
    private string? _wiringFailure;
    private readonly HashSet<string> _launched = new(StringComparer.Ordinal);
    private readonly HashSet<string> _refused = new(StringComparer.Ordinal);
    private readonly HashSet<string> _cancelledFragments = new(StringComparer.Ordinal);
    private readonly HashSet<string> _unresponsive = new(StringComparer.Ordinal);
    private readonly List<string> _openTransfers = [];

    // Total edge participation per fragment (at most one inbound + one outbound -
    // PipelineNode.ValidateEdges), so a teardown can tell a fragment whose every declared
    // edge is already wired - it self-completes once its transfer is torn down, the same
    // path a mid-flow child death already takes - from one still waiting on a Wire it will
    // now never get, which needs an explicit Cancel to ever finish at all.
    private readonly Dictionary<string, int> _totalEdgesByFragment = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _wiredEdgesByFragment = new(StringComparer.Ordinal);

    public async Task<RunResult> ExecuteAsync(CancellationToken ct)
    {
        foreach (var edge in Spec.Edges)
        {
            _totalEdgesByFragment[edge.ProducerFragment] = _totalEdgesByFragment.GetValueOrDefault(edge.ProducerFragment) + 1;
            _totalEdgesByFragment[edge.ConsumerFragment] = _totalEdgesByFragment.GetValueOrDefault(edge.ConsumerFragment) + 1;
        }

        try
        {
            _chosen = await AdmitAsync(ct);
        }
        catch (OperationCanceledException)
        {
            return RunOrchestrator.CancelledByRequester(Spec.Edges);
        }
        // UnboundFragmentException and NoEligibleReplicaException propagate unchanged: nothing was launched yet.

        Place();

        // Subscribed only once the run is placed, so a replica lost during admission itself is never seen
        // here: an unplaced run is already admission's own refusal and must not also surface as a Failed
        // run from this handler. Compared against the replica each fragment was placed on, so a replica
        // of the same node dropping, or a replacement of this one, is never read as this run's own.
        _services.Registry.ReplicaLost += OnReplicaLost;

        try
        {
            if (!Abort.IsCancellationRequested) await LaunchAsync();
            if (!Abort.IsCancellationRequested) await WireEdgesAsync();

            // Execution: nothing times a run in which no fragment has failed. It ends when every
            // fragment has reported, or by the abort a lost replica, a fragment's own local failure or
            // the requester's own ct raises - or, once a fragment has failed Remote, when its peers
            // have not reported within RemoteFailureGrace: the run is then torn down, and the verdict
            // names the fragments that had not reported, never the ones that failed because of them.
            if (!Abort.IsCancellationRequested) await AwaitReportsAsync();

            if (Abort.IsCancellationRequested) await TeardownAsync();
        }
        finally
        {
            _services.Registry.ReplicaLost -= OnReplicaLost;
        }

        return Conclude(ct);
    }

    /// <summary>
    /// Waits, up to <see cref="RunOrchestratorOptions.AdmissionTimeout"/>, for a replica of every fragment's node that
    /// can run it, and places the whole run in one step. Gives up with the reason for each fragment that never had one.
    /// </summary>
    private async Task<IReadOnlyDictionary<string, ReplicaView>> AdmitAsync(CancellationToken ct)
    {
        _services.Admission.ThrowIfUnbound(Spec.Fragments);

        using var timeoutCts = new CancellationTokenSource(_services.Options.AdmissionTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);
        IReadOnlyDictionary<string, string> reasons;
        while (true)
        {
            var (chosen, why) = await _services.Admission.TryReserveAsync(Spec.RunId, Spec.Fragments);
            if (chosen is not null) return chosen;
            reasons = why;

            ct.ThrowIfCancellationRequested();
            if (timeoutCts.IsCancellationRequested)
                throw new NoEligibleReplicaException(reasons.Count > 0 ? reasons
                    : Spec.Fragments.ToDictionary(f => f.Name, _ => "no replica could be placed.", StringComparer.Ordinal));

            try { await Task.Delay(_services.Options.AdmissionPollInterval, linked.Token); }
            catch (OperationCanceledException) { /* loop once more to tell which side fired above */ }
        }
    }

    /// <summary>Publishes where each fragment runs, and under which launch token, in one assignment before the first <c>Launch</c> is sent.</summary>
    private void Place()
    {
        _placed = _chosen.ToDictionary(c => c.Key, c => c.Value.Key, StringComparer.Ordinal);
        // Random, minted for this launch alone: nothing that restarts with the process, or an old child's report would match.
        _tokens = _chosen.ToDictionary(c => c.Key, _ => Guid.NewGuid().ToString("N"), StringComparer.Ordinal);
        Placed = new RunOrchestrator.Placement(_placed, _tokens);
    }

    private void OnReplicaLost(ReplicaKey lost)
    {
        var gone = Spec.Fragments.Select(f => f.Name)
            .Where(f => _placed[f] == lost && !Exited[f].Task.IsCompleted).ToList();
        if (gone.Count == 0) return;
        _lostFragment ??= gone[0];
        // CancelAsync, not Cancel: the teardown this wakes must not run inline, in the hub method of
        // the replica that said it was leaving, which serialises everything that replica sends.
        try { _ = Abort.CancelAsync(); }
        catch (ObjectDisposedException) { /* the run already ended */ }
    }

    private async Task LaunchAsync()
    {
        using var launchBound = CancellationTokenSource.CreateLinkedTokenSource(Abort.Token);
        launchBound.CancelAfter(_services.Options.ReadyTimeout);
        var attempts = await Task.WhenAll(Spec.Fragments.Select(f => LaunchOneAsync(f, _placed[f.Name], _tokens[f.Name], launchBound.Token)));
        foreach (var attempt in attempts)
        {
            switch (attempt.Kind)
            {
                case LaunchKind.Started:
                    break;
                case LaunchKind.Refused:
                    // The replica ran nothing: the coordinator's own teardown excludes the fragment from
                    // cause and consequence, and the reason is the cause.
                    _refused.Add(attempt.Fragment);
                    _cancelledFragments.Add(attempt.Fragment);
                    _wiringFailure ??= $"{attempt.Fragment} could not start on replica {_placed[attempt.Fragment].Short}: {attempt.Reason}";
                    Abort.Cancel();
                    break;
                case LaunchKind.Silent when !Abort.IsCancellationRequested:
                    // No answer within ReadyTimeout, or no connection to ask on: the fragment went quiet.
                    _lostFragment ??= attempt.Fragment;
                    Abort.Cancel();
                    break;
            }
        }
    }

    private enum LaunchKind { Started, Refused, Silent }

    private sealed record LaunchAttempt(string Fragment, LaunchKind Kind, string? Reason = null);

    /// <summary>
    /// Sends one <c>Launch</c> and says how it was received. Never throws: a refusal is the replica's answer; an
    /// unreachable replica, or one that did not answer within the bound, is a fragment gone quiet. The fragment is in
    /// <see cref="_launched"/> from before the message is sent, so a teardown tells it to <c>Cancel</c> whatever happened.
    /// </summary>
    private async Task<LaunchAttempt> LaunchOneAsync(RunFragment fragment, ReplicaKey replica, string token, CancellationToken bound)
    {
        lock (_launched) _launched.Add(fragment.Name);
        try
        {
            var result = await _services.Link.LaunchAsync(replica, new LaunchRequest(Spec.RunId, fragment.Spec, token, fragment.Cursor), bound);
            return result.Started ? new LaunchAttempt(fragment.Name, LaunchKind.Started)
                : new LaunchAttempt(fragment.Name, LaunchKind.Refused, result.Refusal ?? "the replica gave no reason.");
        }
        // Whatever the cancellation of the message looks like on the wire, it is not an answer of the replica.
        catch (Exception) when (bound.IsCancellationRequested) { return new LaunchAttempt(fragment.Name, LaunchKind.Silent); }
        catch (ReplicaUnreachableException) { return new LaunchAttempt(fragment.Name, LaunchKind.Silent); }
        catch (HubException ex) { return new LaunchAttempt(fragment.Name, LaunchKind.Refused, ex.Message); }
        catch (Exception ex)
        {
            _services.Logger.LogWarning(ex, "Launch of {Fragment} on {Replica} failed", fragment.Name, replica);
            return new LaunchAttempt(fragment.Name, LaunchKind.Silent);
        }
    }

    /// <summary>Opens each declared edge and hands both ends their transfer, stopping at the first failure.</summary>
    private async Task WireEdgesAsync()
    {
        foreach (var edge in Spec.Edges)
        {
            if (Abort.IsCancellationRequested) break;

            try
            {
                var producer = _chosen[edge.ProducerFragment];
                var consumer = _chosen[edge.ConsumerFragment];
                var transferId = await _services.TransferInitiator.InitTransferAsync(
                    producer.DataPlaneClientId!.Value, consumer.DataPlaneClientId!.Value,
                    _services.Options.BatchSize, _services.Options.TransferTimeoutMs, Abort.Token);
                _openTransfers.Add(transferId);

                await WireAsync(_placed[edge.ProducerFragment], edge.ProducerFragment, edge.ProducerAlias, transferId);
                await WireAsync(_placed[edge.ConsumerFragment], edge.ConsumerFragment, edge.ConsumerAlias, transferId);

                _wiredEdgesByFragment[edge.ProducerFragment] = _wiredEdgesByFragment.GetValueOrDefault(edge.ProducerFragment) + 1;
                _wiredEdgesByFragment[edge.ConsumerFragment] = _wiredEdgesByFragment.GetValueOrDefault(edge.ConsumerFragment) + 1;
            }
            catch (OperationCanceledException) when (Abort.IsCancellationRequested) { break; }
            catch (ReplicaUnreachableException ex)
            {
                _lostFragment ??= _fragmentNames.First(f => _placed[f] == ex.Replica);
                Abort.Cancel();
                break;
            }
            catch (Exception ex)
            {
                // InitTransferAsync's documented failure modes (unknown client, expired
                // token, unauthorized, unreachable, handshake timeout) name neither party
                // in the exception itself, and neither fragment is known to have gone
                // quiet: the edge is the cause. The verdict carries no reason beyond
                // that (which side, which rule); the hub's log holds the detail.
                _services.Logger.LogWarning(ex, "Wiring edge {Producer}->{Consumer} failed", edge.ProducerFragment, edge.ConsumerFragment);
                _wiringFailure ??= $"transfer {edge.ProducerFragment} -> {edge.ConsumerFragment} could not be opened by the hub";
                Abort.Cancel();
                break;
            }
        }
    }

    /// <summary>Hands one end of a transfer to the replica that runs the fragment; a replica that does not know the run or the edge is a failure of the edge.</summary>
    private async Task WireAsync(ReplicaKey replica, string fragment, string alias, string transferId)
    {
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(Abort.Token);
        bound.CancelAfter(_services.Options.LinkTimeout);
        if (!await _services.Link.WireAsync(replica, Spec.RunId, fragment, alias, transferId, bound.Token))
            throw new InvalidOperationException($"{replica} knows no edge '{alias}' of {fragment} in run {Spec.RunId}.");
    }

    /// <summary>Waits for every fragment to report, or for the abort; once a fragment has failed remotely, only the grace long.</summary>
    private async Task AwaitReportsAsync()
    {
        try
        {
            var allExited = Task.WhenAll(Exited.Values.Select(t => t.Task));
            var remoteFailure = RemoteFailure.Task;
            if (await Task.WhenAny(allExited, remoteFailure).WaitAsync(Abort.Token) == remoteFailure && !allExited.IsCompleted)
            {
                try { await allExited.WaitAsync(_services.Options.RemoteFailureGrace, Abort.Token); }
                catch (TimeoutException)
                {
                    var reported = _fragmentNames.Where(f => Exited[f].Task.IsCompleted).ToHashSet(StringComparer.Ordinal);
                    var failedRemotely = _fragmentNames
                        .Where(f => Exited[f].Task.IsCompletedSuccessfully && Exited[f].Task.Result.Origin == FaultOrigin.Remote)
                        .ToHashSet(StringComparer.Ordinal);
                    _unresponsive.UnionWith(RunOrchestrator.PeersOwingAReport(Spec.Edges, failedRemotely, reported));
                    _services.Logger.LogWarning(
                        "Run {RunId}: {Fragments} did not report within {Grace} of a fragment failing remotely; tearing the run down",
                        Spec.RunId, string.Join(", ", _fragmentNames.Where(f => !reported.Contains(f))), _services.Options.RemoteFailureGrace);
                    Abort.Cancel();
                }
            }
            else
            {
                await allExited.WaitAsync(Abort.Token);
            }
        }
        catch (OperationCanceledException) { /* falls through to the teardown */ }
    }

    /// <summary>
    /// Terminates every transfer this run opened, tells every launched fragment still running to
    /// <c>Cancel</c>, then waits, bounded by <see cref="RunOrchestratorOptions.ExitTimeout"/>, for
    /// whichever of those it could actually reach to finish reporting. A fully wired fragment is told
    /// too - its torn-down transfer faults it only once a relay touches it, which a child with nothing
    /// to write never does - but it stays out of <see cref="_cancelledFragments"/>: it reports the
    /// <c>FaultOrigin.Remote</c> consequence a mid-flow peer death already produces. A fragment whose replica
    /// refused the launch ran nothing, and a replica that knows no such run has nothing to report: neither is waited for.
    /// </summary>
    private async Task TeardownAsync()
    {
        foreach (var transferId in _openTransfers)
            await _services.TransferTerminator.TerminateAsync(transferId);

        // A fragment unreachable here can never report Exited on its own - it is unreachable precisely
        // because nothing can tell it anything, itself included - and a fragment that was fully wired but
        // has since gone silent is still unreachable and must be excluded the
        // same way, not just skipped for Cancel. The identified cause itself is excluded outright, even when
        // its own replica still answers (a fragment that never became ready is still connected): it must stay
        // silent for DetermineOutcome to name it, not be masked by a Cancel it happens to still receive.
        var unreachable = new HashSet<string>(StringComparer.Ordinal);
        if (_lostFragment is not null) unreachable.Add(_lostFragment);
        var knowsNothing = new HashSet<string>(StringComparer.Ordinal);

        async Task CancelOneAsync(string fragment)
        {
            var isFullyWired = _totalEdgesByFragment.TryGetValue(fragment, out var total) && total > 0
                && _wiredEdgesByFragment.GetValueOrDefault(fragment) == total;
            try
            {
                using var bound = new CancellationTokenSource(_services.Options.LinkTimeout);
                var known = await _services.Link.CancelAsync(_placed[fragment], Spec.RunId, fragment, bound.Token);
                lock (_cancelledFragments)
                {
                    if (!isFullyWired) _cancelledFragments.Add(fragment);
                    if (!known) knowsNothing.Add(fragment);
                }
            }
            catch (Exception ex)
            {
                _services.Logger.LogWarning(ex, "Cancel delivery failed for fragment {Fragment}", fragment);
                lock (_cancelledFragments) unreachable.Add(fragment);
            }
        }

        string[] toCancel;
        lock (_launched)
            toCancel = _launched.Where(f => f != _lostFragment && !_refused.Contains(f) && !Exited[f].Task.IsCompleted).ToArray();
        await Task.WhenAll(toCancel.Select(CancelOneAsync));

        var pending = toCancel
            .Where(f => !unreachable.Contains(f) && !knowsNothing.Contains(f) && !Exited[f].Task.IsCompleted)
            .Select(f => Exited[f].Task).ToArray();
        if (pending.Length > 0)
        {
            try { await Task.WhenAll(pending).WaitAsync(_services.Options.ExitTimeout); }
            catch (TimeoutException) { /* still-missing fragments become the silent ones in DetermineOutcome */ }
        }
    }

    private RunResult Conclude(CancellationToken ct)
    {
        var reports = _fragmentNames
            .Where(f => Exited[f].Task.IsCompletedSuccessfully)
            .ToDictionary(f => f, f => Exited[f].Task.Result, StringComparer.Ordinal);

        return RunOrchestrator.DetermineOutcome(reports, Spec.Edges, _fragmentNames, _cancelledFragments, ct.IsCancellationRequested, _wiringFailure, _unresponsive)
            with { Replicas = _chosen.ToDictionary(c => c.Key, c => new RunReplica(c.Value.Key.Short, c.Value.Announcement.Origin), StringComparer.Ordinal) };
    }
}
