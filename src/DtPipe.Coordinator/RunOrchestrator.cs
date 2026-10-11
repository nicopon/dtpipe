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
    /// A replica reports how a fragment ended. Heard only from the replica the run placed that fragment on, and only
    /// under the token of the launch that started the child: anything else, including a replica of the same node and a
    /// report with no token or another launch's, is <see cref="ExitedAck.NotAdmitted"/>.
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
    private readonly RunServices _services;
    private readonly ILogger<RunOrchestrator> _logger;

    /// <summary>
    /// The replica each fragment of a run was placed on, and the token of its launch, published together: a report that
    /// finds the placement finds the token it must carry.
    /// </summary>
    internal sealed record Placement(IReadOnlyDictionary<string, ReplicaKey> Replicas, IReadOnlyDictionary<string, string> Tokens);

    private readonly object _lock = new();
    private readonly Dictionary<string, RunSession> _runs = new(StringComparer.Ordinal);

    public RunOrchestrator(
        ReplicaAdmission admission,
        ReplicaRegistry registry,
        INodeLink link,
        ITransferInitiator transferInitiator,
        ITransferTerminator transferTerminator,
        ILogger<RunOrchestrator> logger,
        RunOrchestratorOptions? options = null)
    {
        _services = new RunServices(admission, registry, link, transferInitiator, transferTerminator, options ?? new RunOrchestratorOptions(), logger);
        _logger = logger;
    }

    public async Task<RunResult> RunAsync(RunSpec spec, CancellationToken ct = default)
    {
        var run = Begin(spec, ct);
        try
        {
            return await run.ExecuteAsync(ct);
        }
        finally
        {
            End(run);
        }
    }

    /// <summary>
    /// Claims the run's id in one step, so two runs of one id never both pass. Several runs of the same fragments
    /// may be in flight: which may overlap is the caller's to decide before it asks.
    /// </summary>
    private RunSession Begin(RunSpec spec, CancellationToken ct)
    {
        lock (_lock)
        {
            if (_runs.ContainsKey(spec.RunId))
                throw new ArgumentException($"A run named '{spec.RunId}' is already in flight.", nameof(spec));

            var run = new RunSession(_services, spec, ct);
            _runs[spec.RunId] = run;
            return run;
        }
    }

    private void End(RunSession run)
    {
        lock (_lock) _runs.Remove(run.Spec.RunId);
        _services.Registry.Release(run.Spec.RunId);
        run.Abort.Dispose();
    }

    private RunSession? Find(string runId)
    {
        lock (_lock) return _runs.GetValueOrDefault(runId);
    }

    internal static RunResult CancelledByRequester(IReadOnlyList<RunEdge> edges) =>
        new(RunOutcome.Cancelled, null, [], new Dictionary<string, FragmentExitReport>(), BuildEdgeCounts(new Dictionary<string, FragmentExitReport>(), edges));

    public Task<ExitedAck> OnExitedAsync(ReplicaKey replica, FragmentExit exit)
    {
        // The fragment name an exit cites finds the fragment; only the replica the run placed it on may report it, and
        // only under the token of the launch that started its child.
        if (Find(exit.RunId) is not { } run || !IsReportOfItsLaunch(run.Placed, replica, exit)
            || !run.Exited.TryGetValue(exit.Fragment, out var tcs))
        {
            _logger.LogWarning(
                "Exited from {Replica} for {Fragment} of run {RunId}, not the report of the launch that run placed there", replica, exit.Fragment, exit.RunId);
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

    /// <summary>
    /// Pure. Whether <paramref name="exit"/> is the report of the launch the run made of its fragment: the run placed the
    /// fragment on <paramref name="replica"/>, and the report carries the token of that launch. A report with no token, or
    /// another one, is never admitted: it is the report of an earlier child that held the same run id, fragment and replica.
    /// </summary>
    internal static bool IsReportOfItsLaunch(Placement? placement, ReplicaKey replica, FragmentExit exit) =>
        placement is not null
        && placement.Replicas.TryGetValue(exit.Fragment, out var placedOn) && placedOn == replica
        && exit.LaunchToken is { } token
        && placement.Tokens.TryGetValue(exit.Fragment, out var expected)
        && string.Equals(token, expected, StringComparison.Ordinal);

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
