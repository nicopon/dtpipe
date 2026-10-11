using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// The run protocol of one replica: what it does with the coordinator's <c>Launch</c>, <c>Wire</c>, <c>Cancel</c>,
/// <c>Check</c> and <c>Kill</c>, and how it reports <c>Exited</c>. The replica's TransportR side is one
/// <see cref="PipelineNode"/> at a time (<see cref="UseNode"/>); each run is a <see cref="FragmentRun"/> in a table keyed by (run, fragment). Every
/// handler answers as soon as it has received the message and never waits for a transfer or for a run: the node
/// channel's client handles messages one after another, and the coordinator bounds each answer. A run ends in
/// one place, <see cref="CompleteAndReportAsync"/>: it is reported, then thrown away.
/// </summary>
internal sealed class ReplicaRuns : IAsyncDisposable
{
    private sealed class Entry(FragmentRun run, FragmentSpec spec, string jobPath, PipelineNode node, string? launchToken)
    {
        public FragmentRun Run { get; } = run;
        public PipelineNode Node { get; } = node;
        public FragmentSpec Spec { get; } = spec;
        public string JobPath { get; } = jobPath;

        /// <summary>The token the coordinator minted for the launch that started this child, returned in its report.</summary>
        public string? LaunchToken { get; } = launchToken;
        public int CompletionStarted;
        public readonly CancellationTokenSource Unwired = new();
    }

    private readonly HubConnection _channel;
    private volatile PipelineNode? _node;
    private readonly string _name;
    private readonly string _dtpipe;
    private readonly IReadOnlyDictionary<string, string> _environment;
    private readonly string _jobsDirectory;
    private readonly Func<FragmentSpec, (string? Yaml, string? Reason)> _intake;
    private readonly Func<string, ILogger> _loggerOf;
    private readonly ILogger _logger;
    private readonly TimeSpan _unwiredTimeout;

    private readonly ConcurrentDictionary<(string RunId, string Fragment), Entry> _runs = new();
    private readonly List<FragmentExit> _held = [];

    // Counts the announcements the coordinator accepted: a report that was in flight when one landed is sent again, not held for the next.
    private int _announcements;

    private static readonly TimeSpan ReportTimeout = TimeSpan.FromSeconds(30);

    public ReplicaRuns(
        HubConnection channel, PipelineNode node, string name, string dtpipe, IReadOnlyDictionary<string, string> environment, string jobsDirectory,
        Func<FragmentSpec, (string? Yaml, string? Reason)> intake, Func<string, ILogger> loggerOf, ILogger logger, TimeSpan unwiredTimeout)
    {
        (_channel, _node, _name, _dtpipe, _environment, _jobsDirectory) = (channel, node, name, dtpipe, environment, jobsDirectory);
        (_intake, _loggerOf, _logger, _unwiredTimeout) = (intake, loggerOf, logger, unwiredTimeout);
        Directory.CreateDirectory(_jobsDirectory);
    }

    /// <summary>
    /// The TransportR client new runs are launched and wired with: null once it was dropped, until another is registered. A
    /// run keeps the node it was launched with, so a transfer is never claimed from another client's book.
    /// </summary>
    public void UseNode(PipelineNode? node) => _node = node;

    /// <summary>
    /// Answers the coordinator's run messages on the node channel: the one table from message to method, for the agent
    /// and for anything that stands in for one. Each handler returns as soon as it has received the message.
    /// </summary>
    public void Bind()
    {
        _channel.On<LaunchRequest, LaunchResult>(NodeChannel.Launch, request => LaunchAsync(request));
        _channel.On<string, string, string, string, bool>(NodeChannel.Wire, (runId, fragment, alias, transferId) =>
            Task.FromResult(Wire(runId, fragment, alias, transferId)));
        _channel.On<string, string, bool>(NodeChannel.Cancel, (runId, fragment) => Task.FromResult(Cancel(runId, fragment)));
        _channel.On<FragmentSpec, FragmentCheck>(NodeChannel.Check, spec => Task.FromResult(Check(spec)));
        _channel.On<string, int>(NodeChannel.Kill, fragment => Task.FromResult(Kill(fragment)));
    }

    /// <summary>The runs in flight on this replica.</summary>
    public int Count => _runs.Count;

    /// <summary>Raised when the last run in flight is gone.</summary>
    public event Action? Idle;

    /// <summary>A run of <paramref name="fragment"/> in flight, if any.</summary>
    public FragmentRun? Find(string fragment) => _runs.FirstOrDefault(r => r.Key.Fragment == fragment).Value?.Run;

    /// <summary>Whether this replica would run <paramref name="spec"/>, spawning nothing: the decision <see cref="LaunchAsync"/> makes first.</summary>
    public FragmentCheck Check(FragmentSpec spec)
    {
        var (yaml, reason) = _intake(spec);
        return new FragmentCheck(yaml is not null, reason);
    }

    /// <summary>
    /// Decides the fragment (the node composes its job from its own bricks, and runs only what its own policy allows),
    /// writes the job and spawns the child. A refusal is an answer, not a failure: the replica ran nothing.
    /// </summary>
    public async Task<LaunchResult> LaunchAsync(LaunchRequest request)
    {
        var spec = request.Spec;
        var key = (request.RunId, spec.Fragment);
        if (_runs.ContainsKey(key)) return new LaunchResult(false, $"{spec.Fragment} already runs in {request.RunId}.");
        if (_node is not { } node) return new LaunchResult(false, "this replica holds no data plane client.");

        var (yaml, reason) = _intake(spec);
        if (yaml is null)
        {
            _logger.LogWarning("Refusing {Fragment} of {RunId}: {Reason}", spec.Fragment, request.RunId, reason);
            return new LaunchResult(false, reason);
        }

        var jobPath = Path.Combine(_jobsDirectory, SafeFileName($"{request.RunId}-{spec.Fragment}") + ".yaml");
        try
        {
            await File.WriteAllTextAsync(jobPath, yaml, new UTF8Encoding(false));
            var launch = new FragmentLaunch
            {
                DtPipeExecutable = _dtpipe,
                FragmentJobPath = jobPath,
                ChildEnvironment = _environment,
                Edges = spec.Edges
                    .Select(e => new EdgeBinding(e.Alias, e.Direction == FragmentEdgeDirection.Inbound ? EdgeDirection.Inbound : EdgeDirection.Outbound))
                    .ToList(),
            };
            var entry = new Entry(node.Launch(launch, _loggerOf(spec.Fragment)), spec, jobPath, node, request.LaunchToken);
            if (!_runs.TryAdd(key, entry))
            {
                await entry.Run.DisposeAsync();
                return new LaunchResult(false, $"{spec.Fragment} already runs in {request.RunId}.");
            }
            _logger.LogInformation("Running {Fragment} of {RunId} ({Edges})", spec.Fragment, request.RunId,
                string.Join(", ", spec.Edges.Select(e => $"{e.Direction} {e.Alias}")));
            if (spec.Edges.Count == 0) StartCompletion(key, entry);
            else _ = AbandonIfUnwiredAsync(key, entry);
            return new LaunchResult(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Could not start {Fragment} of {RunId}", spec.Fragment, request.RunId);
            TryDelete(jobPath);
            return new LaunchResult(false, $"could not start dtpipe: {ex.Message}");
        }
    }

    /// <summary>
    /// Hands one edge of a run its transfer, in the background: the handle of the transfer is delivered by TransportR
    /// on its own schedule, and this answers at once. False when the replica knows no such run or edge.
    /// </summary>
    public bool Wire(string runId, string fragment, string alias, string transferId)
    {
        if (!_runs.TryGetValue((runId, fragment), out var entry) || entry.Run.Edges.All(e => e.Alias != alias)) return false;
        entry.Run.NoteWireRequested();
        // Every edge asked for: the run is the coordinator's to finish, and no longer to abandon for want of a Wire.
        if (entry.Run.WireRequests >= entry.Run.Edges.Count) entry.Unwired.Cancel();
        _ = WireInBackgroundAsync((runId, fragment), entry, alias, transferId);
        return true;
    }

    private async Task WireInBackgroundAsync((string RunId, string Fragment) key, Entry entry, string alias, string transferId)
    {
        try
        {
            // A client dropped since the launch carries no transfer: the run fails now, not after the wait for a handle that cannot come.
            if (!ReferenceEquals(entry.Node, _node))
            {
                entry.Run.Fault(FaultOrigin.Remote, $"the replica's data plane client was dropped before edge '{alias}' was wired");
                StartCompletion(key, entry);
                return;
            }
            var lastEdge = await entry.Node.WireAsync(entry.Run, alias, transferId);
            // Once every declared edge is relaying, the run goes to completion and reports on its own.
            if (lastEdge || entry.Run.WireRequests >= entry.Run.Edges.Count && entry.Run.Origin is not null) StartCompletion(key, entry);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Wiring edge {Alias} of {Fragment} in {RunId} failed", alias, key.Fragment, key.RunId);
            entry.Run.Fault(FaultOrigin.Local, $"wiring edge '{alias}' failed: {ex.Message}");
            StartCompletion(key, entry);
        }
    }

    /// <summary>
    /// The coordinator abandons the run. A fragment wired on every edge is already completing on its own, and its torn-down
    /// transfers fault it - but only once a relay touches them, which a child with nothing to write never makes happen: its
    /// fault is <see cref="FaultOrigin.Remote"/>, the run ended around it. A fragment not yet fully wired has nothing else
    /// to finish it, so it is faulted <see cref="FaultOrigin.Local"/> and run to completion; the coordinator excludes that
    /// one from cause and consequence. Judged on the <c>Wire</c> messages received, as the coordinator judges on the ones
    /// it sent. False when the replica knows no such run.
    /// </summary>
    public bool Cancel(string runId, string fragment)
    {
        if (!_runs.TryGetValue((runId, fragment), out var entry)) return false;
        if (entry.Run.WireRequests == entry.Run.Edges.Count)
        {
            entry.Run.Fault(FaultOrigin.Remote, "run torn down by the coordinator");
            return true;
        }
        entry.Run.Fault(FaultOrigin.Local, "cancelled by coordinator before this fragment opened any transfer");
        StartCompletion((runId, fragment), entry);
        return true;
    }

    /// <summary>Fault injection: kills the child of every run of <paramref name="fragment"/>, the way a crashed host would. How many it killed.</summary>
    public int Kill(string fragment)
    {
        var killed = 0;
        foreach (var (key, entry) in _runs)
        {
            if (key.Fragment != fragment || entry.Run is not { IsLaunched: true, ChildHasExited: false } run) continue;
            try
            {
                Process.GetProcessById(run.ChildProcessId).Kill(entireProcessTree: false);
                killed++;
            }
            catch (Exception) { /* already gone */ }
        }
        _logger.LogWarning("Killed {Count} dtpipe child(ren) of {Fragment}", killed, fragment);
        return killed;
    }

    private void StartCompletion((string RunId, string Fragment) key, Entry entry)
    {
        if (Interlocked.Exchange(ref entry.CompletionStarted, 1) == 1) return;
        _ = Task.Run(() => CompleteAndReportAsync(key, entry));
    }

    /// <summary>A run launched and not wired by <c>UnwiredRunTimeout</c> (the coordinator is gone, or gave up on it) is abandoned.</summary>
    private async Task AbandonIfUnwiredAsync((string RunId, string Fragment) key, Entry entry)
    {
        try { await Task.Delay(_unwiredTimeout, entry.Unwired.Token); }
        catch (OperationCanceledException) { return; }
        _logger.LogWarning("{Fragment} of {RunId} was never wired within {Timeout}; abandoning it", key.Fragment, key.RunId, _unwiredTimeout);
        entry.Run.Fault(FaultOrigin.Local, $"the coordinator did not wire this fragment within {_unwiredTimeout}");
        StartCompletion(key, entry);
    }

    private async Task CompleteAndReportAsync((string RunId, string Fragment) key, Entry entry)
    {
        try
        {
            var outcome = await entry.Run.CompleteAsync();
            await ReportAsync(new FragmentExit(
                key.RunId, key.Fragment, outcome.ExitCode, (outcome.Origin ?? FaultOrigin.Local).ToString(), outcome.FirstFault, outcome.RowCounts,
                entry.LaunchToken));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{Fragment} of {RunId} could not be completed", key.Fragment, key.RunId);
        }
        finally
        {
            await DiscardAsync(key, entry);
        }
    }

    /// <summary>
    /// Tells the coordinator how a run ended. A report that cannot be delivered, or that reaches a connection the coordinator
    /// does not know, is held and sent once after the next successful announcement; one the coordinator says it did not admit
    /// is not sent again. A report that was in flight when an announcement was accepted is sent again at once: the connection it
    /// went out on may be the one that was just accepted.
    /// </summary>
    private async Task ReportAsync(FragmentExit exit)
    {
        for (var attempt = 0; ; attempt++)
        {
            var announced = Volatile.Read(ref _announcements);
            try
            {
                switch (await SendAsync(exit))
                {
                    case ExitedAck.Accepted:
                        return;
                    case ExitedAck.NotAdmitted:
                        _logger.LogWarning("The coordinator did not admit this replica for {Fragment} of {RunId}", exit.Fragment, exit.RunId);
                        return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to report Exited for {Fragment} of {RunId}; it is sent after the next announcement", exit.Fragment, exit.RunId);
            }

            lock (_held)
            {
                if (attempt > 0 || Volatile.Read(ref _announcements) == announced)
                {
                    _held.Add(exit);
                    return;
                }
            }
        }
    }

    private async Task<ExitedAck> SendAsync(FragmentExit exit)
    {
        if (_channel.State != HubConnectionState.Connected) throw new InvalidOperationException("The node channel is not connected.");
        using var bounded = new CancellationTokenSource(ReportTimeout);
        return await _channel.InvokeAsync<ExitedAck>(NodeChannel.Exited, exit, bounded.Token);
    }

    /// <summary>The replica was accepted by the coordinator again: sends, once, the reports it could not deliver before.</summary>
    public async Task OnAnnouncedAsync()
    {
        Interlocked.Increment(ref _announcements);
        FragmentExit[] held;
        lock (_held)
        {
            held = [.. _held];
            _held.Clear();
        }
        foreach (var exit in held)
        {
            try
            {
                if (await SendAsync(exit) != ExitedAck.Accepted)
                    _logger.LogWarning("The coordinator did not take the held report of {Fragment} of {RunId}", exit.Fragment, exit.RunId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "The held report of {Fragment} of {RunId} could not be delivered", exit.Fragment, exit.RunId);
            }
        }
    }

    private async Task DiscardAsync((string RunId, string Fragment) key, Entry entry)
    {
        if (!_runs.TryRemove(key, out _)) return;
        entry.Unwired.Cancel();
        try { await entry.Run.DisposeAsync(); } catch (Exception) { /* best-effort */ }
        TryDelete(entry.JobPath);
        if (_runs.IsEmpty) Idle?.Invoke();
    }

    /// <summary>An orderly stop throws every run away, so no dtpipe child outlives the host.</summary>
    public async ValueTask DisposeAsync()
    {
        foreach (var (key, entry) in _runs.ToArray())
        {
            if (!_runs.TryRemove(key, out _)) continue;
            entry.Unwired.Cancel();
            try { await entry.Run.DisposeAsync(); } catch (Exception) { /* shutting down */ }
            TryDelete(entry.JobPath);
        }
    }

    private static void TryDelete(string path)
    {
        try { File.Delete(path); }
        catch (Exception) { /* best-effort */ }
    }

    private static string SafeFileName(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' or '.' ? c : '_'));
}
