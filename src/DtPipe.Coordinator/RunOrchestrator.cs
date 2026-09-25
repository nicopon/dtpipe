using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using TransportR.Interfaces;

namespace DtPipe.Coordinator;

/// <summary>Mirrors <c>DtPipe.PipelineNode.FaultOrigin</c> on the wire - the two projects never share a type.</summary>
public enum FaultOrigin { Local, Remote }

/// <summary>One flow from a producer fragment's outbound alias to a consumer fragment's inbound alias.</summary>
public sealed record RunEdge(string ProducerFragment, string ProducerAlias, string ConsumerFragment, string ConsumerAlias);

/// <summary>The fragments a run needs present, and the edges to wire once they are.</summary>
public sealed record RunSpec(string RunId, IReadOnlyList<string> Fragments, IReadOnlyList<RunEdge> Edges);

/// <summary>What one fragment reported when its own process finished.</summary>
public sealed record FragmentExitReport(
    string Fragment, int ExitCode, FaultOrigin? Origin, string? FirstFault, IReadOnlyDictionary<string, long> RowCounts);

public enum RunOutcome { Succeeded, Failed }

/// <summary><see cref="Cause"/> is null on <see cref="RunOutcome.Succeeded"/>.</summary>
public sealed record RunResult(
    RunOutcome Outcome, string? Cause, IReadOnlyList<string> Consequences,
    IReadOnlyDictionary<string, FragmentExitReport> Reports);

public sealed class RunOrchestratorOptions
{
    public TimeSpan AdmissionTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan ReadyTimeout { get; init; } = TimeSpan.FromSeconds(10);
    public TimeSpan ExitTimeout { get; init; } = TimeSpan.FromSeconds(30);
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
/// launches all of them, opens each declared edge once both endpoints report ready, then collects
/// every fragment's own exit report and applies the outcome rule (<see cref="DetermineOutcome"/>).
/// One run in flight at a time: a second call to <see cref="RunAsync"/> is refused outright, not
/// queued - concurrent runs are not supported yet. A node whose connection drops mid-run is not
/// handled here yet: see the coordinator's own guidance file.
/// </summary>
public sealed class RunOrchestrator : IRunOrchestrator
{
    private readonly AdmissionGate _admission;
    private readonly IHubContext<CoordinatorHub> _hub;
    private readonly ITransferInitiator _transferInitiator;
    private readonly RunOrchestratorOptions _options;
    private readonly ILogger<RunOrchestrator> _logger;

    private readonly SemaphoreSlim _singleRun = new(1, 1);

    // Live only while a run is in flight; (re)built at the start of RunAsync, cleared in its finally.
    private string? _activeRunId;
    private Dictionary<string, TaskCompletionSource>? _ready;
    private Dictionary<string, TaskCompletionSource<FragmentExitReport>>? _exited;

    public RunOrchestrator(
        AdmissionGate admission,
        IHubContext<CoordinatorHub> hub,
        ITransferInitiator transferInitiator,
        ILogger<RunOrchestrator> logger,
        RunOrchestratorOptions? options = null)
    {
        _admission = admission;
        _hub = hub;
        _transferInitiator = transferInitiator;
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
                f => f, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
                StringComparer.Ordinal);
            _exited = spec.Fragments.ToDictionary(
                f => f, _ => new TaskCompletionSource<FragmentExitReport>(TaskCreationOptions.RunContinuationsAsynchronously),
                StringComparer.Ordinal);

            // Admission: the run does not exist yet, so a refusal here names the absentee and never
            // reaches the outcome rule below.
            var admitted = await _admission.AwaitAllAsync(spec.Fragments, _options.AdmissionTimeout, ct);
            var byFragment = admitted.ToDictionary(n => n.FragmentName, StringComparer.Ordinal);

            foreach (var fragment in spec.Fragments)
                await _hub.Clients.Client(byFragment[fragment].ConnectionId).SendAsync("Launch", spec.RunId, ct);

            await Task.WhenAll(_ready.Values.Select(tcs => tcs.Task)).WaitAsync(_options.ReadyTimeout, ct);

            foreach (var edge in spec.Edges)
            {
                var producer = byFragment[edge.ProducerFragment];
                var consumer = byFragment[edge.ConsumerFragment];
                var transferId = await _transferInitiator.InitTransferAsync(
                    producer.ClientId, consumer.ClientId, _options.BatchSize, _options.TransferTimeoutMs, ct);

                await _hub.Clients.Client(producer.ConnectionId).SendAsync("Wire", spec.RunId, edge.ProducerAlias, transferId, ct);
                await _hub.Clients.Client(consumer.ConnectionId).SendAsync("Wire", spec.RunId, edge.ConsumerAlias, transferId, ct);
            }

            await Task.WhenAll(_exited.Values.Select(tcs => tcs.Task)).WaitAsync(_options.ExitTimeout, ct);

            var reports = spec.Fragments.ToDictionary(f => f, f => _exited[f].Task.Result, StringComparer.Ordinal);
            return DetermineOutcome(reports, spec.Edges);
        }
        finally
        {
            _activeRunId = null;
            _ready = null;
            _exited = null;
            _singleRun.Release();
        }
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
        return Task.CompletedTask;
    }

    /// <summary>
    /// Pure. Every fragment at 0 is not enough: a fragment reporting 0 attests its own process, not
    /// that its peer received what it sent, so a run only succeeds when every declared edge's two
    /// row counts also agree. A count mismatch fails the run even with every exit code at 0, naming
    /// the edge rather than a fragment - neither end is individually at fault.
    ///
    /// Otherwise the cause is the first fragment whose own failure has a <see cref="FaultOrigin.Local"/>
    /// origin - its own child process, not a transfer an aborting peer tore down - and every other
    /// non-zero fragment is a consequence, regardless of arrival order: an aborting peer can fail a
    /// healthy fragment before the fragment that actually failed has finished reporting.
    /// </summary>
    internal static RunResult DetermineOutcome(
        IReadOnlyDictionary<string, FragmentExitReport> reports, IReadOnlyList<RunEdge> edges)
    {
        var mismatchedEdges = edges
            .Where(e =>
                reports.TryGetValue(e.ProducerFragment, out var producer) &&
                reports.TryGetValue(e.ConsumerFragment, out var consumer) &&
                producer.RowCounts.TryGetValue(e.ProducerAlias, out var sent) &&
                consumer.RowCounts.TryGetValue(e.ConsumerAlias, out var received) &&
                sent != received)
            .ToList();

        if (reports.Values.All(r => r.ExitCode == 0))
        {
            if (mismatchedEdges.Count == 0)
                return new RunResult(RunOutcome.Succeeded, null, [], reports);

            var edgeNames = mismatchedEdges.Select(e => $"{e.ProducerFragment}->{e.ConsumerFragment}");
            return new RunResult(RunOutcome.Failed, $"row count mismatch on edge(s): {string.Join(", ", edgeNames)}", [], reports);
        }

        var cause = reports.Values.FirstOrDefault(r => r.ExitCode != 0 && r.Origin == FaultOrigin.Local)
            ?? reports.Values.First(r => r.ExitCode != 0);
        var consequences = reports.Values
            .Where(r => r.Fragment != cause.Fragment && r.ExitCode != 0)
            .Select(r => r.Fragment)
            .ToList();
        return new RunResult(RunOutcome.Failed, cause.Fragment, consequences, reports);
    }
}
