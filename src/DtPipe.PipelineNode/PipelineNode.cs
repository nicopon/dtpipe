using System.Collections.Concurrent;
using System.Security.Cryptography;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Abstractions;
using TransportR.Client.SignalR;
using TransportR.Client.SignalR.Services;
using TransportR.Serialization.MessagePack;

namespace DtPipe.PipelineNode;

/// <summary>
/// Where a fragment's fault came from: its own child process (<see cref="Local"/>), or a transfer a
/// peer tore down (<see cref="Remote"/>, always a <c>TransferFailedException</c> or an equivalent
/// cancellation). The coordinator's outcome rule uses this, not arrival order, to tell a run's cause
/// from its consequences: a peer that aborts its own transfer can fail a healthy fragment in
/// milliseconds, before the fragment that actually died has finished reporting.
/// </summary>
public enum FaultOrigin { Local, Remote }

public sealed class PipelineNodeOptions
{
    public required string HubUrl { get; init; }

    /// <summary>Supplies the bearer token a hub in production mode identifies this node by; the hub
    /// reads the node's group from the token, never from an option. TransportR calls it when the
    /// connection opens, again as the token nears its expiry, and once more after a stream is
    /// refused with 401. Null connects anonymously, which only a development-mode hub accepts. A
    /// provider that throws leaves the previous token in place.</summary>
    public Func<Task<string>>? AccessTokenProvider { get; init; }

    public required string DtPipeExecutable { get; init; }
    public required string FragmentJobPath { get; init; }
    public required IReadOnlyList<EdgeBinding> Edges { get; init; }

    /// <summary>Variables set in the child's environment on top of this process's own, which it
    /// inherits: what a fragment resolves through <c>${{NAME}}</c>. The host's own environment is
    /// never changed for them.</summary>
    public IReadOnlyDictionary<string, string>? ChildEnvironment { get; init; }

    /// <summary>Items per TransportR batch. Bounds sender memory together with <see cref="MaxInFlightBatches"/>:
    /// roughly <see cref="ReadChunkBytes"/> × this × <see cref="MaxInFlightBatches"/> bytes in flight.</summary>
    public int BatchSize { get; init; } = 8;

    /// <summary>Both bounds default to a non-null value. The client library leaves them null, and a
    /// null <see cref="MaxInFlightBatches"/> lets a sender race ahead of a slow receiver — measured
    /// at ~1 GB of receiver-side backlog against a source that finishes in seconds. 8 matches the
    /// engine's own in-process Arrow channel capacity.</summary>
    public int? MaxInFlightBatches { get; init; } = 8;
    public int? ReceiveMaxItems { get; init; }
    public long? ReceiveMaxBytes { get; init; }
    public int ReadChunkBytes { get; init; } = 65536;
}

/// <summary>
/// Launches one dtpipe child per fragment and relays the Arrow IPC bytes of each declared edge
/// between the child's own stdin/stdout and a single TransportR client — the node is the sole
/// TransportR client for the fragment it hosts.
/// </summary>
/// <remarks>
/// Cancellation is by stream rupture, never by signal — Windows has no console to deliver one to
/// a child: a fault closes the child's own stdio, which is exactly what its own <c>arrow:</c>
/// reader/writer already turns into a truncated-stream exit 1, then the child is killed after a
/// grace period if closing its streams alone did not end it.
/// </remarks>
public sealed class PipelineNode : IAsyncDisposable
{
    private readonly PipelineNodeOptions _options;
    private readonly ILogger _logger;
    private readonly SignalRDataClient<byte[]> _client;

    private FragmentRun? _run;

    private readonly ConcurrentDictionary<string, TaskCompletionSource<object>> _handles = new();

    private bool _coordinatorDriven;

    /// <summary>Set once disposal starts: a reconnect that lands after must not register the fragment again.</summary>
    private volatile bool _disposing;
    private string? _runId;
    /// <summary>Set when <see cref="ReportExitedAsync"/> fails - retried once a reconnect's re-Register succeeds.</summary>
    private string? _pendingExitedRunId;
    private int _pendingExitedCode;
    private readonly TaskCompletionSource<int> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Guid ClientId => _client.ClientId;
    public int ChildProcessId => _run!.ChildProcessId;
    public bool ChildHasExited => _run!.ChildHasExited;
    public IReadOnlyDictionary<string, long> RowCounts => _run?.RowCounts ?? new Dictionary<string, long>();
    public string? FirstFault => _run?.FirstFault;
    public FaultOrigin? Origin => _run?.Origin;

    /// <summary>
    /// Resolves with this fragment's exit code once every edge a coordinator wired has finished
    /// relaying and the coordinator has been told (or the report failed, and is held for a
    /// reconnect) - the self-driving path started by <see cref="ConnectAsync"/>. A caller may dispose
    /// the node the moment this resolves: its <c>Unregister</c> cannot overtake its own <c>Exited</c>.
    /// Never resolves for a
    /// node started with <see cref="StartAsync"/>, which has no coordinator to drive it.
    /// </summary>
    public Task<int> Completion => _completion.Task;

    /// <summary>True once <see cref="LaunchAsync"/> has spawned the child - before that, <see cref="ChildProcessId"/> throws.</summary>
    public bool IsLaunched => _run is { IsLaunched: true };

    private PipelineNode(PipelineNodeOptions options, ILogger logger, SignalRDataClient<byte[]> client)
    {
        _options = options;
        _logger = logger;
        _client = client;
    }

    /// <summary>
    /// The only invariant left once a direction can carry more than one edge: <see cref="WireAsync"/>
    /// and the run's pipes both key purely on <see cref="EdgeBinding.Alias"/>, regardless of
    /// direction, so two edges sharing one would collide silently instead of relaying independently.
    /// </summary>
    private static void ValidateEdges(PipelineNodeOptions options)
    {
        var duplicate = options.Edges.GroupBy(e => e.Alias, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1);
        if (duplicate is not null)
            throw new ArgumentException($"Edge alias '{duplicate.Key}' is declared more than once.", nameof(options));
    }

    public static async Task<PipelineNode> StartAsync(
        PipelineNodeOptions options, ILoggerFactory? loggerFactory = null, CancellationToken ct = default)
    {
        ValidateEdges(options);

        loggerFactory ??= NullLoggerFactory.Instance;
        var logger = loggerFactory.CreateLogger<PipelineNode>();

        var client = BuildClient(options, loggerFactory, controlConnectionSetup: null);
        var node = new PipelineNode(options, logger, client);

        client.OnSendRequested += node.HandleSendRequested;
        client.OnTransferStarted += node.HandleTransferStarted;

        await client.ConnectAsync(cancellationToken: ct);
        node._run = new FragmentRun(options, logger);
        node._run.Launch();

        return node;
    }

    /// <summary>
    /// Connects to the coordinator and declares this fragment (<c>Register</c>), but does not
    /// launch the child yet: the coordinator's admission barrier decides when, by pushing
    /// <c>Launch</c> - handled here by <see cref="LaunchAsync"/> - once every fragment a run needs
    /// has registered. <c>Wire</c> pushes are handled the same way, through <see cref="WireAsync"/>;
    /// once every declared edge is wired, the node runs itself to completion and reports
    /// <c>Exited</c> without further prompting - see <see cref="Completion"/>.
    /// </summary>
    public static async Task<PipelineNode> ConnectAsync(
        PipelineNodeOptions options, string fragmentName, ILoggerFactory? loggerFactory = null, CancellationToken ct = default)
    {
        ValidateEdges(options);

        loggerFactory ??= NullLoggerFactory.Instance;
        var logger = loggerFactory.CreateLogger<PipelineNode>();
        var version = ComputeFragmentVersion(options.FragmentJobPath);

        PipelineNode? node = null;
        var client = BuildClient(options, loggerFactory, controlConnectionSetup: conn =>
        {
            conn.On<string>("Launch", async runId =>
            {
                try { await node!.LaunchAsync(runId); }
                catch (Exception ex) { logger.LogError(ex, "Launch handler failed for run {RunId}", runId); }
            });
            conn.On<string, string, string>("Wire", async (runId, alias, transferId) =>
            {
                try { await node!.WireAsync(alias, transferId); }
                catch (Exception ex) { logger.LogError(ex, "Wire handler failed for run {RunId}, alias {Alias}", runId, alias); }
            });
            conn.On<string>("Cancel", async runId =>
            {
                try { await node!.CancelAsync(runId); }
                catch (Exception ex) { logger.LogError(ex, "Cancel handler failed for run {RunId}", runId); }
            });

            // TransportR re-issues its own Connect on this same event (SignalRDataClient); the
            // coordinator's Register is a second, application-level registration on top of it that
            // TransportR has no reason to know about, so nothing re-issues it but this node. A bare
            // reconnect gives this connection a new SignalR ConnectionId, so without this the
            // fragment's inventory entry in NodeRegistry keeps pointing at a dead one until its
            // disconnect grace period lapses and declares the fragment lost outright.
            conn.Reconnected += async _ =>
            {
                if (node!._disposing) return;
                await ReRegisterWithRetryAsync(conn, fragmentName, version, logger);
                await node!.RetryPendingExitedReportAsync();
            };
        });

        node = new PipelineNode(options, logger, client) { _coordinatorDriven = true };
        client.OnSendRequested += node.HandleSendRequested;
        client.OnTransferStarted += node.HandleTransferStarted;

        await client.ConnectAsync(cancellationToken: ct);
        await client.ControlConnection.InvokeAsync("Register", fragmentName, version, ct);

        return node;
    }

    /// <summary>
    /// This fragment's own identity: the SHA-256 hash of its job YAML's bytes, hex-encoded - cheap
    /// and requires no sample run, unlike the contract hash (a different, out-of-scope concept: it
    /// hashes the executed schema, not the file). The coordinator's instance alignment
    /// (<c>AdmissionGate.Resolve</c>) compares this string across every live instance of one fragment
    /// name.
    /// </summary>
    private static string ComputeFragmentVersion(string fragmentJobPath)
    {
        using var stream = File.OpenRead(fragmentJobPath);
        return Convert.ToHexStringLower(SHA256.HashData(stream));
    }

    /// <summary>Spawns the child and reports this fragment ready for every declared edge to be wired.</summary>
    public async Task LaunchAsync(string runId, CancellationToken ct = default)
    {
        _run = new FragmentRun(_options, _logger);
        _runId = runId;
        _run.Launch();
        await _client.ControlConnection.InvokeAsync("Ready", runId, ct);
    }

    /// <summary>
    /// A bounded retry, not a single best-effort attempt: unlike TransportR's own presence heartbeat
    /// (which repairs a missed re-registration on its own next tick), nothing else ever retries this
    /// application-level call, and the reconnect this runs on can itself race the hub's own
    /// bookkeeping for the dropped connection - an immediate Register can still find the old
    /// connection's <c>Connect</c> record not yet superseded and be refused. Giving up after every
    /// attempt fails is deliberate: the coordinator's own disconnect grace period is the backstop,
    /// and it will correctly declare the fragment lost.
    /// </summary>
    private static async Task ReRegisterWithRetryAsync(HubConnection connection, string fragmentName, string version, ILogger logger)
    {
        const int maxAttempts = 5;
        var delay = TimeSpan.FromMilliseconds(200);
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await connection.InvokeAsync("Register", fragmentName, version);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                logger.LogWarning(ex,
                    "Re-Register attempt {Attempt}/{MaxAttempts} failed for fragment {Fragment} after reconnect",
                    attempt, maxAttempts, fragmentName);
                await Task.Delay(delay);
                delay += delay;
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Fragment {Fragment} could not re-Register after reconnect; the coordinator's " +
                    "disconnect grace period will declare it lost", fragmentName);
            }
        }
    }

    /// <summary>
    /// The coordinator's own request to abandon this run: a run the requester cancelled, or another
    /// fragment failing. Reuses the rupture any fault takes - closing the child's stdio turns into its
    /// own truncated-stream exit, and the grace-period kill follows. A stale or mistargeted message
    /// (not this fragment's current run) is silently ignored.
    /// <para>
    /// A fully wired fragment is already completing on its own (<see cref="CompleteAndReportAsync"/>),
    /// and its torn-down transfers fault it - but only once a relay touches them, which a child with
    /// nothing to write never makes happen. Its fault is <see cref="FaultOrigin.Remote"/>: the run
    /// ended around it. A fragment not yet fully wired has nothing else to finish it, so this call
    /// runs it to completion and reports; the coordinator excludes that one from cause and
    /// consequence.
    /// </para>
    /// </summary>
    public async Task CancelAsync(string runId, CancellationToken ct = default)
    {
        if (runId != _runId) return;

        // Judged on the Wire pushes received, as the coordinator judges on the ones it sent: a Cancel
        // racing a WireAsync still waiting on its handle must not read as "not yet wired".
        if (_coordinatorDriven && _run!.WireRequests == _options.Edges.Count)
        {
            _run.Fault(FaultOrigin.Remote, "run torn down by the coordinator");
            return;
        }

        _run!.Fault(FaultOrigin.Local, "cancelled by coordinator before this fragment opened any transfer");

        var exitCode = await RunToCompletionAsync(ct);
        try { await ReportExitedWithFallbackAsync(runId, exitCode, "after Cancel", ct); }
        finally { _completion.TrySetResult(exitCode); }
    }

    /// <summary>
    /// Retries a report <see cref="ReportExitedAsync"/> could not deliver, once a reconnect's
    /// re-Register has (or has not) had its chance: a report sent on a fresh connection before its
    /// own re-Register lands fails <c>ResolveFragment</c> on the hub side, and nothing but this retry
    /// would ever send it again.
    /// </summary>
    private async Task RetryPendingExitedReportAsync()
    {
        if (_pendingExitedRunId is not { } runId) return;
        try
        {
            await ReportExitedAsync(runId, _pendingExitedCode);
            _pendingExitedRunId = null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Retry of Exited for run {RunId} failed again after reconnect", runId);
        }
    }

    private async Task ReportExitedWithFallbackAsync(string runId, int exitCode, string when, CancellationToken ct)
    {
        try { await ReportExitedAsync(runId, exitCode, ct); }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to report Exited {When} for run {RunId}; will retry after the next successful Register", when, runId);
            _pendingExitedRunId = runId;
            _pendingExitedCode = exitCode;
        }
    }

    private static SignalRDataClient<byte[]> BuildClient(
        PipelineNodeOptions options, ILoggerFactory loggerFactory, Action<HubConnection>? controlConnectionSetup)
    {
        var builder = new TransportRClientBuilder<byte[]>()
            .WithUrl(options.HubUrl)
            .WithMessagePackSerialization()
            .WithBatchSize(options.BatchSize)
            .WithLoggerFactory(loggerFactory);

        if (options.AccessTokenProvider is { } tokenProvider)
            builder.WithTokenProvider(tokenProvider);

        if (controlConnectionSetup is not null)
            builder.WithControlConnection(controlConnectionSetup);

        if (options.MaxInFlightBatches is { } maxInFlight)
            builder.WithMaxInFlightBatches(maxInFlight);

        // A receive byte cap is always set, even when the caller leaves ReceiveMaxBytes null: an
        // unset receive capacity is exactly the unbounded-backlog configuration MaxInFlightBatches
        // alone does not fix (a bound at one end only moves the backlog to the other).
        var receiveMaxBytes = options.ReceiveMaxBytes
            ?? (long)options.ReadChunkBytes * options.BatchSize * (options.MaxInFlightBatches ?? 8);
        builder.WithReceiveCapacity(options.ReceiveMaxItems, receiveMaxBytes);

        return builder.Build();
    }

    private Task HandleSendRequested(Send<byte[]> send)
    {
        HandleHandle(send.TransferId, send);
        return Task.CompletedTask;
    }

    private Task HandleTransferStarted(Receive<byte[]> receive)
    {
        HandleHandle(receive.TransferId, receive);
        return Task.CompletedTask;
    }

    private void HandleHandle(string transferId, object handle) =>
        _handles.GetOrAdd(transferId, _ => new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously))
            .TrySetResult(handle);

    /// <summary>
    /// Correlates a declared local alias to whichever handle <c>OnSendRequested</c> or
    /// <c>OnTransferStarted</c> produces for <paramref name="transferId"/> — before or after this
    /// call, since the handshake races the caller's own <c>InitTransferAsync</c> (both sides key on
    /// the transfer id, neither waits for the other) — then starts relaying it.
    /// </summary>
    public async Task WireAsync(string alias, string transferId, CancellationToken ct = default)
    {
        if (_options.Edges.All(e => e.Alias != alias))
            throw new ArgumentException($"'{alias}' is not a declared edge of this node.", nameof(alias));
        _run!.NoteWireRequested();

        var tcs = _handles.GetOrAdd(transferId, _ => new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
        var handle = await tcs.Task.WaitAsync(ct);
        _handles.TryRemove(transferId, out _);

        var lastEdge = _run.Wire(alias, handle);

        // Self-driving path only (ConnectAsync): once every declared edge has started relaying, run
        // this fragment to completion and report Exited without further prompting from the
        // coordinator - StartAsync's hand-wired callers drive RunToCompletionAsync themselves and
        // never touch _coordinatorDriven.
        if (_coordinatorDriven && lastEdge)
            _ = Task.Run(() => CompleteAndReportAsync(_runId!));
    }

    /// <summary>
    /// Waits for every wired edge to finish relaying, then for the child itself, and returns the
    /// child's own exit code, or 1 if the fragment faulted (see <see cref="FragmentRun.RunToCompletionAsync"/>).
    /// </summary>
    public Task<int> RunToCompletionAsync(CancellationToken ct = default) => _run!.RunToCompletionAsync(ct);

    private async Task CompleteAndReportAsync(string runId)
    {
        var exitCode = await RunToCompletionAsync();
        try { await ReportExitedWithFallbackAsync(runId, exitCode, "on completion", default); }
        finally { _completion.TrySetResult(exitCode); }
    }

    /// <summary>Reports this fragment's own outcome to the coordinator: exit code, fault origin, row counts.</summary>
    public Task ReportExitedAsync(string runId, int exitCode, CancellationToken ct = default) =>
        _client.ControlConnection.InvokeAsync(
            "Exited", runId, exitCode, (_run!.Origin ?? FaultOrigin.Local).ToString(), _run.FirstFault, _run.LiveRowCounts, ct);


    /// <summary>How long disposal waits for the coordinator to acknowledge <c>Unregister</c> before closing anyway.</summary>
    private static readonly TimeSpan UnregisterTimeout = TimeSpan.FromSeconds(2);

    public async ValueTask DisposeAsync()
    {
        _disposing = true;

        // The coordinator drops this instance from its inventory only when it processes the closed
        // connection, some time after the close; until then admission can still pick it. Saying so
        // first makes the removal precede the close. Best effort: a node that cannot reach the
        // coordinator is covered by the coordinator's own disconnect handling.
        if (_coordinatorDriven)
        {
            try
            {
                using var timeout = new CancellationTokenSource(UnregisterTimeout);
                await _client.ControlConnection.InvokeAsync("Unregister", timeout.Token);
            }
            catch { }
        }

        try { await _client.DisconnectAsync(); } catch { }
        await _client.DisposeAsync();

        // Null for a ConnectAsync node never reached by a Launch push (e.g. disposed while still
        // waiting on admission): there is no child to tear down, and no pipe was ever created either.
        if (_run is null) return;

        await _run.DisposeAsync();
    }
}
