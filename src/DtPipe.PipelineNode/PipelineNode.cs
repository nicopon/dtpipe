using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Pipes;
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
    private const int GracePeriodMs = 3000;

    private readonly PipelineNodeOptions _options;
    private readonly ILogger _logger;
    private readonly SignalRDataClient<byte[]> _client;
    private Process _child = null!;

    /// <summary>
    /// One server pipe per excess edge (the second-and-later edge of a direction), keyed by
    /// <see cref="EdgeBinding.Alias"/> — empty for a fragment with at most one inbound and one
    /// outbound edge, which still rides stdin/stdout exactly as before J2.
    /// </summary>
    private Dictionary<string, NamedPipeServerStream> _pipes = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, TaskCompletionSource<object>> _handles = new();

    private readonly List<Task> _relayTasks = new();
    private readonly Dictionary<string, long> _rowCounts = new();
    private readonly object _rowCountsLock = new();
    private int _faulted;
    private int _ruptured;
    private string? _firstFault;
    private FaultOrigin? _faultOrigin;
    private bool _coordinatorDriven;
    private string? _runId;
    private int _wiredCount;
    /// <summary>Wire pushes received, counted on arrival: the coordinator's own view of "fully wired".</summary>
    private int _wireRequests;
    /// <summary>Set when <see cref="ReportExitedAsync"/> fails - retried once a reconnect's re-Register succeeds.</summary>
    private string? _pendingExitedRunId;
    private int _pendingExitedCode;
    private readonly TaskCompletionSource<int> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Guid ClientId => _client.ClientId;
    public int ChildProcessId => _child.Id;
    public bool ChildHasExited => _child.HasExited;
    public IReadOnlyDictionary<string, long> RowCounts { get { lock (_rowCountsLock) return new Dictionary<string, long>(_rowCounts); } }
    public string? FirstFault => _firstFault;
    public FaultOrigin? Origin => _faultOrigin;

    /// <summary>
    /// Resolves with this fragment's exit code once every edge a coordinator wired has finished
    /// relaying - the self-driving path started by <see cref="ConnectAsync"/>. Never resolves for a
    /// node started with <see cref="StartAsync"/>, which has no coordinator to drive it.
    /// </summary>
    public Task<int> Completion => _completion.Task;

    /// <summary>True once <see cref="LaunchAsync"/> has spawned the child - before that, <see cref="ChildProcessId"/> throws.</summary>
    public bool IsLaunched => _child is not null;

    private PipelineNode(PipelineNodeOptions options, ILogger logger, SignalRDataClient<byte[]> client)
    {
        _options = options;
        _logger = logger;
        _client = client;
    }

    /// <summary>
    /// The only invariant left once a direction can carry more than one edge: <see cref="WireAsync"/>
    /// and <see cref="_pipes"/> both key purely on <see cref="EdgeBinding.Alias"/>, regardless of
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
        (node._child, node._pipes) = LaunchChild(options, logger);

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
        _runId = runId;
        (_child, _pipes) = LaunchChild(_options, _logger);
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
        if (_coordinatorDriven && Volatile.Read(ref _wireRequests) == _options.Edges.Count)
        {
            Fault(FaultOrigin.Remote, "run torn down by the coordinator");
            return;
        }

        Fault(FaultOrigin.Local, "cancelled by coordinator before this fragment opened any transfer");

        var exitCode = await RunToCompletionAsync(ct);
        _completion.TrySetResult(exitCode);
        await ReportExitedWithFallbackAsync(runId, exitCode, "after Cancel", ct);
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

    /// <summary>Short and random, never a readable composed name (alias + run id): the pipe name
    /// joins <c>$TMPDIR/CoreFxPipe_</c> on Unix, and a long <c>TMPDIR</c> (macOS routinely runs
    /// ~60 characters) already leaves little of the 104-character <c>sun_path</c> budget spare.</summary>
    private static string NewPipeName() => Guid.NewGuid().ToString("N")[..10];

    private static (Process Child, Dictionary<string, NamedPipeServerStream> Pipes) LaunchChild(
        PipelineNodeOptions options, ILogger logger)
    {
        var inbound = options.Edges.Where(e => e.Direction == EdgeDirection.Inbound).ToList();
        var outbound = options.Edges.Where(e => e.Direction == EdgeDirection.Outbound).ToList();

        // The first edge of each direction keeps riding stdin/stdout - the path already proven;
        // only an excess edge (a join's second source, a fan-out's second sink) needs a pipe of
        // its own, created here as server before the child that connects to it as client starts.
        var pipes = new Dictionary<string, NamedPipeServerStream>(StringComparer.Ordinal);
        var bindInputPairs = new List<string>();
        var bindOutputPairs = new List<string>();

        foreach (var edge in inbound.Skip(1))
        {
            var pipeName = NewPipeName();
            pipes[edge.Alias] = new NamedPipeServerStream(pipeName, PipeDirection.Out, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
            // AliasBindingApplier prepends its own 'arrow:' to this location - adding it here too
            // would double it into 'arrow:arrow:pipe://...' and fail closed with "file not found".
            bindInputPairs.Add($"{edge.Alias}=pipe://{pipeName}");
        }
        foreach (var edge in outbound.Skip(1))
        {
            var pipeName = NewPipeName();
            pipes[edge.Alias] = new NamedPipeServerStream(pipeName, PipeDirection.In, 1, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
            bindOutputPairs.Add($"{edge.Alias}=pipe://{pipeName}");
        }

        var psi = new ProcessStartInfo
        {
            FileName = options.DtPipeExecutable,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("--job");
        psi.ArgumentList.Add(options.FragmentJobPath);
        if (bindInputPairs.Count > 0) { psi.ArgumentList.Add("--bind-input"); psi.ArgumentList.Add(string.Join(",", bindInputPairs)); }
        if (bindOutputPairs.Count > 0) { psi.ArgumentList.Add("--bind-output"); psi.ArgumentList.Add(string.Join(",", bindOutputPairs)); }

        // The fragment's own primary 'arrow:-' already means the child's own stdin/stdout, piped
        // to this node rather than inherited (AliasBindingApplier); no binding flag is needed for
        // it. An excess edge's 'arrow:-' is overridden above instead.
        var process = new Process { StartInfo = psi, EnableRaisingEvents = true };
        process.Start();
        _ = DrainStderrAsync(process, logger);

        // An edge-less direction still has to be a live pipe (never the node's own stdio), but
        // nothing ever wires it: close it, or drain it, so the child cannot block writing to or
        // reading from a pipe nobody is ever going to service.
        if (inbound.Count == 0)
            process.StandardInput.Close();
        if (outbound.Count == 0)
            _ = DrainAsync(process.StandardOutput.BaseStream, logger, "stdout");

        return (process, pipes);
    }

    /// <summary>
    /// Races the excess edge's own <see cref="NamedPipeServerStream.WaitForConnectionAsync()"/>
    /// against the child's own exit: without this, a child that dies before ever connecting to its
    /// bound pipe (an invalid job, a binding error) would block this relay task, and so this whole
    /// node, forever - nothing else was ever going to connect to a server nobody dials into after
    /// its only prospective client is dead.
    /// </summary>
    private async Task<Stream> ConnectPipeAsync(NamedPipeServerStream pipe, string alias)
    {
        var childExited = _child.WaitForExitAsync();
        var connected = pipe.WaitForConnectionAsync();
        if (await Task.WhenAny(connected, childExited) == childExited)
            throw new IOException($"child dtpipe exited before connecting to the named pipe for edge '{alias}'.");
        await connected;
        return pipe;
    }

    private static async Task DrainStderrAsync(Process process, ILogger logger)
    {
        string? firstLine = null;
        string? line;
        try
        {
            while ((line = await process.StandardError.ReadLineAsync()) is not null)
            {
                firstLine ??= line;
                logger.LogDebug("child stderr: {Line}", line);
            }
        }
        catch (ObjectDisposedException) { /* process/stream torn down concurrently — nothing left to drain */ }
        if (firstLine is not null)
            logger.LogInformation("child's first stderr line: {Line}", firstLine);
    }

    private static async Task DrainAsync(Stream stream, ILogger logger, string label)
    {
        var buffer = new byte[4096];
        try
        {
            while (await stream.ReadAsync(buffer) > 0) { }
        }
        catch (Exception ex) when (ex is ObjectDisposedException or IOException)
        {
            logger.LogDebug("draining child's {Label} ended: {Message}", label, ex.Message);
        }
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
        var edge = _options.Edges.FirstOrDefault(e => e.Alias == alias)
            ?? throw new ArgumentException($"'{alias}' is not a declared edge of this node.", nameof(alias));
        Interlocked.Increment(ref _wireRequests);

        var tcs = _handles.GetOrAdd(transferId, _ => new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
        var handle = await tcs.Task.WaitAsync(ct);
        _handles.TryRemove(transferId, out _);

        if (edge.Direction == EdgeDirection.Outbound)
            lock (_relayTasks) _relayTasks.Add(Task.Run(() => RelayOutboundAsync(alias, (Send<byte[]>)handle)));
        else
            lock (_relayTasks) _relayTasks.Add(Task.Run(() => RelayInboundAsync(alias, (Receive<byte[]>)handle)));

        // Self-driving path only (ConnectAsync): once every declared edge has started relaying, run
        // this fragment to completion and report Exited without further prompting from the
        // coordinator - StartAsync's hand-wired callers drive RunToCompletionAsync themselves and
        // never touch _coordinatorDriven.
        if (_coordinatorDriven && Interlocked.Increment(ref _wiredCount) == _options.Edges.Count)
            _ = Task.Run(() => CompleteAndReportAsync(_runId!));
    }

    private async Task CompleteAndReportAsync(string runId)
    {
        var exitCode = await RunToCompletionAsync();
        _completion.TrySetResult(exitCode);
        await ReportExitedWithFallbackAsync(runId, exitCode, "on completion", default);
    }

    /// <summary>Reports this fragment's own outcome to the coordinator: exit code, fault origin, row counts.</summary>
    public Task ReportExitedAsync(string runId, int exitCode, CancellationToken ct = default) =>
        _client.ControlConnection.InvokeAsync(
            "Exited", runId, exitCode, (_faultOrigin ?? FaultOrigin.Local).ToString(), _firstFault, _rowCounts, ct);

    private async Task RelayOutboundAsync(string alias, Send<byte[]> send)
    {
        var counter = new ArrowIpcRowCounter();
        var buffer = new byte[_options.ReadChunkBytes];
        var sending = false;
        try
        {
            var stream = _pipes.TryGetValue(alias, out var pipe)
                ? await ConnectPipeAsync(pipe, alias)
                : _child.StandardOutput.BaseStream;

            while (true)
            {
                int read = await stream.ReadAsync(buffer);
                if (read == 0) break;
                var chunk = buffer.AsSpan(0, read).ToArray();
                counter.Feed(chunk);
                sending = true;
                await send.SendAsync(chunk);
                sending = false;
            }

            await _child.WaitForExitAsync();
            if (_child.ExitCode == 0)
            {
                sending = true;
                await send.CompleteAsync();
            }
            else
                Fault(FaultOrigin.Local, $"outbound edge '{alias}': child dtpipe exited {_child.ExitCode}");
        }
        catch (ObjectDisposedException) when (sending)
        {
            // The handle itself was disposed under the call: the transfer was torn down, the same
            // as TransferFailedException - not a local failure worth naming as the run's cause.
            Fault(FaultOrigin.Remote, $"outbound edge '{alias}': the transfer was torn down");
        }
        catch (Exception ex) when (ex is TransferFailedException or OperationCanceledException)
        {
            // TransferFailedException is the documented contract for SendAsync/CompleteAsync. An
            // OperationCanceledException here is never this code's own doing - no cancellation
            // token reaches SendAsync - so it can only be the transfer failing underneath it, the
            // same as TransferFailedException.
            Fault(FaultOrigin.Remote, $"outbound edge '{alias}': {ex.Message}");
        }
        catch (Exception ex)
        {
            // Anything else - reading the child's own stdout failed locally.
            Fault(FaultOrigin.Local, $"outbound edge '{alias}': {ex.Message}");
        }
        finally
        {
            lock (_rowCountsLock) _rowCounts[alias] = counter.RowCount;
            if (_pipes.TryGetValue(alias, out var ownPipe)) { try { await ownPipe.DisposeAsync(); } catch { /* best-effort */ } }
            try { await send.DisposeAsync(); } catch { /* best-effort: the fault is already recorded */ }
        }
    }

    private async Task RelayInboundAsync(string alias, Receive<byte[]> receive)
    {
        var counter = new ArrowIpcRowCounter();
        try
        {
            var stream = _pipes.TryGetValue(alias, out var pipe)
                ? await ConnectPipeAsync(pipe, alias)
                : _child.StandardInput.BaseStream;

            await foreach (var chunk in receive.ReceiveAsync())
            {
                counter.Feed(chunk);
                await stream.WriteAsync(chunk);
            }
            await stream.FlushAsync();
            await receive.WaitForCompletionAsync();
        }
        catch (Exception ex) when (ex is TransferFailedException or OperationCanceledException)
        {
            // Thrown by ReceiveAsync's own enumeration or WaitForCompletionAsync: the child was
            // still fine, the transfer itself failed. OperationCanceledException here is never this
            // code's own doing either, for the same reason as the outbound side.
            Fault(FaultOrigin.Remote, $"inbound edge '{alias}': {ex.Message}");
        }
        catch (Exception ex)
        {
            // Anything else - writing to the child's own stdin failed locally (a dead child closes
            // its end of the pipe first).
            Fault(FaultOrigin.Local, $"inbound edge '{alias}': {ex.Message}");
        }
        finally
        {
            lock (_rowCountsLock) _rowCounts[alias] = counter.RowCount;
            // Never synthesises dtpipe's own end-of-stream marker: on success it was already part of
            // the relayed bytes, on failure the child's arrow: reader turns this truncation into its
            // own EndOfStreamException — the propagation this node adds is exactly one closed stream,
            // the primary edge's real stdin or an excess edge's own named pipe, never the other one.
            if (_pipes.TryGetValue(alias, out var ownPipe)) { try { await ownPipe.DisposeAsync(); } catch { /* already closed */ } }
            else { try { _child.StandardInput.Close(); } catch { /* already closed */ } }
            try { await receive.DisposeAsync(); } catch { /* best-effort: the fault is already recorded */ }
        }
    }

    /// <summary>
    /// Records only the first fault: both origin and message come from that single winning call, so
    /// a second relay task's own fault (this fragment can have one inbound and one outbound edge)
    /// never mixes its origin with the first one's message. Every fault ruptures the child at once.
    /// </summary>
    private void Fault(FaultOrigin origin, string reason)
    {
        if (Interlocked.CompareExchange(ref _firstFault, reason, null) is null)
        {
            _faultOrigin = origin;
            _logger.LogError("Node faulted ({Origin}): {Reason}", origin, reason);
        }
        Interlocked.Exchange(ref _faulted, 1);
        Rupture();
    }

    /// <summary>
    /// Closes the child's stdin and every named pipe, then kills the child if it has not exited
    /// within the grace period - once, at the first fault. It cannot wait for the relays to finish:
    /// an outbound relay blocked reading a child that has nothing to write (an aggregate over a slow
    /// source) finishes only when that child's stdout closes, which is what the kill brings about.
    /// Closes every pipe regardless of whether its own relay task already did (a harmless second
    /// dispose): an excess edge never wired has no relay task, and its pipe would otherwise sit
    /// listening for a client that is never coming.
    /// </summary>
    private void Rupture()
    {
        var child = _child;
        if (child is null || Interlocked.Exchange(ref _ruptured, 1) != 0) return;

        try { child.StandardInput.Close(); } catch { }
        foreach (var pipe in _pipes.Values) { try { pipe.Dispose(); } catch { } }

        _ = Task.Run(async () =>
        {
            try { await child.WaitForExitAsync().WaitAsync(TimeSpan.FromMilliseconds(GracePeriodMs)); }
            catch (TimeoutException) { try { child.Kill(entireProcessTree: true); } catch { } }
            catch (Exception) { /* disposed with the node: nothing left to kill */ }
        });
    }

    /// <summary>
    /// Waits for every wired edge to finish relaying - a fault has already ruptured the child - then
    /// for the child itself. Returns the child's own exit code, or 1 if the fragment faulted.
    /// </summary>
    public async Task<int> RunToCompletionAsync(CancellationToken ct = default)
    {
        Task[] tasks;
        lock (_relayTasks) tasks = _relayTasks.ToArray();
        await Task.WhenAll(tasks);

        if (_faulted != 0)
        {
            Rupture();
            try { _child.StandardOutput.Close(); } catch { }
        }

        await _child.WaitForExitAsync(ct);
        return _faulted != 0 ? Math.Max(_child.ExitCode, 1) : _child.ExitCode;
    }

    public async ValueTask DisposeAsync()
    {
        try { await _client.DisconnectAsync(); } catch { }
        await _client.DisposeAsync();

        // Null for a ConnectAsync node never reached by a Launch push (e.g. disposed while still
        // waiting on admission): there is no child to tear down, and no pipe was ever created either.
        if (_child is null) return;

        if (!_child.HasExited)
        {
            try { _child.Kill(entireProcessTree: true); } catch { }
        }
        _child.Dispose();
        foreach (var pipe in _pipes.Values) { try { pipe.Dispose(); } catch { } }
    }
}
