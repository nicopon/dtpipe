using System.Collections.Concurrent;
using System.Diagnostics;
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

    private readonly ConcurrentDictionary<string, TaskCompletionSource<object>> _handles = new();

    private readonly List<Task> _relayTasks = new();
    private readonly Dictionary<string, long> _rowCounts = new();
    private readonly object _rowCountsLock = new();
    private int _faulted;
    private string? _firstFault;
    private FaultOrigin? _faultOrigin;
    private bool _coordinatorDriven;
    private string? _runId;
    private int _wiredCount;
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

    private static void ValidateEdges(PipelineNodeOptions options)
    {
        if (options.Edges.Count(e => e.Direction == EdgeDirection.Inbound) > 1
            || options.Edges.Count(e => e.Direction == EdgeDirection.Outbound) > 1)
            throw new NotSupportedException(
                "A node relays each direction through the child's single stdin/stdout; a fragment " +
                "with more than one inbound or more than one outbound edge is not supported yet.");
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
        node._child = LaunchChild(options, logger);

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
            conn.Reconnected += _ => ReRegisterWithRetryAsync(conn, fragmentName, logger);
        });

        node = new PipelineNode(options, logger, client) { _coordinatorDriven = true };
        client.OnSendRequested += node.HandleSendRequested;
        client.OnTransferStarted += node.HandleTransferStarted;

        await client.ConnectAsync(cancellationToken: ct);
        await client.ControlConnection.InvokeAsync("Register", fragmentName, ct);

        return node;
    }

    /// <summary>Spawns the child and reports this fragment ready for every declared edge to be wired.</summary>
    public async Task LaunchAsync(string runId, CancellationToken ct = default)
    {
        _runId = runId;
        _child = LaunchChild(_options, _logger);
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
    private static async Task ReRegisterWithRetryAsync(HubConnection connection, string fragmentName, ILogger logger)
    {
        const int maxAttempts = 5;
        var delay = TimeSpan.FromMilliseconds(200);
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await connection.InvokeAsync("Register", fragmentName);
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
    /// The coordinator's own request to abandon this run before this fragment ever opened a
    /// transfer: a run the requester cancelled, or another fragment going silent before this one
    /// reached <see cref="WireAsync"/>. Reuses the rupture a mid-flow peer fault already takes -
    /// closing the child's stdio turns into its own truncated-stream exit, then
    /// <see cref="RunToCompletionAsync"/>'s existing grace-period kill applies unchanged. A stale or
    /// mistargeted message (not this fragment's current run) is silently ignored.
    /// </summary>
    public async Task CancelAsync(string runId, CancellationToken ct = default)
    {
        if (runId != _runId) return;

        Fault(FaultOrigin.Local, "cancelled by coordinator before this fragment opened any transfer");

        var exitCode = await RunToCompletionAsync(ct);
        _completion.TrySetResult(exitCode);
        try { await ReportExitedAsync(runId, exitCode, ct); }
        catch (Exception ex) { _logger.LogError(ex, "Failed to report Exited after Cancel for run {RunId}", runId); }
    }

    private static SignalRDataClient<byte[]> BuildClient(
        PipelineNodeOptions options, ILoggerFactory loggerFactory, Action<HubConnection>? controlConnectionSetup)
    {
        var builder = new TransportRClientBuilder<byte[]>()
            .WithUrl(options.HubUrl)
            .WithMessagePackSerialization()
            .WithBatchSize(options.BatchSize)
            .WithLoggerFactory(loggerFactory);

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

    private static Process LaunchChild(PipelineNodeOptions options, ILogger logger)
    {
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

        // The fragment's own 'arrow:-' already means the child's own stdin/stdout, piped to this
        // node rather than inherited (AliasBindingApplier); no binding flag is needed here.
        var process = new Process { StartInfo = psi, EnableRaisingEvents = true };
        process.Start();
        _ = DrainStderrAsync(process, logger);

        // An edge-less direction still has to be a live pipe (never the node's own stdio), but
        // nothing ever wires it: close it, or drain it, so the child cannot block writing to or
        // reading from a pipe nobody is ever going to service.
        if (!options.Edges.Any(e => e.Direction == EdgeDirection.Inbound))
            process.StandardInput.Close();
        if (!options.Edges.Any(e => e.Direction == EdgeDirection.Outbound))
            _ = DrainAsync(process.StandardOutput.BaseStream, logger, "stdout");

        return process;
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
        try { await ReportExitedAsync(runId, exitCode); }
        catch (Exception ex) { _logger.LogError(ex, "Failed to report Exited for run {RunId}", runId); }
    }

    /// <summary>Reports this fragment's own outcome to the coordinator: exit code, fault origin, row counts.</summary>
    public Task ReportExitedAsync(string runId, int exitCode, CancellationToken ct = default) =>
        _client.ControlConnection.InvokeAsync(
            "Exited", runId, exitCode, (_faultOrigin ?? FaultOrigin.Local).ToString(), _firstFault, _rowCounts, ct);

    private async Task RelayOutboundAsync(string alias, Send<byte[]> send)
    {
        var counter = new ArrowIpcRowCounter();
        var buffer = new byte[_options.ReadChunkBytes];
        var stream = _child.StandardOutput.BaseStream;
        try
        {
            while (true)
            {
                int read = await stream.ReadAsync(buffer);
                if (read == 0) break;
                var chunk = buffer.AsSpan(0, read).ToArray();
                counter.Feed(chunk);
                await send.SendAsync(chunk);
            }

            await _child.WaitForExitAsync();
            if (_child.ExitCode == 0)
                await send.CompleteAsync();
            else
                Fault(FaultOrigin.Local, $"outbound edge '{alias}': child dtpipe exited {_child.ExitCode}");
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
            try { await send.DisposeAsync(); } catch { /* best-effort: the fault is already recorded */ }
        }
    }

    private async Task RelayInboundAsync(string alias, Receive<byte[]> receive)
    {
        var counter = new ArrowIpcRowCounter();
        var stream = _child.StandardInput.BaseStream;
        try
        {
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
            // own EndOfStreamException — the propagation this node adds is exactly one closed pipe.
            try { _child.StandardInput.Close(); } catch { /* already closed */ }
            try { await receive.DisposeAsync(); } catch { /* best-effort: the fault is already recorded */ }
        }
    }

    /// <summary>
    /// Records only the first fault: both origin and message come from that single winning call, so
    /// a second relay task's own fault (this fragment can have one inbound and one outbound edge)
    /// never mixes its origin with the first one's message.
    /// </summary>
    private void Fault(FaultOrigin origin, string reason)
    {
        if (Interlocked.CompareExchange(ref _firstFault, reason, null) is null)
        {
            _faultOrigin = origin;
            _logger.LogError("Node faulted ({Origin}): {Reason}", origin, reason);
        }
        Interlocked.Exchange(ref _faulted, 1);
    }

    /// <summary>
    /// Waits for every wired edge to finish relaying, then — if any faulted — ruptures the child's
    /// streams and kills it after a grace period. Returns the child's own exit code, or 1 if the
    /// child had not yet produced one of its own.
    /// </summary>
    public async Task<int> RunToCompletionAsync(CancellationToken ct = default)
    {
        Task[] tasks;
        lock (_relayTasks) tasks = _relayTasks.ToArray();
        await Task.WhenAll(tasks);

        if (_faulted != 0)
        {
            try { _child.StandardInput.Close(); } catch { }
            try { _child.StandardOutput.Close(); } catch { }

            if (!_child.HasExited)
            {
                var exitedInGrace = _child.WaitForExit(GracePeriodMs);
                if (!exitedInGrace)
                    _child.Kill(entireProcessTree: true);
            }
        }

        await _child.WaitForExitAsync(ct);
        return _faulted != 0 ? Math.Max(_child.ExitCode, 1) : _child.ExitCode;
    }

    public async ValueTask DisposeAsync()
    {
        try { await _client.DisconnectAsync(); } catch { }
        await _client.DisposeAsync();

        // Null for a ConnectAsync node never reached by a Launch push (e.g. disposed while still
        // waiting on admission): there is no child to tear down.
        if (_child is null) return;

        if (!_child.HasExited)
        {
            try { _child.Kill(entireProcessTree: true); } catch { }
        }
        _child.Dispose();
    }
}
