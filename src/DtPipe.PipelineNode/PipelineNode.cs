using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using TransportR.Abstractions;
using TransportR.Client.SignalR;
using TransportR.Client.SignalR.Services;
using TransportR.Serialization.MessagePack;

namespace DtPipe.PipelineNode;

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
    private readonly Process _child;

    private readonly ConcurrentDictionary<string, TaskCompletionSource<object>> _handles = new();

    private readonly List<Task> _relayTasks = new();
    private readonly Dictionary<string, long> _rowCounts = new();
    private readonly object _rowCountsLock = new();
    private int _faulted;
    private string? _firstFault;

    public Guid ClientId => _client.ClientId;
    public int ChildProcessId => _child.Id;
    public bool ChildHasExited => _child.HasExited;
    public IReadOnlyDictionary<string, long> RowCounts { get { lock (_rowCountsLock) return new Dictionary<string, long>(_rowCounts); } }
    public string? FirstFault => _firstFault;

    private PipelineNode(PipelineNodeOptions options, ILogger logger, SignalRDataClient<byte[]> client, Process child)
    {
        _options = options;
        _logger = logger;
        _client = client;
        _child = child;
    }

    public static async Task<PipelineNode> StartAsync(
        PipelineNodeOptions options, ILoggerFactory? loggerFactory = null, CancellationToken ct = default)
    {
        if (options.Edges.Count(e => e.Direction == EdgeDirection.Inbound) > 1
            || options.Edges.Count(e => e.Direction == EdgeDirection.Outbound) > 1)
            throw new NotSupportedException(
                "A node relays each direction through the child's single stdin/stdout; a fragment " +
                "with more than one inbound or more than one outbound edge is not supported yet.");

        loggerFactory ??= NullLoggerFactory.Instance;
        var logger = loggerFactory.CreateLogger<PipelineNode>();

        var child = LaunchChild(options, logger);

        var builder = new TransportRClientBuilder<byte[]>()
            .WithUrl(options.HubUrl)
            .WithMessagePackSerialization()
            .WithBatchSize(options.BatchSize)
            .WithLoggerFactory(loggerFactory);

        if (options.MaxInFlightBatches is { } maxInFlight)
            builder.WithMaxInFlightBatches(maxInFlight);

        // A receive byte cap is always set, even when the caller leaves ReceiveMaxBytes null: an
        // unset receive capacity is exactly the unbounded-backlog configuration MaxInFlightBatches
        // alone does not fix (a bound at one end only moves the backlog to the other).
        var receiveMaxBytes = options.ReceiveMaxBytes
            ?? (long)options.ReadChunkBytes * options.BatchSize * (options.MaxInFlightBatches ?? 8);
        builder.WithReceiveCapacity(options.ReceiveMaxItems, receiveMaxBytes);

        var client = builder.Build();
        var node = new PipelineNode(options, logger, client, child);

        client.OnSendRequested += node.HandleSendRequested;
        client.OnTransferStarted += node.HandleTransferStarted;

        await client.ConnectAsync(cancellationToken: ct);

        return node;
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
    }

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
                Fault($"outbound edge '{alias}': child dtpipe exited {_child.ExitCode}");
        }
        catch (Exception ex)
        {
            Fault($"outbound edge '{alias}': {ex.Message}");
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
        catch (Exception ex)
        {
            Fault($"inbound edge '{alias}': {ex.Message}");
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

    private void Fault(string reason)
    {
        if (Interlocked.CompareExchange(ref _firstFault, reason, null) is null)
            _logger.LogError("Node faulted: {Reason}", reason);
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

        if (!_child.HasExited)
        {
            try { _child.Kill(entireProcessTree: true); } catch { }
        }
        _child.Dispose();
    }
}
