using System.Collections.Concurrent;
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

/// <summary>What a replica needs to move bytes: its TransportR client, and the bounds that keep a fast sender from racing ahead of a slow receiver.</summary>
public sealed class NodeClientOptions
{
    public required string HubUrl { get; init; }

    /// <summary>Supplies the bearer token a hub in production mode identifies this node by; the hub
    /// reads the node's group from the token, never from an option. TransportR calls it when the
    /// connection opens, again as the token nears its expiry, and once more after a stream is
    /// refused with 401. Null connects anonymously, which only a development-mode hub accepts. A
    /// provider that throws leaves the previous token in place.</summary>
    public Func<Task<string>>? AccessTokenProvider { get; init; }

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

    /// <summary>
    /// How long a transfer handle waits for the run that claims it (its <c>Wire</c>), and a <c>Wire</c> for the handle
    /// its transfer is to deliver. A handle nobody wires in that time (its run was torn down first) is dropped.
    /// </summary>
    public TimeSpan UnclaimedTransferTimeout { get; init; } = TimeSpan.FromSeconds(60);
}

/// <summary>
/// One replica's side of the data plane: a single TransportR client, the book of the transfer handles it is given,
/// and the runs it launches, each a <see cref="FragmentRun"/> with a dtpipe child of its own. The node is a byte
/// relay for every run it hosts, and the only TransportR client they have.
/// </summary>
/// <remarks>
/// Cancellation is by stream rupture, never by signal — Windows has no console to deliver one to
/// a child: a fault closes the child's own stdio, which is exactly what its own <c>arrow:</c>
/// reader/writer already turns into a truncated-stream exit 1, then the child is killed after a
/// grace period if closing its streams alone did not end it.
/// </remarks>
public sealed class PipelineNode : IAsyncDisposable
{
    private readonly NodeClientOptions _options;
    private readonly ILogger _logger;
    private readonly SignalRDataClient<byte[]> _client;

    private readonly ConcurrentDictionary<string, TaskCompletionSource<object>> _handles = new();

    public Guid ClientId => _client.ClientId;

    private PipelineNode(NodeClientOptions options, ILogger logger, SignalRDataClient<byte[]> client)
    {
        _options = options;
        _logger = logger;
        _client = client;
    }

    /// <summary>
    /// The only invariant left once a direction can carry more than one edge: <see cref="WireAsync(FragmentRun,string,string,CancellationToken)"/>
    /// and the run's pipes both key purely on <see cref="EdgeBinding.Alias"/>, regardless of
    /// direction, so two edges sharing one would collide silently instead of relaying independently.
    /// </summary>
    private static void ValidateEdges(IReadOnlyList<EdgeBinding> edges)
    {
        var duplicate = edges.GroupBy(e => e.Alias, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1);
        if (duplicate is not null)
            throw new ArgumentException($"Edge alias '{duplicate.Key}' is declared more than once.", nameof(edges));
    }

    /// <summary>
    /// Connects this replica's TransportR client, which registers it with the hub, and launches nothing. Each run
    /// is then started by <see cref="Launch"/> and wired by <see cref="WireAsync(FragmentRun,string,string,CancellationToken)"/>.
    /// </summary>
    public static async Task<PipelineNode> ConnectAsync(
        NodeClientOptions options, ILoggerFactory? loggerFactory = null, CancellationToken ct = default)
    {
        loggerFactory ??= NullLoggerFactory.Instance;
        var logger = loggerFactory.CreateLogger<PipelineNode>();

        var client = BuildClient(options, loggerFactory);
        var node = new PipelineNode(options, logger, client);

        client.OnSendRequested += node.HandleSendRequested;
        client.OnTransferStarted += node.HandleTransferStarted;

        try
        {
            await client.ConnectAsync(cancellationToken: ct);
        }
        catch
        {
            // A refused registration (an identity the coordinator does not know, or has disabled) leaves a started connection behind.
            await client.DisposeAsync();
            throw;
        }
        return node;
    }

    /// <summary>Spawns the dtpipe child of one run. The child is the run's alone; this node's client carries its edges.</summary>
    /// <exception cref="ArgumentException">Two edges share an alias.</exception>
    public FragmentRun Launch(FragmentLaunch launch, ILogger? logger = null)
    {
        ValidateEdges(launch.Edges);
        var run = new FragmentRun(launch, logger ?? _logger);
        try
        {
            run.Launch();
            return run;
        }
        catch
        {
            // A launch that could not start its child leaves nothing to tear down.
            _ = run.DisposeAsync();
            throw;
        }
    }

    private static SignalRDataClient<byte[]> BuildClient(NodeClientOptions options, ILoggerFactory loggerFactory)
    {
        var builder = new TransportRClientBuilder<byte[]>()
            .WithUrl(options.HubUrl)
            .WithMessagePackSerialization()
            .WithBatchSize(options.BatchSize)
            .WithLoggerFactory(loggerFactory);

        if (options.AccessTokenProvider is { } tokenProvider)
            builder.WithTokenProvider(tokenProvider);

        if (options.MaxInFlightBatches is { } maxInFlight)
            builder.WithMaxInFlightBatches(maxInFlight);

        // A receive byte cap is always set, even when the caller leaves ReceiveMaxBytes null: an
        // unset receive capacity is exactly the unbounded-backlog configuration MaxInFlightBatches
        // alone does not fix (a bound at one end only moves the backlog to the other). The client
        // applies it to each transfer it receives, so a replica running several carries one bound apiece.
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

    private void HandleHandle(string transferId, object handle)
    {
        var tcs = _handles.GetOrAdd(transferId, _ => new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
        if (tcs.TrySetResult(handle)) _ = DropIfUnclaimedAsync(transferId, tcs, handle);
    }

    /// <summary>A handle no run wires within the timeout belongs to a run torn down before its Wire: it is released, not kept.</summary>
    private async Task DropIfUnclaimedAsync(string transferId, TaskCompletionSource<object> tcs, object handle)
    {
        try { await Task.Delay(_options.UnclaimedTransferTimeout); }
        catch (Exception) { return; }
        if (!_handles.TryRemove(new KeyValuePair<string, TaskCompletionSource<object>>(transferId, tcs))) return;
        _logger.LogWarning("Transfer {TransferId} was never wired by a run; releasing it", transferId);
        await ReleaseAsync(handle);
    }

    private static async ValueTask ReleaseAsync(object handle)
    {
        try
        {
            if (handle is IAsyncDisposable disposable) await disposable.DisposeAsync();
        }
        catch (Exception) { /* best-effort */ }
    }

    /// <summary>
    /// The handle <c>OnSendRequested</c> or <c>OnTransferStarted</c> produces for <paramref name="transferId"/> — before
    /// or after this call, since the handshake races the coordinator's own <c>InitTransferAsync</c> (both sides key on the
    /// transfer id, neither waits for the other). Claimed once.
    /// </summary>
    private async Task<object> ClaimHandleAsync(string transferId, CancellationToken ct)
    {
        var tcs = _handles.GetOrAdd(transferId, _ => new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
        try
        {
            var handle = await tcs.Task.WaitAsync(_options.UnclaimedTransferTimeout, ct);
            if (!_handles.TryRemove(new KeyValuePair<string, TaskCompletionSource<object>>(transferId, tcs)))
                throw new TimeoutException($"Transfer {transferId} was released before it was wired.");
            return handle;
        }
        catch (TimeoutException)
        {
            _handles.TryRemove(new KeyValuePair<string, TaskCompletionSource<object>>(transferId, tcs));
            throw;
        }
    }

    /// <summary>
    /// Correlates the edge <paramref name="alias"/> of <paramref name="run"/> to the handle of <paramref name="transferId"/>
    /// and starts relaying it. True when it was the last edge the run declares. A transfer that never reaches this node
    /// faults the run (<see cref="FaultOrigin.Remote"/>) and returns false.
    /// </summary>
    public async Task<bool> WireAsync(FragmentRun run, string alias, string transferId, CancellationToken ct = default)
    {
        object handle;
        try { handle = await ClaimHandleAsync(transferId, ct); }
        catch (TimeoutException ex)
        {
            run.Fault(FaultOrigin.Remote, $"transfer {transferId} for edge '{alias}' never reached this node: {ex.Message}");
            return false;
        }
        return run.Wire(alias, handle);
    }

    /// <summary>
    /// Whether the hub knows this client's connection, which is what a registration is: false after the hub lost it or
    /// refused it (an identity it has since disabled), and null while the connection is not up to be asked.
    /// </summary>
    public async Task<bool?> IsRegisteredAsync(CancellationToken ct = default)
    {
        var connection = _client.ControlConnection;
        if (connection.State != HubConnectionState.Connected) return null;
        try { return await connection.InvokeAsync<bool>("Heartbeat", ct); }
        catch (Exception) when (!ct.IsCancellationRequested) { return null; }
    }

    /// <summary>
    /// Registers this client with the hub again, so the hub takes the groups its rights give the node now and a fresh
    /// token. TransportR's client does the work (<c>ReRegisterAsync</c>, then a token push) on every restart of its
    /// control connection, and offers no way to ask for it: <c>ConnectAsync</c> refuses on a started client and a
    /// disconnected one cannot start again. So the connection is stopped and the client's own recovery restarts it,
    /// with the same client id. Call it with no run in flight (a restart interrupts the client's control channel). A client
    /// the hub holds a registration of is refreshed while the node is not announced as able to take a run: until the
    /// connection is back, the client carries nothing. A client the hub does not know (<see cref="IsRegisteredAsync"/>)
    /// holds no groups to be stale, and is registered again at any time. Returns whether the connection was back within
    /// <paramref name="timeout"/>.
    /// </summary>
    public async Task<bool> RefreshRegistrationAsync(TimeSpan timeout, CancellationToken ct = default)
    {
        var connection = _client.ControlConnection;
        await connection.StopAsync(ct);
        var deadline = DateTime.UtcNow + timeout;
        while (connection.State != HubConnectionState.Connected)
        {
            if (DateTime.UtcNow > deadline) return false;
            await Task.Delay(50, ct);
        }
        return true;
    }

    public async ValueTask DisposeAsync()
    {
        try { await _client.DisconnectAsync(); } catch { }
        await _client.DisposeAsync();

        // A handle delivered and never wired belongs to no run any more.
        foreach (var (_, tcs) in _handles)
            if (tcs.Task.IsCompletedSuccessfully) await ReleaseAsync(tcs.Task.Result);
        _handles.Clear();
    }
}
