using System.Threading.Channels;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// One replica of a node: announces the variables it describes and the bricks it offers over the coordinator's node
/// channel, with the TransportR client its runs move their bytes with, and runs the fragments the coordinator launches
/// on it (<see cref="ReplicaRuns"/>). Whatever the coordinator asks, a fragment runs only under the node's own
/// <see cref="FragmentPolicy"/>, read from its configuration and never from a message. Messages are answered as they
/// arrive and never queued behind a run; a brick preview and a lab request answer on the same footing.
/// </summary>
internal sealed class NodeAgent : BackgroundService
{
    private readonly DtPipeNodeOptions _options;
    private readonly FragmentPolicy _policy;
    private readonly Guid _instanceId = Guid.NewGuid();
    private readonly string _stateDir;
    private readonly string _fragmentsDir;
    private readonly IReadOnlyList<DatasetInfo> _datasets;
    private readonly IReadOnlyDictionary<string, string> _environment;
    private readonly BrickHost _bricks;
    private readonly DelegatedJobs _jobs;
    private readonly FragmentGate? _gate;
    private readonly DtPipeCommand _command;
    private string? _dtpipeVersion;
    private readonly OidcTokenClient _tokens;
    private readonly NodeLogForwarder _forwarder = new();
    private readonly ILoggerFactory _hostLoggers;
    private readonly ILogger _logger;
    private HubConnection? _hub;

    // Whether the coordinator accepted this replica's announcement on the connection that is up. The data plane client is asked
    // for only while it is: a host whose identity the coordinator refuses cannot register a client, and asking would cost it a
    // token and a connection every time it is refused.
    private volatile bool _announced;

    // The replica's TransportR client and its runs. The runs are made with the first client and kept; the client is
    // dropped when the hub refuses its registration and another is made when the identity can register one, so the client
    // id an announcement carries is the current client's, or none.
    private PipelineNode? _node;
    private ReplicaRuns? _runs;

    // One connection of the client at a time: the connection loop and the watch both ask for it, and a second client
    // would leak the first and answer every message twice.
    private readonly SemaphoreSlim _connecting = new(1, 1);

    // Signalled by every close of the connection; one loop reads it and connects again.
    private readonly Channel<bool> _closed = Channel.CreateBounded<bool>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    public NodeAgent(IOptions<DtPipeNodeOptions> options, NodeDefinition definition, ILoggerFactory loggers)
    {
        _options = options.Value;
        _policy = definition.Policy ?? _options.Policy;
        _stateDir = _options.ProcessDirectory(_instanceId);
        _fragmentsDir = Path.Combine(_stateDir, "fragments");
        Directory.CreateDirectory(_fragmentsDir);

        var environment = new Dictionary<string, string>(_options.Variables, StringComparer.Ordinal);
        foreach (var variable in definition.Variables) environment[variable.Name] = variable.Value;
        _environment = environment;

        // No value is announced: the coordinator learns a variable's name and description;
        // whatever reads it runs here.
        _datasets = definition.Variables
            .Select(v => new DatasetInfo(v.Name, v.Description ?? ""))
            .Concat(_options.Variables.Keys.Where(k => definition.Variables.All(v => v.Name != k)).Select(k => new DatasetInfo(k, "")))
            .ToList();

        _hostLoggers = loggers;
        _logger = _forwarder.For(loggers, _options.Name, fragment: null).CreateLogger(typeof(NodeAgent).FullName!);
        var dtpipe = new DtPipeCommand(_options.DtPipe, _stateDir, _environment);
        _bricks = new BrickHost(_options.Name, definition.Bricks, dtpipe, _logger);
        _gate = definition.Gate;
        _command = dtpipe;
        _jobs = new DelegatedJobs(_options.Name, _policy == FragmentPolicy.AnyFragment, dtpipe);
        _tokens = new OidcTokenClient(_options.Oidc, _options.Name, _options.CoordinatorUrl);
    }

    private NodeRole Role => _policy == FragmentPolicy.AnyFragment ? NodeRole.Runner : NodeRole.Data;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var hub = _hub = new HubConnectionBuilder()
            .WithUrl(_options.CoordinatorUrl.TrimEnd('/') + NodeChannel.Path, o => o.AccessTokenProvider = async () => await _tokens.GetAsync())
            .WithAutomaticReconnect(new KeepRetrying())
            .Build();

        hub.On<string, int, BrickPreview>(NodeChannel.PreviewBrick, (id, rows) => _bricks.PreviewAsync(id, rows));
        hub.On<string, int, BrickSample>(NodeChannel.SampleBrick, (id, rows) => _bricks.SampleAsync(id, rows));
        hub.On<string, string, string, SourceCheck>(NodeChannel.CheckSource, (id, contract, mode) => _bricks.CheckSourceAsync(id, contract, mode));
        hub.On<string, string, SinkCheck>(NodeChannel.CheckSink, (id, contract) => _bricks.CheckSinkAsync(id, contract));
        hub.On<JobCheck, JobCheckResult>(NodeChannel.ValidateJob, check => _jobs.ValidateAsync(check));
        hub.Reconnecting += _ =>
        {
            _announced = false;
            return Task.CompletedTask;
        };
        hub.Reconnected += async _ => await AnnounceAsync();
        // A connection the coordinator closes (a reconnect it asked for, an announcement it refused)
        // is not retried by SignalR's own policy: the reconnect loop starts it again, so a node the
        // coordinator accepts once more comes back without a restart of its host. The handler only
        // signals: starting the connection from inside it races the next close.
        hub.Closed += _ =>
        {
            _announced = false;
            _closed.Writer.TryWrite(true);
            return Task.CompletedTask;
        };

        // Schemas are inspected before the first announcement, so the coordinator never shows a
        // brick without the columns its node could read.
        await _bricks.InspectAsync(ct);
        _dtpipeVersion = await _command.VersionAsync(ct);
        if (_dtpipeVersion is null) _logger.LogWarning("{DtPipe} did not say its version: the node announces none", _options.DtPipe);

        await ConnectAsync(hub, ct);
        _logger.LogInformation("Node {Name} ({Role}, {Bricks} brick(s), {Policy}) connected to {Url} as {ClientId}",
            _options.Name, Role, _bricks.Bricks.Count, _policy, _options.CoordinatorUrl, _tokens.ClientId);
        _ = ReconnectLoopAsync(hub, ct);
        _ = WatchDataPlaneAsync(hub, ct);

        await foreach (var line in _forwarder.Reader.ReadAllAsync(ct))
        {
            if (hub.State != HubConnectionState.Connected) continue;
            try { await hub.SendAsync(NodeChannel.ReportLog, line, ct); }
            catch (Exception ex) when (ex is not OperationCanceledException) { }
        }
    }

    private async Task ReconnectLoopAsync(HubConnection hub, CancellationToken ct)
    {
        try
        {
            await foreach (var _ in _closed.Reader.ReadAllAsync(ct))
            {
                if (hub.State != HubConnectionState.Disconnected) continue;
                _logger.LogWarning("The coordinator closed the connection; connecting again");
                await Task.Delay(ReconnectDelay, ct);
                await RefreshDataPlaneAsync(ct);
                await ConnectAsync(hub, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan AnnounceTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan RetryDelayCap = TimeSpan.FromSeconds(15);

    /// <summary>The wait before the next try of the connect loop after <paramref name="failures"/> failures in a row: 1 s, doubling, up to <see cref="RetryDelayCap"/>.</summary>
    internal static TimeSpan RetryDelay(int failures)
    {
        var doubled = TimeSpan.FromSeconds(1 << Math.Clamp(failures - 1, 0, 5));
        return doubled < RetryDelayCap ? doubled : RetryDelayCap;
    }


    /// <summary>
    /// Starts the connection, retrying until the coordinator answers, then announces the replica. The announcement comes first: a
    /// coordinator that accepts it knows the identity, and only then is the data plane client asked for, and announced in turn.
    /// </summary>
    private async Task ConnectAsync(HubConnection hub, CancellationToken ct)
    {
        var failures = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                // Nothing is announced on a connection that has not been announced on yet.
                _announced = false;
                if (hub.State == HubConnectionState.Disconnected) await hub.StartAsync(ct);
                var hadClient = _node is not null;
                await AnnounceAsync();
                if (!hadClient && await EnsureNodeAsync(ct)) await AnnounceAsync();
                return;
            }
            // Only the host's own stop ends the loop: an invocation the coordinator's close cancels
            // fails with an OperationCanceledException too.
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                _logger.LogInformation("Coordinator not reachable at {Url} yet, retrying ({Reason})", _options.CoordinatorUrl, ex.Message);
                if (hub.State != HubConnectionState.Disconnected)
                {
                    try { await hub.StopAsync(ct); } catch (Exception) when (!ct.IsCancellationRequested) { }
                }
                await Task.Delay(RetryDelay(++failures), ct);
            }
        }
    }

    /// <summary>
    /// Connects the replica's TransportR client, once it can: it registers with the coordinator's hub under this node's identity,
    /// and its client id is what the announcement carries, so the coordinator opens transfers to it. It is asked for only after an
    /// announcement the coordinator accepted: an identity it does not know, or has disabled, is refused there and could not register
    /// one. A replica announced without a client is placed no run, and <see cref="WatchDataPlaneAsync"/> connects the client when
    /// it can.
    /// </summary>
    private async Task<bool> EnsureNodeAsync(CancellationToken ct)
    {
        if (_node is not null) return true;
        await _connecting.WaitAsync(ct);
        try
        {
            if (_node is not null) return true;
            return await ConnectNodeAsync(ct);
        }
        finally
        {
            _connecting.Release();
        }
    }

    private async Task<bool> ConnectNodeAsync(CancellationToken ct)
    {
        // TransportR keeps the previous token when its provider throws, and a refused identity would then surface as a
        // bare 401 after its connection retries: ask once here so the provider's refusal is what gets reported.
        await _tokens.GetAsync(fresh: true);
        PipelineNode node;
        try
        {
            node = await PipelineNode.ConnectAsync(new NodeClientOptions
            {
                HubUrl = _options.CoordinatorUrl.TrimEnd('/'),
                AccessTokenProvider = () => _tokens.GetAsync(fresh: true),
            }, _forwarder.For(_hostLoggers, _options.Name, fragment: null), ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            _logger.LogInformation("The data plane did not register ({Reason}): announcing without it", ex.Message);
            return false;
        }

        // The channel's handlers are bound once, by the first run protocol; a later client goes to the one that exists.
        if (_runs is null)
        {
            var runs = new ReplicaRuns(
                _hub!, node, _options.Name, _options.DtPipe, _environment, _fragmentsDir,
                spec => FragmentIntake.JobOf(spec, _policy, _gate, _bricks.DeclaredBranch, _bricks.DeclaredVersion, _bricks.Forms),
                fragment => _forwarder.For(_hostLoggers, _options.Name, fragment).CreateLogger(typeof(FragmentRun).FullName!),
                _logger, _options.UnwiredRunTimeout, _options.FaultInjection);
            runs.Bind();
            _runs = runs;
        }
        else
        {
            _runs.UseNode(node);
        }
        _node = node;
        return true;
    }

    /// <summary>
    /// While the replica is announced, keeps its data plane registered: connects the client that could not register at the
    /// announcement, and registers again one the hub does not know (a coordinator that lost it), which TransportR would
    /// otherwise do at its next presence heartbeat. A client the hub does not know holds no registration to be stale, so the
    /// replica is placed no run on it meanwhile.
    /// </summary>
    private async Task WatchDataPlaneAsync(HubConnection hub, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(DataPlaneWatchInterval, ct);
                if (hub.State != HubConnectionState.Connected || !_announced) continue;
                try
                {
                    if (_node is not { } node)
                    {
                        if (await EnsureNodeAsync(ct)) await AnnounceAsync();
                    }
                    else if (await node.IsRegisteredAsync(ct) == false)
                    {
                        _logger.LogInformation("The hub does not know this replica's data plane: registering it again");
                        try
                        {
                            await node.RefreshRegistrationAsync(ct);
                        }
                        catch (HubException ex)
                        {
                            await DropDataPlaneAsync(node, ex, ct);
                            await AnnounceAsync();
                        }
                    }
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    _logger.LogDebug(ex, "The data plane could not be checked");
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    private static readonly TimeSpan DataPlaneWatchInterval = TimeSpan.FromSeconds(3);

    private async Task AnnounceAsync()
    {
        // Bounded: a connection the coordinator aborts as it refuses the announcement may never answer it.
        using var bounded = new CancellationTokenSource(AnnounceTimeout);
        try
        {
            await _hub!.InvokeAsync(NodeChannel.Announce,
                new NodeAnnouncement(_options.Name, _options.Description, _datasets, Role, _bricks.Bricks,
                    Capabilities: [.. _gate is null ? [] : new[] { NodeAnnouncement.RawFragments }],
                    DtpipeVersion: _dtpipeVersion, InstanceId: _instanceId, Origin: $"{Environment.MachineName}:{Environment.ProcessId}",
                    DataPlaneClientId: _node?.ClientId), bounded.Token);
        }
        catch (Exception)
        {
            _announced = false;
            throw;
        }
        _announced = true;

        // Accepted again: what could not be reported before is sent once.
        if (_runs is not null) await _runs.OnAnnouncedAsync();
    }

    /// <summary>
    /// The coordinator closes a replica's channel so that it takes its rights again; its TransportR client registered with the
    /// groups it had, and has the hub take the groups of the moment when asked. That is done at once, runs in flight or not: the
    /// client keeps its connection, and the hub ends the transfers the new groups refuse. A client whose connection is not up
    /// registers by itself when it is back, with the groups of that moment, so that refusal is not retried. A hub that refuses
    /// the refresh (<see cref="HubException"/>: the identity holds no group, so nothing is written and the old groups would
    /// stay) has the client dropped. Any other failure is logged and does not end the loop that connects the channel again.
    /// </summary>
    private async Task RefreshDataPlaneAsync(CancellationToken ct)
    {
        if (_node is not { } node) return;
        try
        {
            await node.RefreshRegistrationAsync(ct);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogDebug(ex, "The data plane's connection is not up: it registers by itself when it is back");
        }
        catch (HubException ex)
        {
            await DropDataPlaneAsync(node, ex, ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "The data plane could not be registered again");
        }
    }

    /// <summary>
    /// Closes the TransportR client the hub refused to renew, so the hub holds no connected registration with the groups the identity
    /// has lost; the runs keep going to their end without it, and announcements carry no client until <see cref="EnsureNodeAsync"/>
    /// registers another. Only the client that was refused is dropped: the loop that connects the channel and the watch can both
    /// meet the refusal, and the second finds it gone.
    /// </summary>
    private async Task DropDataPlaneAsync(PipelineNode node, HubException reason, CancellationToken ct)
    {
        await _connecting.WaitAsync(ct);
        try
        {
            if (!ReferenceEquals(_node, node)) return;
            _node = null;
            _runs?.UseNode(null);
        }
        finally
        {
            _connecting.Release();
        }
        _logger.LogWarning("The hub refused to renew the data plane registration ({Reason}): dropping its data plane client", reason.Message);
        await node.DisposeAsync();
    }

    /// <summary>
    /// An orderly stop says so before it closes: the coordinator ends this replica's runs at once, with no grace period to
    /// wait out, and then every run is thrown away, so no dtpipe child outlives the host.
    /// </summary>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        if (_hub is { State: HubConnectionState.Connected } hub)
        {
            try
            {
                using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                bounded.CancelAfter(TimeSpan.FromSeconds(2));
                await hub.InvokeAsync(NodeChannel.Leave, bounded.Token);
            }
            catch (Exception) { /* the coordinator's own disconnect handling covers a host that cannot say so */ }
        }
        if (_runs is not null) await _runs.DisposeAsync();
        if (_node is not null) await _node.DisposeAsync();
        if (_hub is not null) await _hub.DisposeAsync();
        // The directory belongs to this process alone, and nothing runs from it any more.
        try { Directory.Delete(_stateDir, recursive: true); } catch (Exception) { /* shutting down */ }
    }

    public override void Dispose()
    {
        _tokens.Dispose();
        base.Dispose();
    }

    private sealed class KeepRetrying : IRetryPolicy
    {
        public TimeSpan? NextRetryDelay(RetryContext retryContext) => TimeSpan.FromSeconds(2);
    }
}
