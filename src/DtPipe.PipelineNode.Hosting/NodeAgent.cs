using System.Text;
using System.Threading.Channels;
using DtPipe.Coordinator.Contracts;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// One node: announces the variables it describes and the bricks it offers over the coordinator's
/// node channel, and keeps every fragment the coordinator deploys on it registered through its own
/// <see cref="FragmentSupervisor"/>. Commands are applied one at a time, in arrival order; a brick
/// preview answers outside that queue. Whatever the coordinator asks, a fragment runs only under the
/// node's own <see cref="FragmentPolicy"/>, read from its configuration and never from a message.
/// </summary>
internal sealed class NodeAgent : BackgroundService
{
    private readonly DtPipeNodeOptions _options;
    private readonly FragmentPolicy _policy;
    private readonly string _stateDir;
    private readonly string _fragmentsDir;
    private readonly IReadOnlyList<DatasetInfo> _datasets;
    private readonly IReadOnlyDictionary<string, string> _environment;
    private readonly BrickHost _bricks;
    private readonly OidcTokenClient _tokens;
    private readonly NodeLogForwarder _forwarder = new();
    private readonly ILoggerFactory _hostLoggers;
    private readonly ILogger _logger;
    private readonly Dictionary<string, FragmentSupervisor> _fragments = new(StringComparer.Ordinal);
    private readonly Dictionary<string, FragmentStatus> _refusals = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _commands = new(1, 1);
    private HubConnection? _hub;

    // Signalled by every close of the connection; one loop reads it and connects again.
    private readonly Channel<bool> _closed = Channel.CreateBounded<bool>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    public NodeAgent(IOptions<DtPipeNodeOptions> options, NodeDefinition definition, ILoggerFactory loggers)
    {
        _options = options.Value;
        _policy = definition.Policy ?? _options.Policy;
        _stateDir = _options.ResolvedStateDirectory;
        _fragmentsDir = Path.Combine(_stateDir, "fragments");
        Directory.CreateDirectory(_fragmentsDir);

        var environment = new Dictionary<string, string>(_options.Variables, StringComparer.Ordinal);
        foreach (var variable in definition.Variables) environment[variable.Name] = variable.Value;
        _environment = environment;

        // A value is announced only for a dataset the coordinator may open itself; every other value
        // stays on the node, only its name is described.
        _datasets = definition.Variables
            .Select(v => new DatasetInfo(v.Name, v.Engine ?? "", v.Engine is null ? "" : v.Value, v.Description ?? ""))
            .Concat(_options.Variables.Keys.Where(k => definition.Variables.All(v => v.Name != k)).Select(k => new DatasetInfo(k, "", "", "")))
            .ToList();

        _hostLoggers = loggers;
        _logger = _forwarder.For(loggers, _options.Name, fragment: null).CreateLogger(typeof(NodeAgent).FullName!);
        _bricks = new BrickHost(_options.Name, definition.Bricks, _options.DtPipe, _stateDir, _environment, _logger);
        _tokens = new OidcTokenClient(_options.Oidc);
    }

    private NodeRole Role => _policy == FragmentPolicy.AnyFragment ? NodeRole.Runner : NodeRole.Data;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var hub = _hub = new HubConnectionBuilder()
            .WithUrl(_options.CoordinatorUrl.TrimEnd('/') + NodeChannel.Path, o => o.AccessTokenProvider = async () => await _tokens.GetAsync())
            .WithAutomaticReconnect(new KeepRetrying())
            .Build();

        hub.On<FragmentDeployment>(NodeChannel.Deploy, d => Enqueue(() => DeployAsync(d, ct)));
        hub.On<string>(NodeChannel.Undeploy, f => Enqueue(() => UndeployAsync(f)));
        hub.On<string, string>(NodeChannel.Rearm, (f, generation) => Enqueue(() => RearmAsync(f, generation, ct)));
        hub.On<string>(NodeChannel.Kill, f => Enqueue(() => KillAsync(f)));
        hub.On<string, int, BrickPreview>(NodeChannel.PreviewBrick, (id, rows) => _bricks.PreviewAsync(id, rows));
        hub.Reconnected += async _ => await AnnounceAsync();
        // A connection the coordinator closes (a reconnect it asked for, an announcement it refused)
        // is not retried by SignalR's own policy: the reconnect loop starts it again, so a node the
        // coordinator accepts once more comes back without a restart of its host. The handler only
        // signals: starting the connection from inside it races the next close.
        hub.Closed += _ =>
        {
            _closed.Writer.TryWrite(true);
            return Task.CompletedTask;
        };

        // Schemas are inspected before the first announcement, so the coordinator never shows a
        // brick without the columns its node could read.
        await _bricks.InspectAsync(ct);

        await ConnectAsync(hub, ct);
        _logger.LogInformation("Node {Name} ({Role}, {Bricks} brick(s), {Policy}) connected to {Url} as {ClientId}",
            _options.Name, Role, _bricks.Bricks.Count, _policy, _options.CoordinatorUrl, _tokens.ClientId);
        _ = ReconnectLoopAsync(hub, ct);

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
                await ConnectAsync(hub, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan AnnounceTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Starts the connection, retrying until the coordinator answers, then announces the node.</summary>
    private async Task ConnectAsync(HubConnection hub, CancellationToken ct)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                if (hub.State == HubConnectionState.Disconnected) await hub.StartAsync(ct);
                await AnnounceAsync();
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
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
        }
    }

    private async Task AnnounceAsync()
    {
        // Bounded: a connection the coordinator aborts as it refuses the announcement may never answer it.
        using var bounded = new CancellationTokenSource(AnnounceTimeout);
        await _hub!.InvokeAsync(NodeChannel.Announce,
            new NodeAnnouncement(_options.Name, "", _options.Description, _datasets, Role, _bricks.Bricks,
                Sandbox: _policy == FragmentPolicy.AnyReadOrWrite), bounded.Token);

        FragmentStatus[] statuses;
        await _commands.WaitAsync();
        try { statuses = _fragments.Values.Select(f => f.LastStatus).Concat(_refusals.Values).ToArray(); }
        finally { _commands.Release(); }
        foreach (var status in statuses) await ReportAsync(status);
    }

    private Task Enqueue(Func<Task> command) => Task.Run(async () =>
    {
        await _commands.WaitAsync();
        try { await command(); }
        catch (Exception ex) { _logger.LogError(ex, "Command failed"); }
        finally { _commands.Release(); }
    });

    private async Task DeployAsync(FragmentDeployment deployment, CancellationToken ct)
    {
        var key = FragmentSupervisor.KeyOf(deployment.Fragment, deployment.Instance);
        if (_fragments.Remove(key, out var previous))
            await previous.DisposeAsync();
        _refusals.Remove(key);

        // Before anything registers: a registered instance is admissible, and admission is the
        // coordinator's to grant, not this node's to leave open.
        if (_policy != FragmentPolicy.AnyFragment
            && BrickRules.FragmentRefusal(deployment.Yaml, _bricks.Forms, requireBricks: _policy == FragmentPolicy.Bricks) is { } reason)
        {
            _logger.LogWarning("Refusing {Fragment}: {Reason}", key, reason);
            var refusal = new FragmentStatus(_options.Name, deployment.Fragment, deployment.Instance, deployment.Generation,
                FragmentState.Failed, null, null, null, $"refused by {_options.Name}: {reason}");
            _refusals[key] = refusal;
            await ReportAsync(refusal);
            return;
        }

        var jobPath = Path.Combine(_fragmentsDir, SafeFileName(key) + ".yaml");
        await File.WriteAllTextAsync(jobPath, deployment.Yaml, new UTF8Encoding(false), ct);

        var supervisor = new FragmentSupervisor(
            _options.Name, deployment, jobPath, _options.CoordinatorUrl.TrimEnd('/'), _options.DtPipe, _environment,
            () => _tokens.GetAsync(fresh: true), _forwarder.For(_hostLoggers, _options.Name, deployment.Fragment), ReportAsync);
        _fragments[key] = supervisor;
        _logger.LogInformation("Deploying {Fragment} ({Edges})", key,
            string.Join(", ", deployment.Edges.Select(e => $"{e.Direction} {e.Alias}")));
        await supervisor.ConnectAsync(ct);
    }

    /// <summary>Every instance of <paramref name="fragment"/> on this node, or every fragment for <c>*</c>.</summary>
    private List<FragmentSupervisor> Matching(string fragment) =>
        _fragments.Values.Where(s => fragment == "*" || s.Fragment == fragment).ToList();

    private async Task UndeployAsync(string fragment)
    {
        foreach (var supervisor in Matching(fragment))
        {
            _fragments.Remove(supervisor.Key);
            await supervisor.DisposeAsync();
        }
        foreach (var (key, refusal) in _refusals.Where(r => fragment == "*" || r.Value.Fragment == fragment).ToList())
        {
            _refusals.Remove(key);
            await ReportAsync(refusal with { State = FragmentState.Undeployed });
        }
    }

    private async Task RearmAsync(string fragment, string generation, CancellationToken ct)
    {
        foreach (var supervisor in Matching(fragment))
            await supervisor.RearmAsync(generation, ct);
    }

    private Task KillAsync(string fragment)
    {
        var killed = Matching(fragment).Count(s => s.KillChild());
        _logger.LogWarning("Killed {Count} dtpipe child(ren) of {Fragment}", killed, fragment);
        return Task.CompletedTask;
    }

    private async Task ReportAsync(FragmentStatus status)
    {
        if (_hub is not { State: HubConnectionState.Connected } hub) return;
        try { await hub.SendAsync(NodeChannel.ReportFragment, status); }
        catch (Exception ex) { _logger.LogDebug(ex, "Status report dropped"); }
    }

    private static string SafeFileName(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' or '.' ? c : '_'));

    /// <summary>An orderly stop disposes every fragment first, so no dtpipe child outlives the host.</summary>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        await _commands.WaitAsync(cancellationToken);
        try
        {
            foreach (var supervisor in _fragments.Values)
            {
                try { await supervisor.DisposeAsync(); } catch { /* shutting down */ }
            }
            _fragments.Clear();
        }
        finally
        {
            _commands.Release();
        }
        if (_hub is not null) await _hub.DisposeAsync();
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
