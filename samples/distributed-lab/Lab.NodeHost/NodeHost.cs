using System.Text;
using DtPipe.Lab.Contracts;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

namespace DtPipe.Lab.NodeHost;

/// <summary>
/// One lab node: announces the databases it hosts and the bricks it offers, and keeps every
/// fragment the coordinator deploys on it registered through its own <see cref="FragmentSupervisor"/>.
/// Commands are applied one at a time, in arrival order; a brick preview answers outside that queue.
/// </summary>
public sealed class NodeHost : IAsyncDisposable
{
    private readonly NodeHostOptions _options;
    private readonly NodeConfig _config;
    private readonly string _fragmentsDir;
    private readonly IReadOnlyList<DatasetInfo> _datasets;
    private readonly BrickHost _bricks;
    private readonly IdpTokenClient _idp;
    private readonly LabLogForwarder _forwarder = new();
    private readonly ILogger _logger;
    private readonly ILoggerFactory _hostLoggerFactory;
    private readonly Dictionary<string, FragmentSupervisor> _fragments = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _commands = new(1, 1);
    private HubConnection _hub = null!;

    public NodeHost(NodeHostOptions options, NodeConfig config)
    {
        _options = options;
        _config = config;

        var dataDir = Path.Combine(options.StateDir, config.Name);
        _fragmentsDir = Path.Combine(dataDir, "fragments");
        Directory.CreateDirectory(_fragmentsDir);

        // The dtpipe child inherits this process's environment, so a fragment written against
        // ${{LAB_CRM_DB}} resolves to this node's own copy of the database.
        _datasets = (config.Datasets ?? [])
            .Select(d => new DatasetInfo(d.Variable, d.Engine, Path.GetFullPath(Path.Combine(dataDir, d.File)), d.Description))
            .ToList();
        foreach (var dataset in _datasets)
            Environment.SetEnvironmentVariable(dataset.Variable, dataset.Path);

        _hostLoggerFactory = CreateLoggerFactory(fragment: null);
        _logger = _hostLoggerFactory.CreateLogger($"node.{config.Name}");
        _bricks = new BrickHost(config, options, _logger);
        _idp = new IdpTokenClient(options, config);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        // Authenticated by the coordinator's IDP: each (re)connection presents a current token.
        _hub = new HubConnectionBuilder()
            .WithUrl(_options.CoordinatorUrl + LabHubMethods.Path, o => o.AccessTokenProvider = async () => await _idp.GetAsync())
            .WithAutomaticReconnect(new KeepRetrying())
            .Build();

        _hub.On<FragmentDeployment>(LabHubMethods.Deploy, d => Enqueue(() => DeployAsync(d, ct)));
        _hub.On<string>(LabHubMethods.Undeploy, f => Enqueue(() => UndeployAsync(f)));
        _hub.On<string, string>(LabHubMethods.Rearm, (f, generation) => Enqueue(() => RearmAsync(f, generation, ct)));
        _hub.On<string>(LabHubMethods.Kill, f => Enqueue(() => KillAsync(f)));
        _hub.On<string, int, BrickPreview>(LabHubMethods.PreviewBrick, (id, rows) => _bricks.PreviewAsync(id, rows));
        _hub.Reconnected += async _ => await AnnounceAsync();

        // Schemas are inspected before the first announcement, so the coordinator never shows a
        // brick without the columns its node could read.
        await _bricks.InspectAsync(ct);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await _hub.StartAsync(ct);
                break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogInformation("Coordinator not reachable at {Url} yet, retrying ({Reason})", _options.CoordinatorUrl, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
        }

        await AnnounceAsync();
        _logger.LogInformation("Node {Name} ({Role}, {Bricks} brick(s)) connected to {Url} as {ClientId}",
            _config.Name, _config.Role, _bricks.Bricks.Count, _options.CoordinatorUrl, _idp.ClientId);

        await foreach (var line in _forwarder.Reader.ReadAllAsync(ct))
        {
            if (_hub.State != HubConnectionState.Connected) continue;
            try { await _hub.SendAsync(LabHubMethods.ReportLog, line, ct); }
            catch (Exception ex) when (ex is not OperationCanceledException) { }
        }
    }

    private async Task AnnounceAsync()
    {
        await _hub.InvokeAsync(LabHubMethods.Announce,
            new NodeAnnouncement(_config.Name, "", _config.Description, _datasets, _config.Role, _bricks.Bricks));

        FragmentStatus[] statuses;
        await _commands.WaitAsync();
        try { statuses = _fragments.Values.Select(f => f.LastStatus).ToArray(); }
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

        var jobPath = Path.Combine(_fragmentsDir, SafeFileName(key) + ".yaml");
        await File.WriteAllTextAsync(jobPath, deployment.Yaml, new UTF8Encoding(false), ct);

        var supervisor = new FragmentSupervisor(
            _config.Name, deployment, jobPath, _options, _idp.HubUrlAsync, CreateLoggerFactory(deployment.Fragment), ReportAsync);
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
        if (_hub.State != HubConnectionState.Connected) return;
        try { await _hub.SendAsync(LabHubMethods.ReportFragment, status); }
        catch (Exception ex) { _logger.LogDebug(ex, "Status report dropped"); }
    }

    /// <summary>
    /// The pipeline node's own logs, including the child's stderr (logged at Debug), go to the
    /// coordinator; the console keeps Information and up. TransportR stays at Warning.
    /// </summary>
    private ILoggerFactory CreateLoggerFactory(string? fragment) => LoggerFactory.Create(builder =>
    {
        builder.SetMinimumLevel(LogLevel.Debug);
        builder.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
        builder.AddProvider(_forwarder.CreateProvider(_config.Name, fragment));
        builder.AddFilter((provider, category, level) =>
        {
            if (category?.StartsWith("TransportR", StringComparison.Ordinal) == true
                || category?.StartsWith("Microsoft", StringComparison.Ordinal) == true)
                return level >= LogLevel.Warning;
            return provider == typeof(ConsoleLoggerProvider).FullName ? level >= LogLevel.Information : true;
        });
    });

    private static string SafeFileName(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' or '.' ? c : '_'));

    public async ValueTask DisposeAsync()
    {
        foreach (var supervisor in _fragments.Values)
        {
            try { await supervisor.DisposeAsync(); } catch { /* shutting down */ }
        }
        _fragments.Clear();
        if (_hub is not null) await _hub.DisposeAsync();
        _idp.Dispose();
        _hostLoggerFactory.Dispose();
    }

    private sealed class KeepRetrying : IRetryPolicy
    {
        public TimeSpan? NextRetryDelay(RetryContext retryContext) => TimeSpan.FromSeconds(2);
    }
}
