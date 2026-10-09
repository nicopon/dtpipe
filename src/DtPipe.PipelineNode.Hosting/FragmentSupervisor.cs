using System.Diagnostics;
using System.Security.Cryptography;
using DtPipe.Coordinator.Contracts;
using Microsoft.Extensions.Logging;
using Node = DtPipe.PipelineNode.PipelineNode;

namespace DtPipe.PipelineNode.Hosting;

/// <summary>
/// Keeps one fragment registered with the coordinator through a <see cref="Node"/>, and
/// reports what it does. A <see cref="Node"/> serves a single run - its child, pipes and
/// relay tasks are never reset - so <see cref="RearmAsync"/> replaces it with a fresh instance
/// rather than reusing it.
/// </summary>
internal sealed class FragmentSupervisor : IAsyncDisposable
{
    private readonly string _node;
    private readonly FragmentDeployment _deployment;
    private readonly string _jobPath;
    private readonly string _coordinatorUrl;
    private readonly string _dtpipe;
    private readonly IReadOnlyDictionary<string, string> _environment;
    private readonly Func<Task<string>> _accessToken;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Func<FragmentStatus, Task> _report;

    private Node? _pipelineNode;
    private CancellationTokenSource? _watch;
    private Task? _watchTask;

    public string Fragment => _deployment.Fragment;
    public string Key => KeyOf(_deployment.Fragment, _deployment.Instance);

    public static string KeyOf(string fragment, string instance) => $"{fragment}#{instance}";

    /// <summary>The same identity <see cref="Node"/> registers: SHA-256 of the job file's bytes.</summary>
    public string Version { get; }

    public FragmentStatus LastStatus { get; private set; }

    private string _generation;

    public FragmentSupervisor(
        string node, FragmentDeployment deployment, string jobPath, string coordinatorUrl, string dtpipe,
        IReadOnlyDictionary<string, string> environment, Func<Task<string>> accessToken,
        ILoggerFactory loggerFactory, Func<FragmentStatus, Task> report)
    {
        _accessToken = accessToken;
        _node = node;
        _deployment = deployment;
        _jobPath = jobPath;
        _coordinatorUrl = coordinatorUrl;
        _dtpipe = dtpipe;
        _environment = environment;
        _loggerFactory = loggerFactory;
        _report = report;
        Version = Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(jobPath)));
        _generation = deployment.Generation;
        LastStatus = new FragmentStatus(node, Fragment, deployment.Instance, _generation, FragmentState.Registering, Version, null, null, null);
    }

    public async Task ConnectAsync(CancellationToken ct)
    {
        await ReportAsync(FragmentState.Registering);
        try
        {
            // TransportR keeps the previous token when its provider throws, and a refused identity
            // would then surface as a bare 401 after its connection retries: ask once here so the
            // provider's refusal is what gets reported.
            await _accessToken();
            _pipelineNode = await Node.ConnectAsync(new PipelineNodeOptions
            {
                HubUrl = _coordinatorUrl,
                // The coordinator decides this node's group from the identity its token names.
                AccessTokenProvider = _accessToken,
                DtPipeExecutable = _dtpipe,
                ChildEnvironment = _environment,
                FragmentJobPath = _jobPath,
                Edges = _deployment.Edges
                    .Select(e => new EdgeBinding(e.Alias,
                        e.Direction == FragmentEdgeDirection.Inbound ? EdgeDirection.Inbound : EdgeDirection.Outbound))
                    .ToList(),
            }, Fragment, _loggerFactory, ct);
        }
        catch (Exception ex)
        {
            await ReportAsync(FragmentState.Failed, message: $"registration failed: {ex.Message}");
            return;
        }

        await ReportAsync(FragmentState.Registered, clientId: _pipelineNode.ClientId);
        _watch = new CancellationTokenSource();
        _watchTask = WatchAsync(_pipelineNode, _watch.Token);
    }

    public async Task RearmAsync(string generation, CancellationToken ct)
    {
        await StopAsync();
        _generation = generation;
        await ConnectAsync(ct);
    }

    /// <summary>Fault injection: kills the running child outright, the way a crashed host would.</summary>
    public bool KillChild()
    {
        if (_pipelineNode is not { IsLaunched: true, ChildHasExited: false } node) return false;
        try
        {
            Process.GetProcessById(node.ChildProcessId).Kill(entireProcessTree: false);
            return true;
        }
        catch (Exception) { return false; }
    }

    private async Task WatchAsync(Node node, CancellationToken ct)
    {
        try
        {
            while (!node.IsLaunched) await Task.Delay(100, ct);
            await ReportAsync(FragmentState.Launched, clientId: node.ClientId, message: $"pid {node.ChildProcessId}");

            var exitCode = await node.Completion.WaitAsync(ct);
            await ReportAsync(exitCode == 0 ? FragmentState.Exited : FragmentState.Failed,
                clientId: node.ClientId, exitCode: exitCode, message: FormatCounts(node) + FormatFault(node));
        }
        catch (OperationCanceledException) { }
    }

    private static string FormatCounts(Node node) =>
        string.Join(", ", node.RowCounts.Select(kv => $"{kv.Key}={kv.Value:N0} rows"));

    private static string FormatFault(Node node) =>
        node.FirstFault is null ? "" : $" - {node.Origin}: {node.FirstFault}";

    private async Task StopAsync()
    {
        if (_watch is not null)
        {
            await _watch.CancelAsync();
            if (_watchTask is not null) await _watchTask;
            _watch.Dispose();
            _watch = null;
        }
        if (_pipelineNode is not null)
        {
            await _pipelineNode.DisposeAsync();
            _pipelineNode = null;
        }
    }

    private Task ReportAsync(FragmentState state, Guid? clientId = null, int? exitCode = null, string? message = null)
    {
        LastStatus = new FragmentStatus(_node, Fragment, _deployment.Instance, _generation, state, Version, clientId, exitCode, message);
        return _report(LastStatus);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await ReportAsync(FragmentState.Undeployed);
        _loggerFactory.Dispose();
    }
}
