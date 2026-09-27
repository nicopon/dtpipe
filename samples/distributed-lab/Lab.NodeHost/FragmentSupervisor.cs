using System.Diagnostics;
using System.Security.Cryptography;
using DtPipe.Lab.Contracts;
using DtPipe.PipelineNode;
using Microsoft.Extensions.Logging;

namespace DtPipe.Lab.NodeHost;

/// <summary>
/// Keeps one fragment registered with the coordinator through a <see cref="PipelineNode"/>, and
/// reports what it does. A <see cref="PipelineNode"/> serves a single run - its child, pipes and
/// relay tasks are never reset - so <see cref="RearmAsync"/> replaces it with a fresh instance
/// rather than reusing it.
/// </summary>
public sealed class FragmentSupervisor : IAsyncDisposable
{
    private readonly string _node;
    private readonly FragmentDeployment _deployment;
    private readonly string _jobPath;
    private readonly NodeHostOptions _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Func<FragmentStatus, Task> _report;

    private PipelineNode.PipelineNode? _pipelineNode;
    private CancellationTokenSource? _watch;
    private Task? _watchTask;

    public string Fragment => _deployment.Fragment;
    public string Key => KeyOf(_deployment.Fragment, _deployment.Instance);

    public static string KeyOf(string fragment, string instance) => $"{fragment}#{instance}";

    /// <summary>The same identity <see cref="PipelineNode.PipelineNode"/> registers: SHA-256 of the job file's bytes.</summary>
    public string Version { get; }

    public FragmentStatus LastStatus { get; private set; }

    private string _generation;

    public FragmentSupervisor(
        string node, FragmentDeployment deployment, string jobPath, NodeHostOptions options,
        ILoggerFactory loggerFactory, Func<FragmentStatus, Task> report)
    {
        _node = node;
        _deployment = deployment;
        _jobPath = jobPath;
        _options = options;
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
            _pipelineNode = await PipelineNode.PipelineNode.ConnectAsync(new PipelineNodeOptions
            {
                HubUrl = _options.CoordinatorUrl,
                DtPipeExecutable = _options.DtPipeExecutable,
                FragmentJobPath = _jobPath,
                Edges = _deployment.Edges
                    .Select(e => new EdgeBinding(e.Alias,
                        e.Direction == LabEdgeDirection.Inbound ? EdgeDirection.Inbound : EdgeDirection.Outbound))
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

    /// <summary>Fault injection for the demonstrator: kills the running child outright, the way a crashed host would.</summary>
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

    private async Task WatchAsync(PipelineNode.PipelineNode node, CancellationToken ct)
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

    private static string FormatCounts(PipelineNode.PipelineNode node) =>
        string.Join(", ", node.RowCounts.Select(kv => $"{kv.Key}={kv.Value:N0} rows"));

    private static string FormatFault(PipelineNode.PipelineNode node) =>
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
