using DtPipe.Coordinator.Contracts;
using DtPipe.PipelineNode.Hosting;

namespace DtPipe.PipelineNode.Tests.Infrastructure;

/// <summary>
/// A replica that hosts one run and is driven by no coordinator: it connects its TransportR client, launches the run's
/// child, and hands the run the transfer of an edge when the test says so. For the tests that exercise the relay
/// between a hub and a child, with the hub's transfers opened by the test itself.
/// </summary>
internal sealed class HandWiredNode : IAsyncDisposable
{
    private readonly PipelineNode _node;
    private readonly FragmentRun _run;

    private HandWiredNode(PipelineNode node, FragmentRun run)
    {
        _node = node;
        _run = run;
    }

    public Guid ClientId => _node.ClientId;
    public int ChildProcessId => _run.ChildProcessId;
    public bool ChildHasExited => _run.ChildHasExited;
    public IReadOnlyDictionary<string, long> RowCounts => _run.RowCounts;
    public string? FirstFault => _run.FirstFault;
    public FaultOrigin? Origin => _run.Origin;
    public IReadOnlyList<TransportR.Client.TransferInfo> Transfers => _node.Transfers;

    /// <summary>Connects, then launches: a hub that refuses the connection leaves no child behind.</summary>
    public static async Task<HandWiredNode> StartAsync(NodeClientOptions client, FragmentLaunch launch)
    {
        var node = await PipelineNode.ConnectAsync(client);
        try
        {
            return new HandWiredNode(node, node.Launch(launch));
        }
        catch
        {
            await node.DisposeAsync();
            throw;
        }
    }

    /// <summary>Hands the run the transfer of the edge <paramref name="alias"/>. A transfer that never reaches the node faults the run, and is an error here.</summary>
    public async Task WireAsync(string alias, string transferId, CancellationToken ct = default)
    {
        var lastEdge = await _node.WireAsync(_run, alias, transferId, ct);
        if (!lastEdge && _run.FirstFault is { } fault)
            throw new InvalidOperationException($"edge '{alias}' could not be wired: {fault}");
    }

    public Task<int> RunToCompletionAsync(CancellationToken ct = default) => _run.RunToCompletionAsync(ct);

    /// <summary>Runs to completion and returns the exit a replica would report for it, built by the one function a replica builds it with.</summary>
    public async Task<FragmentExit> ExitAsync(CancellationToken ct = default) =>
        ReplicaRuns.ExitOf("run-1", "fragment", "launch-token", await _run.CompleteAsync(ct), cursors: null);

    public async ValueTask DisposeAsync()
    {
        try { await _node.DisposeAsync(); }
        finally { await _run.DisposeAsync(); }
    }
}
