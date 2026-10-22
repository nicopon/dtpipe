using System.Runtime.CompilerServices;
using DtPipe.PipelineNode.Launcher;

namespace DtPipe.Cli.Node;

/// <summary>
/// The one place the CLI names the node assembly. A method that mentions its types is compiled only when it is called, so
/// the assembly, and the transport library behind it, load when a node starts and never for any other command.
/// </summary>
internal static class NodeHostEntry
{
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static Task<int> RunAsync(string? name, string? coordinator, string? description, string? clientSecret, string dtpipe,
        IReadOnlyList<DeclaredBrick> bricks, CancellationToken ct) =>
        NodeLauncher.RunAsync(
            new LaunchSettings(name, coordinator, description, clientSecret, dtpipe),
            [.. bricks.Select(b => new LaunchedBrick(b.Id, b.IsSource, b.BranchJson))],
            ct);
}
