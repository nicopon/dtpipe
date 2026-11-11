namespace DtPipe.PipelineNode;

/// <summary>
/// What one run of a fragment needs: the job its dtpipe child runs, the edges it declares, and how the child is
/// started. Nothing of the replica that hosts it, nor of the coordinator that asks.
/// </summary>
public sealed class FragmentLaunch
{
    public required string DtPipeExecutable { get; init; }
    public required string FragmentJobPath { get; init; }
    public required IReadOnlyList<EdgeBinding> Edges { get; init; }

    /// <summary>Arrow IPC buffer compression codec used by the fragment child process ('none', 'lz4' or 'zstd'), passed as <c>--bind-compression</c>. Defaults to 'lz4'.</summary>
    public string ArrowCompression { get; init; } = "lz4";

    /// <summary>Arrow IPC buffer compression level used by the fragment child process, or null for codec default.</summary>
    public int? ArrowCompressionLevel { get; init; }

    /// <summary>Variables set in the child's environment on top of this process's own, which it
    /// inherits: what a fragment resolves through <c>${{NAME}}</c>. The host's own environment is
    /// never changed for them.</summary>
    public IReadOnlyDictionary<string, string>? ChildEnvironment { get; init; }

    /// <summary>The size of the chunks read from the child's stdout onto a transfer.</summary>
    public int ReadChunkBytes { get; init; } = 65536;
}

/// <summary>How a run ended: the child's exit code (1 when the fragment faulted), where the first fault came from, and the rows each edge carried.</summary>
public sealed record FragmentOutcome(int ExitCode, FaultOrigin? Origin, string? FirstFault, IReadOnlyDictionary<string, long> RowCounts);
