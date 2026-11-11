namespace DtPipe.PipelineNode.Hosting;

/// <summary>Which fragments a node host agrees to run, whatever the coordinator sends.</summary>
public enum FragmentPolicy
{
    /// <summary>A data node: each branch only reads or writes, and is one of the node's bricks or a bare edge, unless the host registered a <see cref="FragmentGate"/>.</summary>
    Bricks,

    /// <summary>A runner: it hosts no data, and runs any fragment whose every input and output is one of its arrow: edges (<c>RunnerEndpoints</c>), under <c>--edges-only</c>.</summary>
    AnyFragment,
}

/// <summary>How the node obtains its tokens: the client-credentials flow against an OpenID Connect provider.</summary>
public sealed class NodeOidcOptions
{
    /// <summary>
    /// The provider's issuer; its token endpoint is read from its discovery document. The provider the coordinator names in its
    /// protected resource metadata when left out.
    /// </summary>
    public string Authority { get; set; } = "";

    /// <summary>The node's client at the provider; the node's <see cref="DtPipeNodeOptions.Name"/> when left out.</summary>
    public string ClientId { get; set; } = "";

    /// <summary>The client's secret, or <c>@path</c> to the file that holds it (a coordinator's key, mounted in a container), read once at start.</summary>
    public string ClientSecret { get; set; } = "";

    /// <summary>Space-separated scopes to ask for, if the provider needs any.</summary>
    public string? Scope { get; set; }
}

/// <summary>
/// A node host's settings, bound from a configuration section (<c>DtPipeNode__Name</c>, …). Bricks
/// and described variables are added through <see cref="DtPipeNodeBuilder"/>.
/// </summary>
public sealed class DtPipeNodeOptions
{
    /// <summary>The name the host proposes for its node, and files its own logs under. The coordinator names the node from the rights, and shows that name.</summary>
    public string Name { get; set; } = "";
    public string Description { get; set; } = "";

    /// <summary>The coordinator's base URL: its node channel and its TransportR hub.</summary>
    public string CoordinatorUrl { get; set; } = "";

    /// <summary>The dtpipe binary every fragment runs as a child of this process; a bare name is looked up on the PATH.</summary>
    public string DtPipe { get; set; } = "dtpipe";

    /// <summary>
    /// The parent of the directory this process writes fragment jobs in and runs dtpipe from; a directory per node
    /// under the temp directory by default. Each process makes its own subdirectory under it, named by its instance
    /// id, so replicas that share a configuration never share a directory. Databases and variables are the host's
    /// own and do not live here.
    /// </summary>
    public string? StateDirectory { get; set; }

    public FragmentPolicy Policy { get; set; } = FragmentPolicy.Bricks;

    /// <summary>
    /// How long a run this host launched may wait for the coordinator to wire its edges before the host abandons it. The
    /// coordinator wires a run at once after launching it; a run still unwired after this has lost its coordinator, and
    /// would otherwise keep its dtpipe child until the host stops. Only the wait for the last <c>Wire</c> is bounded.
    /// </summary>
    public TimeSpan UnwiredRunTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Whether this host ends the child of a run when the coordinator asks it to (<c>Kill</c>), which a test bench does to inject a
    /// fault. Off by default: a host that did not enable it ends nothing, answers that it ended none and logs why. Only the host's own
    /// configuration enables it, never a message.
    /// </summary>
    public bool FaultInjection { get; set; }

    /// <summary>Arrow IPC buffer compression codec used by fragment child processes ('none', 'lz4' or 'zstd'), passed as <c>--bind-compression</c>. Defaults to 'lz4'.</summary>
    public string ArrowCompression { get; set; } = "lz4";

    /// <summary>Arrow IPC buffer compression level used by fragment child processes, or null for codec default.</summary>
    public int? ArrowCompressionLevel { get; set; }

    /// <summary>TransportR batch size in bytes for the data plane client. Defaults to 262144 (256 KB).</summary>
    public int BatchBytes { get; set; } = global::DtPipe.PipelineNode.NodeClientOptions.DefaultBatchBytes;

    /// <summary>Maximum in-flight TransportR batches before sender pause. Defaults to 16.</summary>
    public int MaxInFlightBatches { get; set; } = global::DtPipe.PipelineNode.NodeClientOptions.DefaultMaxInFlightBatches;

    /// <summary>Buffer size in bytes for reading/writing child fragment stdio pipes. Defaults to 262144 (256 KB).</summary>
    public int ReadChunkBytes { get; set; } = global::DtPipe.PipelineNode.NodeClientOptions.DefaultReadChunkBytes;

    public NodeOidcOptions Oidc { get; set; } = new();

    /// <summary>Variables every dtpipe child sees, which a fragment resolves through <c>${{NAME}}</c>. Their values never leave the node.</summary>
    public Dictionary<string, string> Variables { get; set; } = new(StringComparer.Ordinal);

    /// <summary>The directory of the process whose instance id is <paramref name="instanceId"/>: one per process, whatever the configuration.</summary>
    internal string ProcessDirectory(Guid instanceId) =>
        Path.Combine(Path.GetFullPath(StateDirectory ?? Path.Combine(Path.GetTempPath(), "dtpipe-node", Name)), instanceId.ToString("N"));

    private static readonly string[] ArrowCompressionValues = ["none", "lz4", "zstd"];

    internal IEnumerable<string> Validate()
    {
        if (string.IsNullOrWhiteSpace(Name)) yield return "Name is required.";
        if (!Uri.TryCreate(CoordinatorUrl, UriKind.Absolute, out _)) yield return "CoordinatorUrl must be an absolute URL.";
        if (!string.IsNullOrWhiteSpace(Oidc.Authority) && !Uri.TryCreate(Oidc.Authority, UriKind.Absolute, out _)) yield return "Oidc:Authority must be an absolute URL.";
        if (string.IsNullOrEmpty(Oidc.ClientSecret)) yield return "Oidc:ClientSecret is required.";
        if (string.IsNullOrWhiteSpace(DtPipe)) yield return "DtPipe is required.";
        if (!ArrowCompressionValues.Contains(ArrowCompression?.Trim().ToLowerInvariant() ?? ""))
            yield return $"ArrowCompression '{ArrowCompression}' is unknown. Expected one of: {string.Join(", ", ArrowCompressionValues)}.";
        if (UnwiredRunTimeout <= TimeSpan.Zero) yield return "UnwiredRunTimeout must be positive.";
        if (BatchBytes <= 0) yield return "BatchBytes must be positive.";
        if (MaxInFlightBatches <= 0) yield return "MaxInFlightBatches must be positive.";
        if (ReadChunkBytes <= 0) yield return "ReadChunkBytes must be positive.";
    }
}
