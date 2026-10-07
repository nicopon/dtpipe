namespace DtPipe.PipelineNode.Hosting;

/// <summary>Which fragments a node host agrees to run, whatever the coordinator sends.</summary>
public enum FragmentPolicy
{
    /// <summary>A data node: each branch only reads or writes, and is one of the node's bricks or a bare edge.</summary>
    Bricks,

    /// <summary>A data node in sandbox mode: each branch only reads or writes, through any reader or writer.</summary>
    AnyReadOrWrite,

    /// <summary>A runner: it hosts no data, and runs any fragment; network isolation is what contains it.</summary>
    AnyFragment,
}

/// <summary>How the node obtains its tokens: the client-credentials flow against an OpenID Connect provider.</summary>
public sealed class NodeOidcOptions
{
    /// <summary>The provider's issuer; its token endpoint is read from its discovery document.</summary>
    public string Authority { get; set; } = "";
    public string ClientId { get; set; } = "";
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

    /// <summary>Where fragment jobs are written and dtpipe runs; a directory per node under the temp directory by default.</summary>
    public string? StateDirectory { get; set; }

    public FragmentPolicy Policy { get; set; } = FragmentPolicy.Bricks;

    /// <summary>
    /// Whether the node answers the coordinator's lab-mode requests on its datasets (a query). Off by
    /// default: like <see cref="Policy"/>, the host's own word, never something the coordinator sends.
    /// </summary>
    public bool LabMode { get; set; }

    public NodeOidcOptions Oidc { get; set; } = new();

    /// <summary>Variables every dtpipe child sees, which a fragment resolves through <c>${{NAME}}</c>. Their values never leave the node.</summary>
    public Dictionary<string, string> Variables { get; set; } = new(StringComparer.Ordinal);

    internal string ResolvedStateDirectory =>
        Path.GetFullPath(StateDirectory ?? Path.Combine(Path.GetTempPath(), "dtpipe-node", Name));

    internal IEnumerable<string> Validate()
    {
        if (string.IsNullOrWhiteSpace(Name)) yield return "Name is required.";
        if (!Uri.TryCreate(CoordinatorUrl, UriKind.Absolute, out _)) yield return "CoordinatorUrl must be an absolute URL.";
        if (!Uri.TryCreate(Oidc.Authority, UriKind.Absolute, out _)) yield return "Oidc:Authority must be an absolute URL.";
        if (string.IsNullOrWhiteSpace(Oidc.ClientId)) yield return "Oidc:ClientId is required.";
        if (string.IsNullOrEmpty(Oidc.ClientSecret)) yield return "Oidc:ClientSecret is required.";
        if (string.IsNullOrWhiteSpace(DtPipe)) yield return "DtPipe is required.";
    }
}
