namespace DtPipe.Lab.Contracts;

/// <summary>
/// The lab's own control channel between a node host and the lab coordinator, separate from the
/// coordinator's TransportR hub: it carries only what the demonstrator needs to place fragments on
/// hosts and to show what they do. Run admission, launch and wiring never travel here.
/// </summary>
public static class LabHubMethods
{
    public const string Path = "/lab";

    // Node host -> coordinator.
    public const string Announce = nameof(Announce);
    public const string ReportFragment = nameof(ReportFragment);
    public const string ReportLog = nameof(ReportLog);

    // Coordinator -> node host.
    public const string Deploy = nameof(Deploy);
    public const string Undeploy = nameof(Undeploy);
    public const string Rearm = nameof(Rearm);
    public const string Kill = nameof(Kill);
}

/// <summary>A database a node hosts, reachable from a fragment's job through <c>${{Variable}}</c>.</summary>
public sealed record DatasetInfo(string Variable, string Engine, string Path, string Description);

public sealed record NodeAnnouncement(string Name, string Group, string Description, IReadOnlyList<DatasetInfo> Datasets);

public enum LabEdgeDirection { Inbound, Outbound }

/// <summary>
/// One frontier of a fragment. Order matters: the first edge of each direction rides the child's
/// stdin or stdout, every further one a named pipe (<c>DtPipe.PipelineNode.EdgeBinding</c>).
/// </summary>
public sealed record LabEdge(string Alias, LabEdgeDirection Direction);

/// <summary>
/// <see cref="Instance"/> tells two deployments of one fragment name apart on the same node: a
/// second instance is what lets the demonstrator show admission refusing misaligned versions.
/// <see cref="Generation"/> is echoed in every <see cref="FragmentStatus"/>, and renewed by each
/// <c>Rearm</c>, so the coordinator never reads a status that predates its last command as current.
/// </summary>
public sealed record FragmentDeployment(
    string Fragment, string Yaml, IReadOnlyList<LabEdge> Edges, string Generation, string Instance = "main");

public enum FragmentState { Registering, Registered, Launched, Exited, Failed, Undeployed }

public sealed record FragmentStatus(
    string Node, string Fragment, string Instance, string Generation, FragmentState State, string? Version, Guid? ClientId,
    int? ExitCode, string? Message);

public sealed record NodeLogLine(string Node, string? Fragment, string Level, string Message);
