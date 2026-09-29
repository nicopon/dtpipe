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

    // Coordinator -> node host, answered: a few rows of a brick, read by the node that owns it.
    public const string PreviewBrick = nameof(PreviewBrick);
}

/// <summary>A database a node hosts, reachable from a fragment's job through <c>${{Variable}}</c>.</summary>
public sealed record DatasetInfo(string Variable, string Engine, string Path, string Description);

/// <summary>A data node hosts databases and offers bricks; a runner hosts neither and carries every step between them.</summary>
public enum NodeRole { Data, Runner }

/// <summary>
/// <see cref="Sandbox"/> only reports how the host was started: a data node runs a fragment only if
/// each branch is one of its bricks, unless its own start command said otherwise. The coordinator
/// shows it and never sets it.
/// </summary>
public sealed record NodeAnnouncement(
    string Name, string Group, string Description, IReadOnlyList<DatasetInfo> Datasets, NodeRole Role,
    IReadOnlyList<BrickInfo> Bricks, bool Sandbox = false);

public enum BrickKind { Source, Sink }

public sealed record BrickColumn(string Name, string Type, bool IsNullable);

/// <summary>
/// A read or a write the node's owner preconfigured on one of its databases. <see cref="Branch"/>
/// is the brick's piece of a dtpipe branch, as JSON text exactly as the node's file declares it: a
/// source's <c>input</c> and reader options, a sink's <c>output</c> and writer options. The node
/// inspects a source's schema itself, where the data is; <see cref="SchemaError"/> says why it
/// could not.
/// </summary>
public sealed record BrickInfo(
    string Id, BrickKind Kind, string Title, string Description, string Branch,
    IReadOnlyList<BrickColumn>? Schema, string? SchemaError);

/// <summary>What <c>PreviewBrick</c> answers: CSV text, or why the node could not read it.</summary>
public sealed record BrickPreview(string? Csv, string? Error);

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
