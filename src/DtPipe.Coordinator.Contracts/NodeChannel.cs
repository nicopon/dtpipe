namespace DtPipe.Coordinator.Contracts;

/// <summary>
/// The control channel between a node host and the coordinator, a SignalR hub separate from the
/// coordinator's TransportR hub: it places fragments on hosts and reports what they do. Run
/// admission, launch and wiring never travel here; they belong to the TransportR hub.
/// </summary>
public static class NodeChannel
{
    public const string Path = "/node-channel";

    // Node host -> coordinator.
    public const string Announce = nameof(Announce);
    public const string ReportFragment = nameof(ReportFragment);
    public const string ReportLog = nameof(ReportLog);

    // Coordinator -> node host. Kill is fault injection: it ends a fragment's child outright.
    public const string Deploy = nameof(Deploy);
    public const string Undeploy = nameof(Undeploy);
    public const string Rearm = nameof(Rearm);
    public const string Kill = nameof(Kill);

    // Coordinator -> node host, answered: a few rows of a brick, read by the node that owns it.
    public const string PreviewBrick = nameof(PreviewBrick);

    // Coordinator -> node host, answered: what checking a designer's job needs from a node, which
    // never lets the coordinator open its database. A source brick's first rows as its fragment would
    // send them; a sink brick's verdict on the schema that would reach it.
    public const string SampleBrick = nameof(SampleBrick);
    public const string CheckSink = nameof(CheckSink);

    // Coordinator -> node host, answered: a query on one of the node's datasets, for the coordinator's
    // lab mode. The node answers it only if its own configuration says so.
    public const string QueryDataset = nameof(QueryDataset);

    // Coordinator -> node host, answered: dtpipe split of one branch that reads the node's data, for
    // the Lab view's cuts; a lab request too (BranchCut).
    public const string SplitBranch = nameof(SplitBranch);
}

/// <summary>A database a node hosts, reachable from a fragment's job through <c>${{Variable}}</c>.</summary>
public sealed record DatasetInfo(string Variable, string Engine, string Path, string Description);

/// <summary>A data node hosts databases and offers bricks; a runner hosts neither and carries every step between them.</summary>
public enum NodeRole { Data, Runner }

/// <summary>
/// <see cref="Sandbox"/> only reports how the host was configured: a data node runs a fragment only
/// if each branch is one of its bricks, unless its own configuration accepts any read or write. The
/// coordinator shows it and never sets it.
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

/// <summary>
/// What <c>SampleBrick</c> answers: the source brick's first rows as an Arrow IPC stream, written by
/// the same <c>arrow:-</c> writer its fragment sends the runner, or why the node could not read them.
/// </summary>
public sealed record BrickSample(byte[]? Arrow, string? Error);

/// <summary>What <c>CheckSink</c> answers: whether the sink brick's target accepts a contract, and dtpipe's report.</summary>
public sealed record SinkCheck(bool Accepted, string Report);

/// <summary>What <c>QueryDataset</c> answers: CSV text, or why the node did not or could not read it.</summary>
public sealed record DatasetQueryResult(string? Csv, string? Error);

/// <summary>
/// What <c>SplitBranch</c> answers: the cut branch's producer and consumer halves, each a one-branch
/// job (<see cref="BranchCut.Half"/>), or why the node did not or could not cut it.
/// </summary>
public sealed record BranchSplit(string? Producer, string? Consumer, string? Error);

public enum FragmentEdgeDirection { Inbound, Outbound }

/// <summary>
/// One frontier of a fragment. Order matters: the first edge of each direction rides the child's
/// stdin or stdout, every further one a named pipe (<c>DtPipe.PipelineNode.EdgeBinding</c>).
/// </summary>
public sealed record FragmentEdge(string Alias, FragmentEdgeDirection Direction);

/// <summary>
/// <see cref="Instance"/> tells two deployments of one fragment name apart on the same node: a
/// second instance is how admission's refusal of misaligned versions is shown and tested.
/// <see cref="Generation"/> is echoed in every <see cref="FragmentStatus"/>, and renewed by each
/// <c>Rearm</c>, so the coordinator never reads a status that predates its last command as current.
/// </summary>
public sealed record FragmentDeployment(
    string Fragment, string Yaml, IReadOnlyList<FragmentEdge> Edges, string Generation, string Instance = "main");

public enum FragmentState { Registering, Registered, Launched, Exited, Failed, Undeployed }

public sealed record FragmentStatus(
    string Node, string Fragment, string Instance, string Generation, FragmentState State, string? Version, Guid? ClientId,
    int? ExitCode, string? Message);

public sealed record NodeLogLine(string Node, string? Fragment, string Level, string Message);
