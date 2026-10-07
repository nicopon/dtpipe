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

    // Coordinator -> node host, answered: a request a host registered under a name of its own (it
    // announces the names in NodeAnnouncement.Capabilities). The coordinator's lab mode asks a lab host to
    // query a dataset and to cut a branch this way; a host that registered nothing answers that it does not.
    public const string Request = nameof(Request);

    // Coordinator -> runner, answered: the dtpipe runs the coordinator no longer makes itself. A designer's
    // job checked over one-row samples with its writers neutralised, and a command line turned into a job.
    // Only a host that runs any fragment answers them.
    public const string ValidateJob = nameof(ValidateJob);
    public const string ExportJob = nameof(ExportJob);
}

/// <summary>
/// A variable a node describes, reachable from a fragment's job through <c>${{Variable}}</c>. Its value
/// never leaves the node; <see cref="Engine"/> says it is a database the node queries for the
/// coordinator's lab mode, and is empty for any other variable.
/// </summary>
public sealed record DatasetInfo(string Variable, string Engine, string Description);

/// <summary>A data node hosts databases and offers bricks; a runner hosts neither and carries every step between them.</summary>
public enum NodeRole { Data, Runner }

/// <summary>
/// What a node host says of itself. <see cref="Name"/> and <see cref="Description"/> are proposals: the
/// coordinator names the node from its rights, and a host its rights do not know only waits for
/// approval. <see cref="Capabilities"/> only report how the host was configured: <see cref="RawFragments"/>
/// when its own configuration lets it run a fragment that is no brick, and the name of each request it
/// answers. The coordinator shows them and never sets them; the host enforces what it offers.
/// </summary>
public sealed record NodeAnnouncement(
    string Name, string Description, IReadOnlyList<DatasetInfo> Datasets, NodeRole Role,
    IReadOnlyList<BrickInfo> Bricks, IReadOnlyList<string>? Capabilities = null)
{
    /// <summary>The capability of a host that accepts a fragment written as a job, not only one made of its bricks.</summary>
    public const string RawFragments = "raw-fragments";

    public bool Offers(string capability) => Capabilities?.Contains(capability, StringComparer.Ordinal) == true;
}

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

/// <summary>
/// What <c>ValidateJob</c> asks of a runner: dry-run <see cref="Job"/> over one row per source, each source
/// alias bound to its Arrow sample, each sink alias bound to a throwaway file and its contract saved
/// (<see cref="ContractFile"/>) for the coordinator to send on to the sink's own node.
/// </summary>
public sealed record JobCheck(string Job, IReadOnlyDictionary<string, byte[]> Samples, IReadOnlyList<string> Sinks)
{
    public static string SampleFile(string alias) => alias + ".arrows";

    public static string ContractFile(string alias) => alias + ".contract.json";
}

/// <summary>What <c>ValidateJob</c> answers: whether dtpipe accepts the job, its report when it does not, and each sink's contract by alias.</summary>
public sealed record JobCheckResult(bool Ok, string Report, IReadOnlyDictionary<string, string> Contracts);

/// <summary>What <c>ExportJob</c> answers: the job dtpipe's <c>--export-job</c> wrote for a command line, or why it did not.</summary>
public sealed record ExportedJob(string? Yaml, string? Error);

/// <summary>What a <c>Request</c> answers: the handler's payload (JSON), or why the host does not answer it.</summary>
public sealed record NodeResponse(string? Payload, string? Error);

/// <summary>What the lab's <c>lab.query</c> request answers: CSV text, or why the node did not or could not read it.</summary>
public sealed record DatasetQueryResult(string? Csv, string? Error);

/// <summary>
/// What the lab's <c>lab.split</c> request answers: the cut branch's producer and consumer halves, each a one-branch
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
