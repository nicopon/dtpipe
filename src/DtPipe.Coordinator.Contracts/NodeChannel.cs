using System.Text.Json.Serialization;

namespace DtPipe.Coordinator.Contracts;

/// <summary>
/// The control channel between a node host and the coordinator, a SignalR hub separate from the
/// coordinator's TransportR hub: it places runs on replicas, wires them and reports what they do. The
/// TransportR hub carries only the data plane. Everything the coordinator sends is an invocation the
/// host answers, never a push: the client handles pushes one after the other, so a push that takes
/// time delays the next. A handler answers as soon as it has received the message and never waits for
/// a run or for a transfer.
/// </summary>
public static class NodeChannel
{
    public const string Path = "/node-channel";

    // Node host -> coordinator. Who calls is the connection, never an argument. Exited is answered
    // (ExitedAck); Leave says this host is going away for good.
    public const string Announce = nameof(Announce);
    public const string Leave = nameof(Leave);
    public const string Exited = nameof(Exited);
    public const string ReportLog = nameof(ReportLog);

    // Coordinator -> node host, answered. Launch spawns a fragment's child (LaunchResult); Wire hands
    // the child an edge's transfer; Cancel ruptures a run; Check says whether the host would run a
    // fragment (FragmentCheck), spawning nothing. Kill is fault injection: it ends a run's child outright on a host that
    // enabled it, and the answer is how many children it ended.
    public const string Launch = nameof(Launch);
    public const string Wire = nameof(Wire);
    public const string Cancel = nameof(Cancel);
    public const string Check = nameof(Check);
    public const string Kill = nameof(Kill);

    // Coordinator -> node host, answered: a few rows of a brick, read by the node that owns it.
    public const string PreviewBrick = nameof(PreviewBrick);

    // Coordinator -> node host, answered: what checking a designer's job needs from a node, which
    // never lets the coordinator open its database. A source brick's first rows as its fragment would
    // send them; a sink brick's verdict on the schema that would reach it.
    public const string SampleBrick = nameof(SampleBrick);
    public const string CheckSink = nameof(CheckSink);

    // Coordinator -> node host, answered: whether a source brick still produces what a binding was checked against. The
    // node reads its sample as SampleBrick does and judges the contract it produces against the pinned one.
    public const string CheckSource = nameof(CheckSource);

    // Coordinator -> node host, answered: a request a host registered under a name of its own (it
    // announces the names in NodeAnnouncement.Capabilities). The coordinator's lab mode asks a lab host to
    // query a dataset and to cut a branch this way; a host that registered nothing answers that it does not.
    public const string Request = nameof(Request);

    // Coordinator -> runner, answered: the dtpipe run the coordinator no longer makes itself. A library
    // save's job checked over one-row samples with its writers neutralised. Only a host that runs any
    // fragment answers it.
    public const string ValidateJob = nameof(ValidateJob);
}

/// <summary>
/// A variable a node describes, reachable from a fragment's job through <c>${{Variable}}</c>. Its value
/// never leaves the node, and the coordinator learns nothing of what it points at.
/// </summary>
public sealed record DatasetInfo(string Variable, string Description);

/// <summary>A data node hosts databases and offers bricks; a runner hosts neither and carries every step between them.</summary>
public enum NodeRole { Data, Runner }

/// <summary>
/// What a node host says of itself. <see cref="Name"/> and <see cref="Description"/> are proposals: the
/// coordinator names the node from its rights, and a host its rights do not know only waits for
/// approval. <see cref="Capabilities"/> are what the host offers: <see cref="RawFragments"/> when its own
/// configuration lets it run a fragment that is no brick, and the name of each request it answers. Admission reads
/// <see cref="RawFragments"/> to tell the replicas that can run a fragment from the others. The coordinator shows them
/// and never sets them; the host enforces what it offers.
/// <see cref="DtpipeVersion"/> is what the host's dtpipe binary says of itself (<c>--version</c>), null
/// when it did not say: shown beside a studio's own, never a reason to refuse. One identity may be
/// carried by several hosts, its replicas: <see cref="InstanceId"/> is chosen by the host process once and
/// sent with every announcement, so a reconnection is told from a second replica; a host that sends none
/// is told apart by its connection. <see cref="Origin"/> says where the host runs (machine and process),
/// for people to read: untrusted text, never a ground for a decision. <see cref="DataPlaneClientId"/> is the
/// TransportR client the host moves a run's bytes with: the coordinator takes it only when that client is
/// registered under the same subject as the connection, and opens every transfer to it.
/// </summary>
public sealed record NodeAnnouncement(
    string Name, string Description, IReadOnlyList<DatasetInfo> Datasets, NodeRole Role,
    IReadOnlyList<BrickInfo> Bricks, IReadOnlyList<string>? Capabilities = null, string? DtpipeVersion = null,
    Guid? InstanceId = null, string? Origin = null, Guid? DataPlaneClientId = null)
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

/// <summary>
/// What <c>PreviewBrick</c> answers: CSV text, or why the node could not read it. For a source brick
/// <see cref="Columns"/> are the types of the Arrow the preview was read from, which the CSV cannot carry.
/// </summary>
public sealed record BrickPreview(string? Csv, string? Error, IReadOnlyList<BrickColumn>? Columns = null);

/// <summary>
/// What <c>SampleBrick</c> answers: the source brick's first rows as an Arrow IPC stream, written by
/// the same <c>arrow:-</c> writer its fragment sends the runner, or why the node could not read them.
/// <see cref="Contract"/> is the schema contract that read produced (the JSON of <c>contract-save</c>), absent from a node that
/// does not capture it.
/// </summary>
public sealed record BrickSample(byte[]? Arrow, string? Error, string? Contract = null);

/// <summary>What <c>CheckSource</c> answers: <see cref="SourceVerdicts"/> (<c>same</c>, <c>compatible</c>, <c>breaking</c> or <c>unread</c>) and dtpipe's report.</summary>
public sealed record SourceCheck(string Verdict, string Report);

/// <summary>The verdicts a node gives a source brick against its pinned contract.</summary>
public static class SourceVerdicts
{
    /// <summary>The contract it produces now hashes as the pinned one.</summary>
    public const string Same = "same";

    /// <summary>It differs, and still satisfies the consumers of the pinned one (<c>dtpipe contract diff</c>).</summary>
    public const string Compatible = "compatible";

    /// <summary>It differs and breaks them, or differs when the mode accepts no change.</summary>
    public const string Breaking = "breaking";

    /// <summary>The node could not tell: the sample or the comparison failed. Never a verdict on the brick.</summary>
    public const string Unread = "unread";
}

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

/// <summary>What a <c>Request</c> answers: the handler's payload (JSON), or why the host does not answer it.</summary>
public sealed record NodeResponse(string? Payload, string? Error);

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

/// <summary>What a fragment's piece is, in the closed vocabulary a data node composes its job from.</summary>
public enum FragmentUnitKind
{
    /// <summary>The bare inbound end of an edge: <c>input: arrow:-</c>.</summary>
    Inbound,

    /// <summary>A source brick, kept as its owner declared it.</summary>
    Source,

    /// <summary>A source brick that also sends its rows onto an edge: <c>output: arrow:-</c> added.</summary>
    SourceSend,

    /// <summary>A branch that only forwards another branch of the fragment onto an edge: <c>from</c> and <c>output: arrow:-</c>.</summary>
    Relay,

    /// <summary>A sink brick, fed from another branch of the fragment (<see cref="FragmentUnit.From"/>).</summary>
    Sink,
}

/// <summary>
/// One piece of a data node's fragment, named by its brick id: the node builds the job from what its own
/// owner declared, so the coordinator never sends the text of a read or a write. <see cref="Brick"/> is the
/// node's brick id, <see cref="From"/> the key of the piece a relay or a sink reads.
/// </summary>
public sealed record FragmentUnit(string Key, FragmentUnitKind Kind, string? Brick = null, string? From = null);

/// <summary>
/// A fragment as a host is asked to run it, or to say whether it would: the same record for <c>Launch</c>
/// and <c>Check</c>, so a check verifies what a launch will. A fragment for a data node is its
/// <see cref="Units"/>, in order, with the <see cref="Version"/> of the job they compose to
/// (<see cref="FragmentJob"/>): the host composes the job itself and refuses it when its bricks no longer
/// give that version. A runner takes <see cref="Yaml"/>, the job as written.
/// </summary>
public sealed record FragmentSpec(
    string Fragment, string Version, IReadOnlyList<FragmentEdge> Edges,
    IReadOnlyList<FragmentUnit>? Units = null, string? Yaml = null);

/// <summary>
/// What <c>Launch</c> asks: run <see cref="Spec"/> as part of the run <see cref="RunId"/>. <see cref="LaunchToken"/> is
/// minted by the coordinator for this launch alone and returned by the replica in its <c>Exited</c>: it tells the report of
/// this child from the report of an earlier one that held the same run id, fragment and replica. It is no secret.
/// </summary>
public sealed record LaunchRequest(string RunId, FragmentSpec Spec, string LaunchToken);

/// <summary>What <c>Launch</c> answers: the child is spawned and its edges can be wired, or why the host refused to run the fragment.</summary>
public sealed record LaunchResult(bool Started, string? Refusal = null);

/// <summary>What <c>Check</c> answers: whether the host would run the fragment, and why not.</summary>
public sealed record FragmentCheck(bool Ok, string? Reason = null);

/// <summary>
/// What <c>Exited</c> carries: how one run of a fragment ended, and the <see cref="LaunchToken"/> of the launch that
/// started the child, copied from its <see cref="LaunchRequest"/>.
/// </summary>
public sealed record FragmentExit(
    string RunId, string Fragment, int ExitCode, string Origin, string? FirstFault, IReadOnlyDictionary<string, long> RowCounts,
    string? LaunchToken = null);

/// <summary>
/// The two words <see cref="FragmentExit.Origin"/> carries, each by what it does to the run's verdict. The node writes
/// one of them, and the coordinator reads any word other than <see cref="Remote"/> as <see cref="Local"/>.
/// </summary>
public static class ExitOrigins
{
    /// <summary>The fragment's own fault, which ends the run at once.</summary>
    public const string Local = "Local";

    /// <summary>A consequence: the fragment's transfer failed or was torn down under it, and its peers owe a report within the grace.</summary>
    public const string Remote = "Remote";
}

/// <summary>
/// What <c>Exited</c> answers. <see cref="NotAdmitted"/>: the coordinator did not admit this host for that
/// fragment of that run under that launch token (or the run is over), and a report sent again would be answered the same.
/// <see cref="UnknownConnection"/>: the connection announced nothing the coordinator accepted, so the host
/// keeps its report and sends it once after its next announcement.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter<ExitedAck>))]
public enum ExitedAck { Accepted, NotAdmitted, UnknownConnection }

public sealed record NodeLogLine(string Node, string? Fragment, string Level, string Message);
