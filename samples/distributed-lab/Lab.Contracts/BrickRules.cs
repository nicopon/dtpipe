using System.Text.Json.Nodes;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Lab.Contracts;

/// <summary>A brick reduced to what matching needs: its kind and the canonical text of its branch.</summary>
public sealed record BrickForm(BrickKind Kind, string Canonical);

/// <summary>
/// What a branch has to be for a data node to run it. The coordinator uses the same reading to
/// recognise a brick in a job and the node host uses it to refuse anything else, so the designer can
/// never call something a brick that the node then declines.
/// <para>
/// Two layers. <b>Structure</b>, always: a data node only reads and writes, so a branch carries no
/// transformer, no processor and reads at most one other branch; everything between belongs on a
/// runner. <b>Bricks</b>, unless the host runs in sandbox mode: the branch is also one of the node's own
/// bricks as its owner declared them, or a bare arrow edge.
/// </para>
/// </summary>
public static class BrickRules
{
    public const string ArrowEndpoint = "arrow:-";

    /// <summary>provider-options keys that name a processor rather than a reader or writer.</summary>
    public static readonly IReadOnlyList<string> Processors = ["sql", "merge"];

    private const string RunnerHint = "a data node only reads and writes, and everything between belongs on a runner";

    // What a branch that only reads or writes may carry: where it reads and writes, and the engine
    // settings that bound how many rows or bytes move (dtpipe split writes them on every branch). A key
    // that computes (transformers, ref) or touches the node's disk (checkpoint, cursor, state, a log or
    // metrics path) is not on the list, so a key dtpipe adds later is refused until someone decides.
    private static readonly HashSet<string> IoKeys = new(
        ["input", "output", "from", "provider-options", "batch-size", "max-batch-bytes", "limit", "sampling-rate", "sampling-seed"],
        StringComparer.Ordinal);
    private static readonly YamlScalarNode FromKey = new("from");
    private static readonly YamlScalarNode OutputKey = new("output");

    /// <summary>The form a brick has as its node declares it (its branch as JSON text).</summary>
    public static BrickForm Declared(BrickKind kind, string branchJson) =>
        Declared(kind, (YamlMappingNode)JobYaml.FromJson(JsonNode.Parse(branchJson)));

    public static BrickForm Declared(BrickKind kind, YamlMappingNode branch) => new(kind, JobYaml.Canonical(branch));

    /// <summary>
    /// The form of a whole branch, with the wiring the planner adds around a brick set aside: a sink is
    /// fed through <c>from</c>, and a source sent straight to another node gains <c>output: arrow:-</c>.
    /// </summary>
    public static BrickForm FormOf(YamlMappingNode branch)
    {
        var isSink = JobYaml.Has(branch, "from");
        var kept = branch.Children.Where(kv =>
            !kv.Key.Equals(FromKey)
            && !(!isSink && kv.Key.Equals(OutputKey) && kv.Value is YamlScalarNode { Value: ArrowEndpoint }));
        return new BrickForm(isSink ? BrickKind.Sink : BrickKind.Source, JobYaml.Canonical(new YamlMappingNode(kept)));
    }

    /// <summary>
    /// Why a data node offering <paramref name="bricks"/> must not run this branch, or <c>null</c> when it
    /// may. With <paramref name="requireBricks"/> the branch must also be one of those bricks, the bare
    /// inbound end of an edge (<c>input: arrow:-</c>), or a relay of another branch of the fragment onto
    /// an edge; without it, any reader or writer passes. Anything else can reach the node's own data or
    /// environment in a way its owner never declared.
    /// </summary>
    public static string? Refusal(
        string alias, YamlMappingNode branch, IReadOnlySet<string> fragmentAliases, IReadOnlyList<BrickForm> bricks,
        bool requireBricks)
    {
        var keys = branch.Children.Keys.OfType<YamlScalarNode>().Select(k => k.Value ?? "").ToHashSet(StringComparer.Ordinal);

        var extra = keys.Where(k => !IoKeys.Contains(k)).Order(StringComparer.Ordinal).ToList();
        if (extra.Count > 0) return $"'{alias}' does more than read or write ({string.Join(", ", extra)}): {RunnerHint}.";

        var processor = JobYaml.Mapping(branch, "provider-options")?.Children.Keys.OfType<YamlScalarNode>()
            .Select(k => k.Value ?? "").FirstOrDefault(Processors.Contains);
        if (processor is not null) return $"'{alias}' runs the {processor} processor: {RunnerHint}.";

        if (JobYaml.Aliases(branch, "from").Count > 1) return $"'{alias}' reads several branches at once: {RunnerHint}.";

        if (!requireBricks) return null;

        if (keys.SetEquals(["input"]) && JobYaml.Scalar(branch, "input") == ArrowEndpoint) return null;

        if (keys.SetEquals(["from", "output"]) && JobYaml.Scalar(branch, "output") == ArrowEndpoint
            && JobYaml.Aliases(branch, "from") is [var source] && source != alias && fragmentAliases.Contains(source))
            return null;

        var form = FormOf(branch);
        if (bricks.Any(b => b.Kind == form.Kind && b.Canonical == form.Canonical)) return null;

        return $"'{alias}' is not one of this node's bricks as its owner declared them, nor a bare arrow edge.";
    }

    /// <summary>
    /// Why a node must not offer the brick <paramref name="id"/> declared as <paramref name="branchJson"/>, or
    /// <c>null</c> when it may: a brick is something the node itself would run, so it obeys the structure
    /// rule too, and is announced only if it does.
    /// </summary>
    public static string? DeclaredRefusal(string id, string branchJson)
    {
        try
        {
            if (JobYaml.FromJson(JsonNode.Parse(branchJson)) is not YamlMappingNode branch)
                return $"'{id}' is not an object.";
            return Refusal(id, branch, new HashSet<string>(StringComparer.Ordinal) { id }, [], requireBricks: false);
        }
        catch (Exception ex)
        {
            return $"'{id}' is not readable: {ex.Message}";
        }
    }

    /// <summary>
    /// The first branch of a fragment's job a data node may not run, described, or <c>null</c> when it may
    /// run all of it. A job it cannot read is refused too.
    /// </summary>
    public static string? FragmentRefusal(string yaml, IReadOnlyList<BrickForm> bricks, bool requireBricks)
    {
        try
        {
            var job = JobYaml.Parse(yaml);
            var aliases = JobYaml.Branches(job).Select(b => b.Alias).ToHashSet(StringComparer.Ordinal);
            foreach (var (alias, branch) in JobYaml.Branches(job))
            {
                if (Refusal(alias, branch, aliases, bricks, requireBricks) is { } reason) return reason;
            }
            return null;
        }
        catch (Exception ex)
        {
            return $"the fragment is not readable: {ex.Message}";
        }
    }
}
