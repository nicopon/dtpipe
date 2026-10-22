using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using DtPipe.Jobs;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Contracts;

/// <summary>A brick reduced to what matching needs: its kind and the canonical text of its branch.</summary>
public sealed record BrickForm(BrickKind Kind, string Canonical);

/// <summary>
/// What a branch has to be for a data node to run it. The coordinator uses the same reading to refuse a branch that is no
/// brick and the node host uses it to refuse anything else, so the designer can
/// never call something a brick that the node then declines.
/// <para>
/// Two layers. <b>Structure</b>, always: a data node only reads and writes, so a branch carries no
/// transformer, no processor and reads at most one other branch; everything between belongs on a
/// runner. <b>Bricks</b>, unless the host registered a fragment gate that waives it: the branch is also one of the node's own
/// bricks as its owner declared them, or a bare arrow edge.
/// </para>
/// </summary>
public static class BrickRules
{
    public const string ArrowEndpoint = "arrow:-";

    /// <summary>
    /// provider-options keys that name a processor rather than a reader or writer: the <c>stream-processor</c> entries of
    /// <c>dtpipe providers</c>, which <c>BrickRulesProcessorsTests</c> compares with this list.
    /// </summary>
    public static readonly IReadOnlyList<string> Processors = ["sql", "merge"];

    private const string RunnerHint = "a data node only reads and writes, and everything between belongs on a runner";

    // What a branch that only reads or writes may carry: where it reads and writes, and the engine
    // settings that bound how many rows or bytes move (dtpipe split writes them on every branch). A key
    // that computes (transformers, ref) or touches the node's disk (checkpoint, state, a log or metrics path)
    // is not on the list, so a key dtpipe adds later is refused until someone decides. The one exception
    // is a sink's cursor column (CursorKey): the file that keeps it is the node's own, never the brick's.
    private static readonly HashSet<string> IoKeys = new(
        ["input", "output", "from", "provider-options", "batch-size", "max-batch-bytes", "limit", "sampling-rate", "sampling-seed"],
        StringComparer.Ordinal);
    private static readonly YamlScalarNode FromKey = new("from");

    /// <summary>The column a sink brick tracks the highest value of; the node adds the file that keeps it.</summary>
    public const string CursorKey = "cursor";
    private static readonly YamlScalarNode OutputKey = new("output");

    /// <summary>The form a brick has as its node declares it (its branch as JSON text).</summary>
    public static BrickForm Declared(BrickKind kind, string branchJson) =>
        Declared(kind, (YamlMappingNode)JobYaml.FromJson(JsonNode.Parse(branchJson)));

    public static BrickForm Declared(BrickKind kind, YamlMappingNode branch) => new(kind, Canonical(branch));

    /// <summary>
    /// The version of a brick declared as <paramref name="branchJson"/>: the start of the SHA-256 of its canonical text, taken once
    /// by the node that owns the text. Variables stay as the <c>${{Name}}</c> tokens the text holds, so no value enters the hash.
    /// A secret written in the text itself would, and the version would let anyone who sees it test guesses offline: a brick keeps
    /// its secrets in <c>${{Name}}</c> or <c>keyring://</c> references.
    /// </summary>
    public static string VersionOf(BrickKind kind, string branchJson) =>
        ContentHash.Of(Declared(kind, branchJson).Canonical, 12);

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
        return new BrickForm(isSink ? BrickKind.Sink : BrickKind.Source, Canonical(new YamlMappingNode(kept)));
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

        if (keys.Contains(CursorKey) && keys.Contains("input"))
            return $"'{alias}' reads a source and sets a cursor: a cursor is the column a sink tracks, and a source is filtered by ${{{{cursor://|default}}}}.";
        if (CursorTokenRefusal(alias, branch) is { } token) return token;

        var extra = keys.Where(k => !IoKeys.Contains(k) && k != CursorKey).Order(StringComparer.Ordinal).ToList();
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
    /// Why a branch's <c>${{cursor://…}}</c> is refused, or <c>null</c>: the value of a cursor reaches the fragment from the coordinator, so
    /// the token names no file (<c>${{cursor://|default}}</c>) and the child reads none.
    /// </summary>
    public static string? CursorTokenRefusal(string alias, YamlMappingNode branch)
    {
        const string scheme = "cursor://";
        foreach (var token in VariableTokens.Of(JobYaml.Serialize(new YamlMappingNode { { alias, branch } })))
        {
            if (!token.Expression.StartsWith(scheme, StringComparison.OrdinalIgnoreCase)) continue;
            var body = token.Expression[scheme.Length..];
            var path = body.Contains('|') ? body[..body.IndexOf('|')] : body;
            if (path.Trim().Length > 0)
                return $"'{alias}' reads the cursor file '{path.Trim()}': the coordinator gives a fragment its cursor, so write ${{{{cursor://|default}}}}.";
        }
        return null;
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

    /// <summary>
    /// A form that ignores key order and scalar style: two branches that dtpipe reads the same way
    /// have the same canonical text. What a brick is matched on, and hashed into its version.
    /// </summary>
    public static string Canonical(YamlNode node)
    {
        var text = new StringBuilder();
        void Write(YamlNode n)
        {
            switch (n)
            {
                case YamlMappingNode m:
                    text.Append('{');
                    var first = true;
                    foreach (var (key, value) in m.Children.OrderBy(kv => ((YamlScalarNode)kv.Key).Value, StringComparer.Ordinal))
                    {
                        if (!first) text.Append(',');
                        first = false;
                        text.Append(JsonSerializer.Serialize(((YamlScalarNode)key).Value)).Append(':');
                        Write(value);
                    }
                    text.Append('}');
                    break;
                case YamlSequenceNode s:
                    text.Append('[');
                    for (var i = 0; i < s.Children.Count; i++)
                    {
                        if (i > 0) text.Append(',');
                        Write(s.Children[i]);
                    }
                    text.Append(']');
                    break;
                case YamlScalarNode s:
                    text.Append(JsonSerializer.Serialize(s.Value));
                    break;
            }
        }
        Write(node);
        return text.ToString();
    }
}
