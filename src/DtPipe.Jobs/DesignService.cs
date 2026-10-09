using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Jobs;

/// <summary>
/// The kinds of card the designer draws. Each one is one branch of the job, except where
/// <see cref="DesignService.Compose"/> joins a chain of cards back into the branch they came from.
/// </summary>
public static class StepKinds
{
    public const string Source = "source";
    public const string Sink = "sink";
    public const string Transform = "transform";
    public const string Sql = "sql";
    public const string Merge = "merge";

    /// <summary>A branch no other kind describes, kept as its own YAML: imported, never lost.</summary>
    public const string Branch = "branch";
}

/// <summary>
/// One card. A source or a sink names a brick by <see cref="Brick"/> (<see cref="IBrickLookup"/>),
/// or carries its own endpoint as <see cref="Yaml"/>: <c>input</c> or <c>output</c>, the keys of
/// that side (<see cref="BranchSides"/>) and its provider-options blocks. <see cref="Transformers"/>
/// is the <c>transformers</c> list as JSON; <see cref="Yaml"/> is also the body of a
/// <see cref="StepKinds.Branch"/> step.
/// </summary>
public sealed record DesignStep(
    string Alias, string Kind, string? Brick = null, IReadOnlyList<string>? From = null, IReadOnlyList<string>? Ref = null,
    string? Query = null, JsonArray? Transformers = null, string? Yaml = null);

/// <summary>
/// The cards of a job and its header: the first comment line is the title, the next ones the
/// description, and <see cref="Directives"/> the header lines of the form <c>some-name: value</c>,
/// which belong to whoever reads them and are written back as they came.
/// </summary>
public sealed record DesignModel(
    string Title, string? Description, IReadOnlyList<string>? Directives, IReadOnlyList<DesignStep> Steps, JsonObject? Layout = null);

/// <summary>
/// What the designer shows for a step besides the step itself: its brick, its node, and the branch
/// of the composed job that runs it (<see cref="Branch"/>, its own alias unless a chain was joined).
/// </summary>
public sealed record StepInfo(
    string Alias, string? Node, string? BrickVersion, string? BrickTitle, IReadOnlyList<string> DependsOn, string? Branch = null);

public sealed record DesignResult(
    DesignModel Model, string Yaml, IReadOnlyList<StepInfo> Steps, IReadOnlyList<string> Errors, IReadOnlyList<string> Warnings)
{
    public bool Valid => Errors.Count == 0;
}

/// <summary>
/// Converts between the designer's cards and a dtpipe job, both ways, and is the only writer of the
/// job text, for <c>dtpipe studio</c> and the coordinator alike. A composed job is a plain dtpipe
/// job, readable and runnable as a whole on one machine: a source or sink brick is copied in as
/// <paramref name="bricks"/> declares it, and a branch <paramref name="endpoints"/> took apart into
/// its own source, transform and sink cards is joined back into one branch.
/// </summary>
public sealed partial class DesignService(IBrickLookup bricks, IEndpointRules? endpoints = null)
{
    /// <summary>
    /// A source or a sink inside a branch: a brick, or the branch's own endpoint (<see cref="Local"/>).
    /// <see cref="Keys"/> and <see cref="Components"/> are the branch keys and provider-options
    /// blocks it accounts for.
    /// </summary>
    private sealed record Endpoint(
        string Kind, DesignBrick? Brick, YamlMappingNode? Local, IReadOnlySet<string> Keys, IReadOnlySet<string> Components);

    public DesignResult Compose(DesignModel model)
    {
        var errors = new List<string>();
        var warnings = new List<string>();
        var steps = model.Steps;
        var aliases = new HashSet<string>(StringComparer.Ordinal);
        var infos = new List<StepInfo>();

        foreach (var step in steps)
        {
            if (!AliasPattern().IsMatch(step.Alias ?? ""))
                errors.Add($"'{step.Alias}' is not a usable alias: a letter first, then letters, digits, '_' or '-'.");
            else if (!aliases.Add(step.Alias!))
                errors.Add($"Two steps are named '{step.Alias}'.");
        }

        // A step whose alias is unusable or taken is reported above and composes into nothing.
        var usable = new HashSet<string>(aliases, StringComparer.Ordinal);
        var branches = new List<(string Alias, YamlMappingNode Branch)>();
        var joinable = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var step in steps)
        {
            if (!usable.Remove(step.Alias ?? "")) continue;
            var from = step.From ?? [];
            var refs = step.Ref ?? [];
            DesignBrick? brick = null;
            YamlMappingNode? branch = null;
            switch (step.Kind)
            {
                case StepKinds.Source or StepKinds.Sink when step.Brick is null:
                    branch = LocalEndpoint(step, from, refs, errors, warnings);
                    if (branch is not null) joinable[step.Alias!] = step.Kind;
                    break;

                case StepKinds.Source:
                case StepKinds.Sink:
                    brick = bricks.Find(step.Brick!);
                    if (brick is null)
                    {
                        errors.Add($"'{step.Alias}': no brick '{step.Brick}' is on offer.");
                        break;
                    }
                    if (brick.Kind != step.Kind)
                    {
                        errors.Add($"'{step.Alias}': {brick.Key} is a {brick.Kind}, not a {step.Kind}.");
                        break;
                    }
                    if (step.Kind == StepKinds.Source)
                    {
                        if (from.Count > 0 || refs.Count > 0) warnings.Add($"'{step.Alias}' is a source: its inputs are ignored.");
                        branch = bricks.BranchOf(brick);
                    }
                    else
                    {
                        if (from.Count != 1) errors.Add($"'{step.Alias}' writes {brick.Key}: connect exactly one step to it.");
                        branch = new YamlMappingNode();
                        if (from.Count > 0) branch.Add("from", string.Join(',', from));
                        foreach (var (key, value) in bricks.BranchOf(brick).Children) branch.Add(key, value);
                    }
                    if (!brick.Online) warnings.Add($"'{step.Alias}': {brick.Node} is offline.");
                    break;

                case StepKinds.Transform:
                    if (from.Count != 1) errors.Add($"'{step.Alias}' transforms one input: connect exactly one step to it.");
                    if (step.Transformers is not { Count: > 0 }) warnings.Add($"'{step.Alias}' has no transformer yet: it passes its rows through.");
                    branch = new YamlMappingNode();
                    if (from.Count > 0) branch.Add("from", string.Join(',', from));
                    if (step.Transformers is { Count: > 0 }) branch.Add("transformers", JobYaml.FromJson(step.Transformers));
                    joinable[step.Alias!] = StepKinds.Transform;
                    break;

                case StepKinds.Sql:
                    if (from.Count != 1) errors.Add($"'{step.Alias}': connect exactly one main input (from); join the others as ref.");
                    if (string.IsNullOrWhiteSpace(step.Query)) errors.Add($"'{step.Alias}' has no query.");
                    branch = new YamlMappingNode();
                    if (from.Count > 0) branch.Add("from", string.Join(',', from));
                    if (refs.Count > 0) branch.Add("ref", new YamlSequenceNode(refs.Select(r => new YamlScalarNode(r))));
                    branch.Add("provider-options", new YamlMappingNode { { "sql", new YamlMappingNode { { "query", JobYaml.Text(step.Query?.Trim() ?? "") } } } });
                    break;

                case StepKinds.Merge:
                    if (from.Count < 2) errors.Add($"'{step.Alias}' merges several inputs: connect at least two steps to it.");
                    branch = new YamlMappingNode();
                    if (from.Count > 0) branch.Add("from", string.Join(',', from));
                    branch.Add("provider-options", new YamlMappingNode { { "merge", new YamlMappingNode() } });
                    break;

                case StepKinds.Branch:
                    branch = Body(step, errors);
                    break;

                default:
                    errors.Add($"'{step.Alias}': unknown step kind '{step.Kind}'.");
                    break;
            }

            if (branch is null) continue;
            branches.Add((step.Alias!, branch));
            var dependsOn = JobYaml.Aliases(branch, "from").Concat(JobYaml.Aliases(branch, "ref")).ToList();
            infos.Add(new StepInfo(step.Alias!, brick?.Node, brick?.Version, brick?.Title, dependsOn));
        }

        // Wiring: every input names a step, no cycle, and every step's rows end somewhere.
        var known = branches.Select(b => b.Alias).ToHashSet(StringComparer.Ordinal);
        var consumed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var info in infos)
        {
            foreach (var dep in info.DependsOn)
            {
                if (!known.Contains(dep)) errors.Add($"'{info.Alias}' reads '{dep}', which no step defines.");
                consumed.Add(dep);
            }
        }
        foreach (var (alias, branch) in branches)
        {
            if (!consumed.Contains(alias) && !JobYaml.Has(branch, "output"))
                errors.Add($"'{alias}' leads nowhere: connect it to a sink, or remove it.");
        }
        var branchOf = infos.ToDictionary(i => i.Alias, i => i.Alias, StringComparer.Ordinal);
        var ordered = Join(TopologicalOrder(branches, infos, errors), joinable, branchOf);
        infos = infos.Select(i => i with { Branch = branchOf[i.Alias] }).ToList();

        var root = new YamlMappingNode();
        foreach (var (alias, branch) in ordered) root.Add(alias, branch);
        var yaml = Header(model) + (ordered.Count > 0 ? JobYaml.Serialize(root) : "");
        return new DesignResult(model, yaml, infos, errors.Distinct().ToList(), warnings);
    }

    /// <summary>
    /// A job back into cards. A branch that is a brick becomes that brick; a branch that reads, writes
    /// or does both around its own work is taken apart into one card each, the last one before the
    /// sink keeping the alias its readers name, when each part is a brick or an endpoint
    /// <paramref name="endpoints"/> can place. Anything else stays one <see cref="StepKinds.Branch"/>
    /// card, with its YAML as written.
    /// </summary>
    public DesignResult Decompose(string yaml, JsonObject? layout = null)
    {
        var (title, description, directives) = ReadHeader(yaml);
        YamlMappingNode job;
        try { job = JobYaml.Parse(yaml); }
        catch (Exception ex)
        {
            return new DesignResult(new DesignModel(title, description, directives, []), yaml, [], [$"The job is not readable: {ex.Message}"], []);
        }

        var taken = JobYaml.Branches(job).Select(b => b.Alias).ToHashSet(StringComparer.Ordinal);
        string Fresh(string alias, string suffix)
        {
            var candidate = $"{alias}_{suffix}";
            for (var i = 2; !taken.Add(candidate); i++) candidate = $"{alias}_{suffix}{i}";
            return candidate;
        }

        var steps = new List<DesignStep>();
        foreach (var (alias, branch) in JobYaml.Branches(job))
            steps.AddRange(Split(alias, branch, Fresh));

        var composed = Compose(new DesignModel(title, description, directives, steps, layout));
        return composed;
    }

    private IEnumerable<DesignStep> Split(string alias, YamlMappingNode branch, Func<string, string, string> fresh)
    {
        var whole = bricks.Match(branch);
        if (whole is not null)
        {
            yield return whole.Kind == StepKinds.Source
                ? new DesignStep(alias, StepKinds.Source, whole.Key)
                : new DesignStep(alias, StepKinds.Sink, whole.Key, JobYaml.Aliases(branch, "from"));
            yield break;
        }

        var keys = branch.Children.Keys.OfType<YamlScalarNode>().Select(k => k.Value!).ToHashSet(StringComparer.Ordinal);
        var options = JobYaml.Mapping(branch, "provider-options");
        var components = options?.Children.Keys.OfType<YamlScalarNode>().Select(k => k.Value!).ToHashSet(StringComparer.Ordinal) ?? [];
        var owners = endpoints is not null && (keys.Contains("input") || keys.Contains("output"))
            ? endpoints.Blocks(JobYaml.Scalar(branch, "input"), JobYaml.Scalar(branch, "output"), components.ToList())
            : null;
        var source = keys.Contains("input") ? FindEndpoint(branch, StepKinds.Source, owners) : null;
        var sink = keys.Contains("output") ? FindEndpoint(branch, StepKinds.Sink, owners) : null;
        var middle = keys.Contains("input") && source is null || keys.Contains("output") && sink is null
            ? NoCard(alias)
            : Middle(alias, branch, keys, components, source, sink);

        // A branch no card describes, or one whose only content is a plain tee, stays as written. A
        // branch that is nothing but one endpoint of its own is that endpoint's card.
        var alone = source is null != sink is null && (source ?? sink)!.Local is not null;
        if (middle is { Kind: "" } || middle is null && (source is null || sink is null) && !alone)
        {
            yield return RawStep(alias, branch);
            yield break;
        }

        // Readers of the branch get its rows after its own work: the alias stays with the last
        // card before the sink.
        var carriers = new List<DesignStep>();
        if (source is not null) carriers.Add(Card(source, middle is null ? alias : fresh(alias, "source"), null));
        if (middle is not null) carriers.Add(source is null ? middle : middle with { From = [carriers[0].Alias] });
        foreach (var card in carriers) yield return card;
        if (sink is not null)
            yield return carriers.Count == 0
                ? Card(sink, alias, JobYaml.Aliases(branch, "from"))
                : Card(sink, fresh(alias, "sink"), [carriers[^1].Alias]);
    }

    /// <summary>
    /// The brick a branch starts or ends with, or else the branch's own endpoint on that side when
    /// <paramref name="owners"/> places every provider-options block it needs.
    /// </summary>
    private Endpoint? FindEndpoint(YamlMappingNode branch, string kind, EndpointBlocks? owners)
    {
        var endpointKey = kind == StepKinds.Source ? "input" : "output";
        if (bricks.Contained(branch, kind) is { } brick)
            return new Endpoint(kind, brick, null, new HashSet<string>([endpointKey], StringComparer.Ordinal), ComponentsOf(brick).ToHashSet(StringComparer.Ordinal));
        if (owners is null) return null;

        var sideKeys = kind == StepKinds.Source ? BranchSides.Read : BranchSides.Write;
        var owned = kind == StepKinds.Source ? owners.Reader : owners.Writer;
        var local = new YamlMappingNode();
        var keys = new HashSet<string>(StringComparer.Ordinal) { endpointKey };
        var components = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (key, value) in branch.Children)
        {
            var name = ((YamlScalarNode)key).Value!;
            if (name == endpointKey || sideKeys.Contains(name))
            {
                local.Add(name, value);
                keys.Add(name);
            }
            else if (name == "provider-options" && value is YamlMappingNode blocks)
            {
                var mine = blocks.Children.Where(kv => owned.Contains(((YamlScalarNode)kv.Key).Value!)).ToList();
                if (mine.Count == 0) continue;
                local.Add("provider-options", new YamlMappingNode(mine));
                components.UnionWith(mine.Select(kv => ((YamlScalarNode)kv.Key).Value!));
            }
        }
        return new Endpoint(kind, null, JobYaml.CloneBranch(local), keys, components);
    }

    private static DesignStep Card(Endpoint endpoint, string alias, IReadOnlyList<string>? from) =>
        endpoint.Brick is not null
            ? new DesignStep(alias, endpoint.Kind, endpoint.Brick.Key, from)
            : new DesignStep(alias, endpoint.Kind, From: from, Yaml: BodyText(endpoint.Local!));

    private static DesignStep NoCard(string alias) => new(alias, "");

    /// <summary>
    /// The card for what a branch does between its endpoints, or null when it does nothing there. A
    /// step whose <see cref="DesignStep.Kind"/> is empty means "no card fits": the branch stays raw.
    /// </summary>
    private static DesignStep? Middle(
        string alias, YamlMappingNode branch, HashSet<string> keys, HashSet<string> components, Endpoint? source, Endpoint? sink)
    {
        var rest = new HashSet<string>(components, StringComparer.Ordinal);
        if (source is not null) rest.ExceptWith(source.Components);
        if (sink is not null) rest.ExceptWith(sink.Components);
        var middleKeys = keys.Except(["provider-options"]).ToHashSet(StringComparer.Ordinal);
        if (source is not null) middleKeys.ExceptWith(source.Keys);
        if (sink is not null) middleKeys.ExceptWith(sink.Keys);
        var from = JobYaml.Aliases(branch, "from");

        if (source is not null && (middleKeys.Contains("from") || middleKeys.Contains("ref"))) return NoCard(alias);
        if (rest.Count == 0 && middleKeys.IsSubsetOf(["from", "transformers"]))
        {
            if (!middleKeys.Contains("transformers"))
                return source is not null && sink is not null || (source ?? sink)?.Local is not null ? null : NoCard(alias);
            return new DesignStep(alias, StepKinds.Transform, From: from, Transformers: JobYaml.ToJson(branch["transformers"]) as JsonArray);
        }
        if (source is null && rest.SetEquals(["sql"]) && middleKeys.Contains("from") && middleKeys.IsSubsetOf(["from", "ref"])
            && JobYaml.Mapping(JobYaml.Mapping(branch, "provider-options")!, "sql") is { } sql
            && sql.Children.Count == 1 && JobYaml.Scalar(sql, "query") is { } query)
            return new DesignStep(alias, StepKinds.Sql, From: from, Ref: JobYaml.Aliases(branch, "ref"), Query: query);
        if (source is null && rest.SetEquals(["merge"]) && middleKeys.SetEquals(["from"]))
            return new DesignStep(alias, StepKinds.Merge, From: from);
        return NoCard(alias);
    }

    private IEnumerable<string> ComponentsOf(DesignBrick brick) =>
        JobYaml.Mapping(bricks.BranchOf(brick), "provider-options")?.Children.Keys.OfType<YamlScalarNode>().Select(k => k.Value!) ?? [];

    /// <summary>
    /// A source or sink card that carries its own endpoint: a source's YAML is its whole branch, a
    /// sink's gets its one input as <c>from</c>.
    /// </summary>
    private static YamlMappingNode? LocalEndpoint(
        DesignStep step, IReadOnlyList<string> from, IReadOnlyList<string> refs, List<string> errors, List<string> warnings)
    {
        if (string.IsNullOrWhiteSpace(step.Yaml))
        {
            errors.Add($"'{step.Alias}' is a {step.Kind} with neither a brick nor an endpoint of its own.");
            return null;
        }
        var local = Body(step, errors);
        if (local is null) return null;

        var (endpointKey, other) = step.Kind == StepKinds.Source ? ("input", "output") : ("output", "input");
        if (!JobYaml.Has(local, endpointKey)) errors.Add($"'{step.Alias}' is a {step.Kind}: its YAML names no {endpointKey}.");
        var stray = new[] { "from", "ref", "transformers", other }.Where(k => JobYaml.Has(local, k)).ToList();
        if (stray.Count > 0) errors.Add($"'{step.Alias}' is a {step.Kind}: {string.Join(", ", stray)} belong to other cards.");

        if (step.Kind == StepKinds.Source)
        {
            if (from.Count > 0 || refs.Count > 0) warnings.Add($"'{step.Alias}' is a source: its inputs are ignored.");
            return local;
        }
        if (from.Count != 1) errors.Add($"'{step.Alias}' writes {JobYaml.Scalar(local, "output")}: connect exactly one step to it.");
        var branch = new YamlMappingNode();
        if (from.Count > 0) branch.Add("from", string.Join(',', from));
        foreach (var (key, value) in local.Children) branch.Add(key, value);
        return branch;
    }

    /// <summary>
    /// Joins each chain the cards drew from one branch back into that branch: a source of its own
    /// into the transform or sink that alone reads it, then a transform into the sink of its own that
    /// alone reads it. The joined branch keeps the alias of the transform, or the source's when there
    /// is none, as <see cref="Decompose"/> gave it. A brick never joins: its branch is what its node
    /// runs.
    /// </summary>
    private static List<(string Alias, YamlMappingNode Branch)> Join(
        List<(string Alias, YamlMappingNode Branch)> ordered, Dictionary<string, string> joinable, Dictionary<string, string> branchOf)
    {
        var list = ordered.ToList();
        var kinds = new Dictionary<string, string>(joinable, StringComparer.Ordinal);

        bool JoinPass(string upstreamKind)
        {
            for (var i = 0; i < list.Count; i++)
            {
                var (up, upBranch) = list[i];
                if (kinds.GetValueOrDefault(up) != upstreamKind || JobYaml.Has(upBranch, "output")) continue;

                var readers = list.Where(b => JobYaml.Aliases(b.Branch, "from").Concat(JobYaml.Aliases(b.Branch, "ref")).Contains(up)).ToList();
                if (readers.Count != 1) continue;
                var (down, downBranch) = readers[0];
                var downKind = kinds.GetValueOrDefault(down);
                var accepts = downKind == StepKinds.Sink || upstreamKind == StepKinds.Source && downKind == StepKinds.Transform;
                if (!accepts || JobYaml.Has(downBranch, "input") || JobYaml.Has(downBranch, "ref")
                    || !JobYaml.Aliases(downBranch, "from").SequenceEqual([up])) continue;
                if (downKind == StepKinds.Sink && list.Any(b => JobYaml.Aliases(b.Branch, "from").Concat(JobYaml.Aliases(b.Branch, "ref")).Contains(down))) continue;

                var joined = JoinBranches(upBranch, downBranch);
                if (joined is null) continue;
                var alias = downKind == StepKinds.Transform ? down : up;
                var j = list.FindIndex(b => b.Alias == down);
                list[i] = (alias, joined);
                list.RemoveAt(j);
                foreach (var card in branchOf.Where(kv => kv.Value == up || kv.Value == down).Select(kv => kv.Key).ToList())
                    branchOf[card] = alias;
                kinds.Remove(up);
                kinds.Remove(down);
                kinds[alias] = downKind == StepKinds.Transform ? StepKinds.Transform : "";
                return true;
            }
            return false;
        }

        while (JoinPass(StepKinds.Source)) { }
        while (JoinPass(StepKinds.Transform)) { }
        return list;
    }

    /// <summary>
    /// The upstream branch's keys, then the downstream's without its <c>from</c>, their
    /// provider-options blocks in one mapping. Null when a key or a block appears on both sides.
    /// </summary>
    private static YamlMappingNode? JoinBranches(YamlMappingNode up, YamlMappingNode down)
    {
        var upOptions = JobYaml.Mapping(up, "provider-options");
        var downOptions = JobYaml.Mapping(down, "provider-options");
        var joined = new YamlMappingNode();
        foreach (var (key, value) in up.Children)
        {
            if (key is YamlScalarNode { Value: "provider-options" } && downOptions is not null)
            {
                if (upOptions!.Children.Keys.Any(downOptions.Children.ContainsKey)) return null;
                joined.Add(key, new YamlMappingNode(upOptions.Children.Concat(downOptions.Children)));
            }
            else joined.Add(key, value);
        }
        foreach (var (key, value) in down.Children)
        {
            if (key is YamlScalarNode { Value: "from" }) continue;
            if (key is YamlScalarNode { Value: "provider-options" } && upOptions is not null) continue;
            if (joined.Children.ContainsKey(key)) return null;
            joined.Add(key, value);
        }
        return JobYaml.CloneBranch(joined);
    }

    private static YamlMappingNode? Body(DesignStep step, List<string> errors)
    {
        try
        {
            var parsed = JobYaml.Parse($"b:\n{Indent(step.Yaml ?? "")}");
            return (YamlMappingNode)parsed["b"];
        }
        catch (Exception ex)
        {
            errors.Add($"'{step.Alias}': its YAML is not a branch ({ex.Message}).");
            return null;
        }
    }

    private static string BodyText(YamlMappingNode branch)
    {
        var text = JobYaml.Serialize(new YamlMappingNode { { "b", JobYaml.CloneBranch(branch) } });
        return string.Join('\n', text.Split('\n').Skip(1).Select(l => l.StartsWith("  ") ? l[2..] : l)).TrimEnd() + "\n";
    }

    private static DesignStep RawStep(string alias, YamlMappingNode branch) =>
        new(alias, StepKinds.Branch, From: JobYaml.Aliases(branch, "from"), Ref: JobYaml.Aliases(branch, "ref"), Yaml: BodyText(branch));

    private static List<(string Alias, YamlMappingNode Branch)> TopologicalOrder(
        List<(string Alias, YamlMappingNode Branch)> branches, List<StepInfo> infos, List<string> errors)
    {
        var deps = infos.ToDictionary(i => i.Alias, i => i.DependsOn, StringComparer.Ordinal);
        var byAlias = branches.ToDictionary(b => b.Alias, b => b.Branch, StringComparer.Ordinal);
        var ordered = new List<(string, YamlMappingNode)>();
        var state = new Dictionary<string, int>(StringComparer.Ordinal);
        void Visit(string alias)
        {
            if (state.TryGetValue(alias, out var s))
            {
                if (s == 1) errors.Add($"'{alias}' reads its own output through a cycle.");
                return;
            }
            state[alias] = 1;
            foreach (var dep in deps.GetValueOrDefault(alias) ?? []) if (byAlias.ContainsKey(dep)) Visit(dep);
            state[alias] = 2;
            ordered.Add((alias, byAlias[alias]));
        }
        foreach (var (alias, _) in branches) Visit(alias);
        return ordered;
    }

    private static string Header(DesignModel model)
    {
        var text = new StringBuilder();
        text.Append("# ").Append(OneLine(model.Title) is { Length: > 0 } t ? t : "Untitled pipeline").Append('\n');
        foreach (var line in Wrap(OneLine(model.Description ?? ""), 96)) text.Append("# ").Append(line).Append('\n');
        foreach (var directive in model.Directives ?? [])
            if (OneLine(directive) is { Length: > 0 } line) text.Append("# ").Append(line).Append('\n');
        return text.ToString();
    }

    /// <summary>
    /// The header of a job: its leading comment lines. A line of the form <c>some-name: value</c> is
    /// a directive, kept whole; the first other line is the title, the rest the description.
    /// </summary>
    public static (string Title, string? Description, IReadOnlyList<string> Directives) ReadHeader(string yaml)
    {
        var header = yaml.Split('\n').TakeWhile(l => l.StartsWith('#')).Select(l => l.TrimStart('#').Trim()).ToList();
        var directives = header.Where(l => DirectivePattern().IsMatch(l)).ToList();
        var prose = header.Where(l => !DirectivePattern().IsMatch(l)).ToList();
        return (prose.FirstOrDefault() ?? "Untitled pipeline", prose.Count > 1 ? string.Join(' ', prose.Skip(1)) : null, directives);
    }

    private static string OneLine(string text) => string.Join(' ', text.Split(['\n', '\r'], StringSplitOptions.RemoveEmptyEntries)).Trim();

    private static IEnumerable<string> Wrap(string text, int width)
    {
        var line = new StringBuilder();
        foreach (var word in text.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            if (line.Length > 0 && line.Length + word.Length + 1 > width) { yield return line.ToString(); line.Clear(); }
            if (line.Length > 0) line.Append(' ');
            line.Append(word);
        }
        if (line.Length > 0) yield return line.ToString();
    }

    private static string Indent(string text) =>
        string.Join('\n', text.Replace("\r\n", "\n").Split('\n').Select(l => "  " + l));

    [GeneratedRegex("^[A-Za-z][A-Za-z0-9_-]*$")]
    private static partial Regex AliasPattern();

    [GeneratedRegex("^[a-z][a-z0-9]*(-[a-z0-9]+)+:")]
    private static partial Regex DirectivePattern();
}
