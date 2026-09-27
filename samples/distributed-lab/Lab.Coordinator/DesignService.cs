using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using DtPipe.Lab.Contracts;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Lab.Coordinator;

/// <summary>The kinds of card the designer draws. Each one is exactly one branch of the job.</summary>
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
/// One card. <see cref="Brick"/> is a brick key (<c>node/id</c>) for a source or a sink;
/// <see cref="Transformers"/> is the <c>transformers</c> list as JSON; <see cref="Yaml"/> is the
/// body of a <see cref="StepKinds.Branch"/> step.
/// </summary>
public sealed record DesignStep(
    string Alias, string Kind, string? Brick = null, IReadOnlyList<string>? From = null, IReadOnlyList<string>? Ref = null,
    string? Query = null, JsonArray? Transformers = null, string? Yaml = null);

public sealed record DesignModel(
    string Title, string? Description, string? Check, IReadOnlyList<DesignStep> Steps, JsonObject? Layout = null);

/// <summary>What the designer shows for a step besides the step itself: its brick, and its node.</summary>
public sealed record StepInfo(string Alias, string? Node, string? BrickVersion, string? BrickTitle, IReadOnlyList<string> DependsOn);

public sealed record DesignResult(
    DesignModel Model, string Yaml, IReadOnlyList<StepInfo> Steps, IReadOnlyList<string> Errors, IReadOnlyList<string> Warnings)
{
    public bool Valid => Errors.Count == 0;
}

/// <summary>
/// Converts between the designer's cards and a dtpipe job, both ways, and is the only writer of the
/// job text. A composed job is a plain dtpipe job: one branch per card, a source or sink brick
/// copied in as its node declares it, readable and runnable as a whole on one machine.
/// </summary>
public sealed partial class DesignService(BrickCatalog bricks)
{
    private const string CheckDirective = "lab-check:";

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

        var branches = new List<(string Alias, YamlMappingNode Branch)>();
        foreach (var step in steps)
        {
            var from = step.From ?? [];
            var refs = step.Ref ?? [];
            BrickView? brick = null;
            YamlMappingNode? branch = null;
            switch (step.Kind)
            {
                case StepKinds.Source:
                case StepKinds.Sink:
                    brick = step.Brick is null ? null : bricks.Find(step.Brick);
                    if (brick is null)
                    {
                        errors.Add($"'{step.Alias}': no online node offers the brick '{step.Brick}'.");
                        break;
                    }
                    var expected = step.Kind == StepKinds.Source ? BrickKind.Source : BrickKind.Sink;
                    if (brick.Kind != expected)
                    {
                        errors.Add($"'{step.Alias}': {brick.Key} is a {brick.Kind.ToString().ToLowerInvariant()}, not a {step.Kind}.");
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
                    try
                    {
                        var parsed = JobYaml.Parse($"b:\n{Indent(step.Yaml ?? "")}");
                        branch = (YamlMappingNode)parsed["b"];
                    }
                    catch (Exception ex)
                    {
                        errors.Add($"'{step.Alias}': its YAML is not a branch ({ex.Message}).");
                    }
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
        var ordered = TopologicalOrder(branches, infos, errors);

        var root = new YamlMappingNode();
        foreach (var (alias, branch) in ordered) root.Add(alias, branch);
        var yaml = Header(model) + (ordered.Count > 0 ? JobYaml.Serialize(root) : "");
        return new DesignResult(model, yaml, infos, errors.Distinct().ToList(), warnings);
    }

    /// <summary>
    /// A job back into cards. A branch that is a brick becomes that brick; a branch that reads a
    /// source brick, writes a sink brick, or does both around its own work is taken apart into one
    /// card each, the last one keeping the alias its readers name. Anything else stays one
    /// <see cref="StepKinds.Branch"/> card, with its YAML as written.
    /// </summary>
    public DesignResult Decompose(string yaml, JsonObject? layout = null)
    {
        var (title, description, check) = ReadHeader(yaml);
        YamlMappingNode job;
        try { job = JobYaml.Parse(yaml); }
        catch (Exception ex)
        {
            return new DesignResult(new DesignModel(title, description, check, []), yaml, [], [$"The job is not readable: {ex.Message}"], []);
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

        var composed = Compose(new DesignModel(title, description, check, steps, layout));
        return composed;
    }

    private IEnumerable<DesignStep> Split(string alias, YamlMappingNode branch, Func<string, string, string> fresh)
    {
        var whole = bricks.Match(branch);
        if (whole is not null)
        {
            yield return whole.Kind == BrickKind.Source
                ? new DesignStep(alias, StepKinds.Source, whole.Key)
                : new DesignStep(alias, StepKinds.Sink, whole.Key, JobYaml.Aliases(branch, "from"));
            yield break;
        }

        var keys = branch.Children.Keys.OfType<YamlScalarNode>().Select(k => k.Value!).ToHashSet(StringComparer.Ordinal);
        var options = JobYaml.Mapping(branch, "provider-options");
        var components = options?.Children.Keys.OfType<YamlScalarNode>().Select(k => k.Value!).ToHashSet(StringComparer.Ordinal) ?? [];
        var source = keys.Contains("input") ? bricks.Contained(branch, BrickKind.Source) : null;
        var sink = keys.Contains("output") ? bricks.Contained(branch, BrickKind.Sink) : null;
        var middle = keys.Contains("input") && source is null || keys.Contains("output") && sink is null
            ? null
            : Middle(alias, branch, keys, components, source, sink);

        // A branch no card describes, or one whose only content is a plain tee, stays as written.
        if (middle is null && (source is null || sink is null) || middle is { Kind: "" })
        {
            yield return RawStep(alias, branch);
            yield break;
        }

        // Readers of the branch get its rows after its own work: the alias stays with the last
        // card before the sink.
        var carriers = new List<DesignStep>();
        if (source is not null) carriers.Add(new DesignStep(middle is null ? alias : fresh(alias, "source"), StepKinds.Source, source.Key));
        if (middle is not null) carriers.Add(source is null ? middle : middle with { From = [carriers[0].Alias] });
        foreach (var card in carriers) yield return card;
        if (sink is not null) yield return new DesignStep(fresh(alias, "sink"), StepKinds.Sink, sink.Key, [carriers[^1].Alias]);
    }

    /// <summary>
    /// The card for what a branch does between its bricks, or null when it does nothing there. A
    /// step whose <see cref="DesignStep.Kind"/> is empty means "no card fits": the branch stays raw.
    /// </summary>
    private DesignStep? Middle(
        string alias, YamlMappingNode branch, HashSet<string> keys, HashSet<string> components, BrickView? source, BrickView? sink)
    {
        var rest = new HashSet<string>(components, StringComparer.Ordinal);
        if (source is not null) rest.ExceptWith(ComponentsOf(source));
        if (sink is not null) rest.ExceptWith(ComponentsOf(sink));
        var middleKeys = keys.Except(["input", "output", "provider-options"]).ToHashSet(StringComparer.Ordinal);
        var from = JobYaml.Aliases(branch, "from");
        var noCard = new DesignStep(alias, "");

        if (source is not null && (middleKeys.Contains("from") || middleKeys.Contains("ref"))) return noCard;
        if (rest.Count == 0 && middleKeys.IsSubsetOf(["from", "transformers"]))
        {
            if (!middleKeys.Contains("transformers")) return source is not null && sink is not null ? null : noCard;
            return new DesignStep(alias, StepKinds.Transform, From: from, Transformers: JobYaml.ToJson(branch["transformers"]) as JsonArray);
        }
        if (source is null && rest.SetEquals(["sql"]) && middleKeys.Contains("from") && middleKeys.IsSubsetOf(["from", "ref"])
            && JobYaml.Mapping(JobYaml.Mapping(branch, "provider-options")!, "sql") is { } sql
            && sql.Children.Count == 1 && JobYaml.Scalar(sql, "query") is { } query)
            return new DesignStep(alias, StepKinds.Sql, From: from, Ref: JobYaml.Aliases(branch, "ref"), Query: query);
        if (source is null && rest.SetEquals(["merge"]) && middleKeys.SetEquals(["from"]))
            return new DesignStep(alias, StepKinds.Merge, From: from);
        return noCard;
    }

    private IEnumerable<string> ComponentsOf(BrickView brick) =>
        JobYaml.Mapping(bricks.BranchOf(brick), "provider-options")?.Children.Keys.OfType<YamlScalarNode>().Select(k => k.Value!) ?? [];

    private static DesignStep RawStep(string alias, YamlMappingNode branch)
    {
        var text = JobYaml.Serialize(new YamlMappingNode { { "b", JobYaml.CloneBranch(branch) } });
        var body = string.Join('\n', text.Split('\n').Skip(1).Select(l => l.StartsWith("  ") ? l[2..] : l)).TrimEnd() + "\n";
        return new DesignStep(alias, StepKinds.Branch, From: JobYaml.Aliases(branch, "from"), Ref: JobYaml.Aliases(branch, "ref"), Yaml: body);
    }

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
        if (!string.IsNullOrWhiteSpace(model.Check)) text.Append("# ").Append(CheckDirective).Append(' ').Append(OneLine(model.Check)).Append('\n');
        return text.ToString();
    }

    /// <summary>Same header convention as the catalog: the first comment line is the title, the others the description.</summary>
    public static (string Title, string? Description, string? Check) ReadHeader(string yaml)
    {
        var header = yaml.Split('\n').TakeWhile(l => l.StartsWith('#')).Select(l => l.TrimStart('#').Trim()).ToList();
        var check = header.FirstOrDefault(l => l.StartsWith(CheckDirective))?[CheckDirective.Length..].Trim();
        var prose = header.Where(l => !l.StartsWith("lab-")).ToList();
        return (prose.FirstOrDefault() ?? "Untitled pipeline", prose.Count > 1 ? string.Join(' ', prose.Skip(1)) : null, check);
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
}
