using System.Security.Cryptography;
using System.Text;
using DtPipe.Coordinator;
using DtPipe.Lab.Contracts;
using TransportR.Interfaces;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Lab.Coordinator;

public sealed record CutRequest(string Branch, int At);

public sealed record PlanRequest(
    string PipelineId, string Yaml, IReadOnlyList<CutRequest>? Cuts, IReadOnlyDictionary<string, string>? Placement);

/// <summary>
/// A placeable piece of the pipeline: a branch, or the head of a branch a cut separated from it
/// (id <c>alias^</c>). A head feeds its tail over an <c>arrow:</c> edge and nothing else.
/// </summary>
public sealed record UnitView(
    string Id, string YamlKey, bool IsHead, BranchView Branch, IReadOnlyList<string> DependsOn,
    string? SuggestedNode, string? Node);

public sealed record PlannedFragment(
    string Name, string Node, string Group, string Yaml, string Version, IReadOnlyList<LabEdge> Edges,
    IReadOnlyList<string> Units);

public sealed record PlannedEdge(
    string ProducerFragment, string ProducerAlias, string ConsumerFragment, string ConsumerAlias,
    string ProducerGroup, string ConsumerGroup, bool Allowed);

public sealed record LabPlan(
    string Id, string PipelineId, IReadOnlyList<CutRequest> Cuts, IReadOnlyList<UnitView> Units,
    IReadOnlyList<PlannedFragment> Fragments, IReadOnlyList<PlannedEdge> Edges,
    IReadOnlyList<string> Errors, IReadOnlyList<string> Warnings, string? FlowRejection)
{
    public bool Deployable => Errors.Count == 0 && FlowRejection is null && Fragments.Count > 0;
}

/// <summary>
/// Turns one monolithic job, a set of cuts and a placement into one fragment per node.
/// <para>
/// A cut inside a branch is <c>dtpipe split</c>'s: the lab runs it and keeps both halves as
/// written, so where each option lands stays dtpipe's decision. Only the regrouping of whole
/// branches is the lab's own: a <c>from</c>/<c>ref</c> dependency that crosses two nodes becomes an
/// <c>arrow:-</c> input branch on the consumer's side and an <c>arrow:-</c> output on the
/// producer's - the shape <c>dtpipe split</c> itself gives a cut. It never edits an option.
/// </para>
/// </summary>
public sealed class PlanBuilder(
    LabOptions options, DtPipeCli cli, NodeInventory inventory, IPlanRegistry planRegistry, IFlowControlService flowControl)
{
    private const string ArrowEndpoint = "arrow:-";
    private static readonly TimeSpan SplitTimeout = TimeSpan.FromSeconds(90);

    private sealed class Unit
    {
        public required string Id;
        public required string Key;
        public required bool IsHead;
        public required YamlMappingNode Branch;
        public required IReadOnlyList<string> DependsOn;
    }

    private sealed class FragmentDraft(string name, string node, string group)
    {
        public string Name { get; } = name;
        public string Node { get; } = node;
        public string Group { get; } = group;
        public List<(string Key, YamlMappingNode Branch)> Inbound { get; } = [];
        public List<(string Key, YamlMappingNode Branch)> Owned { get; } = [];
        public List<(string Key, YamlMappingNode Branch)> Relays { get; } = [];
        public List<LabEdge> Edges { get; } = [];
        public List<string> Units { get; } = [];

        public bool HasKey(string key) => Inbound.Concat(Owned).Concat(Relays).Any(b => b.Key == key);
        public YamlMappingNode Branch(string key) => Owned.First(b => b.Key == key).Branch;
    }

    public async Task<LabPlan> BuildAsync(PlanRequest request, CancellationToken ct)
    {
        var planId = $"{DateTime.UtcNow:HHmmss}-{Guid.NewGuid().ToString("N")[..6]}";
        var pipelineId = Sanitize(request.PipelineId);
        var cuts = request.Cuts ?? [];
        var errors = new List<string>();
        var warnings = new List<string>();
        LabPlan Result(IReadOnlyList<UnitView> units, IReadOnlyList<PlannedFragment>? fragments = null,
            IReadOnlyList<PlannedEdge>? edges = null, string? flowRejection = null) =>
            new(planId, pipelineId, cuts, units, fragments ?? [], edges ?? [], errors, warnings, flowRejection);

        YamlMappingNode job;
        try { job = JobYaml.Parse(request.Yaml); }
        catch (Exception ex)
        {
            errors.Add($"The job is not readable: {ex.Message}");
            return Result([]);
        }

        // 1. Cuts inside branches, one dtpipe split each, applied in turn to what remains.
        var heads = await ApplyCutsAsync(planId, request.Yaml, cuts, errors, ct);
        if (heads is null) return Result(DescribeUnits(BuildUnits(job, new Dictionary<string, YamlMappingNode>()), request.Placement));
        var (remaining, headBranches) = heads.Value;

        // 2. Units, and where each one goes.
        var units = BuildUnits(remaining, headBranches);
        var unitViews = DescribeUnits(units, request.Placement);
        var nodes = inventory.Snapshot().ToDictionary(n => n.Name, StringComparer.Ordinal);
        var placement = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var view in unitViews)
        {
            if (view.Node is null) errors.Add($"'{view.Id}' is not placed on a node.");
            else if (!nodes.ContainsKey(view.Node)) errors.Add($"'{view.Id}' is placed on unknown node '{view.Node}'.");
            else placement[view.Id] = view.Node;
        }
        if (errors.Count > 0) return Result(unitViews);

        // 3. One fragment per node, whole units copied in.
        var drafts = new Dictionary<string, FragmentDraft>(StringComparer.Ordinal);
        FragmentDraft DraftOf(string node) => drafts.TryGetValue(node, out var d)
            ? d
            : drafts[node] = new FragmentDraft($"{pipelineId}@{node}", node, nodes[node].Group);

        foreach (var unit in units)
        {
            var draft = DraftOf(placement[unit.Id]);
            // Only a cut's two halves share a key; 3a names that mistake.
            if (draft.HasKey(unit.Key)) continue;
            draft.Owned.Add((unit.Key, JobYaml.CloneBranch(unit.Branch)));
            draft.Units.Add(unit.Id);
        }

        var runEdges = new List<(FragmentDraft Producer, string ProducerAlias, FragmentDraft Consumer, string ConsumerAlias)>();
        void Connect(FragmentDraft producer, string producerAlias, FragmentDraft consumer, string consumerAlias)
        {
            producer.Edges.Add(new LabEdge(producerAlias, LabEdgeDirection.Outbound));
            consumer.Edges.Add(new LabEdge(consumerAlias, LabEdgeDirection.Inbound));
            runEdges.Add((producer, producerAlias, consumer, consumerAlias));
        }

        // 3a. A cut's two halves: dtpipe split already wrote arrow:- on both sides.
        foreach (var head in units.Where(u => u.IsHead))
        {
            var headNode = placement[head.Id];
            var tailNode = placement[head.Key];
            if (headNode == tailNode)
            {
                errors.Add($"The cut of '{head.Key}' leaves both halves on {headNode}: place '{head.Id}' and '{head.Key}' on different nodes, or remove the cut.");
                continue;
            }
            Connect(drafts[headNode], head.Key, drafts[tailNode], head.Key);
        }

        // 3b. A from/ref dependency that crosses nodes becomes an arrow edge.
        var producerOf = units.Where(u => !u.IsHead).ToDictionary(u => u.Key, StringComparer.Ordinal);
        var crossings = new SortedDictionary<(string Alias, string ProducerNode), SortedSet<string>>();
        foreach (var unit in units.Where(u => !u.IsHead))
        {
            foreach (var alias in unit.DependsOn.Where(a => !a.EndsWith('^')))
            {
                if (!producerOf.TryGetValue(alias, out var producer))
                {
                    errors.Add($"'{unit.Id}' reads '{alias}', which no branch defines.");
                    continue;
                }
                var (from, to) = (placement[producer.Id], placement[unit.Id]);
                if (from == to) continue;
                if (!crossings.TryGetValue((alias, from), out var consumers))
                    crossings[(alias, from)] = consumers = new SortedSet<string>(StringComparer.Ordinal);
                consumers.Add(to);
            }
        }

        foreach (var ((alias, producerNode), consumerNodes) in crossings)
        {
            var producer = drafts[producerNode];
            var producerBranch = producer.Branch(alias);
            var readLocally = units.Any(u => placement[u.Id] == producerNode && u.DependsOn.Contains(alias));
            // A cut's tail already receives its head under this alias, and a fragment declares each
            // edge alias once: what it sends on goes out through a relay.
            var isCutTail = units.Any(u => u.IsHead && u.Key == alias);
            var sendDirectly = !JobYaml.Has(producerBranch, "output") && consumerNodes.Count == 1 && !readLocally && !isCutTail;

            foreach (var consumerNode in consumerNodes)
            {
                var consumer = drafts[consumerNode];
                if (consumer.HasKey(alias))
                {
                    errors.Add($"{consumer.Name} would need two branches named '{alias}'. Move one of them.");
                    continue;
                }
                consumer.Inbound.Add((alias, new YamlMappingNode { { "input", ArrowEndpoint } }));

                string producerAlias;
                if (sendDirectly)
                {
                    JobYaml.Set(producerBranch, "output", ArrowEndpoint);
                    producerAlias = alias;
                }
                else
                {
                    // The branch already writes somewhere, is read on its own node too, or feeds
                    // several nodes: each remote reader gets its own relay branch.
                    producerAlias = $"{alias}__to__{Sanitize(consumerNode)}";
                    producer.Relays.Add((producerAlias, new YamlMappingNode { { "from", alias }, { "output", ArrowEndpoint } }));
                }
                Connect(producer, producerAlias, consumer, alias);
            }
        }

        if (errors.Count > 0) return Result(unitViews);

        // 4. Assemble, then check what the lab can check before anything is deployed.
        var fragments = new List<PlannedFragment>();
        foreach (var draft in drafts.Values.OrderBy(d => d.Node, StringComparer.Ordinal))
        {
            var root = new YamlMappingNode();
            foreach (var (key, branch) in draft.Inbound.Concat(draft.Owned).Concat(draft.Relays))
                root.Add(key, branch);
            var yaml = JobYaml.Serialize(root);
            var version = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(yaml)));

            if (draft.Edges.Count == 0)
                errors.Add($"{draft.Name} exchanges nothing with another node. A pipeline node completes only once every one of its edges is wired: cut the pipeline, or spread its branches over several nodes.");

            var hosted = nodes[draft.Node].Datasets.Select(d => d.Variable).ToHashSet(StringComparer.Ordinal);
            foreach (var variable in JobYaml.Variables(yaml).Where(v => !hosted.Contains(v)))
            {
                var owners = nodes.Values.Where(n => n.Datasets.Any(d => d.Variable == variable)).Select(n => n.Name).ToList();
                errors.Add($"{draft.Name} uses ${{{{{variable}}}}}, which {draft.Node} does not host" +
                           (owners.Count > 0 ? $" (hosted by {string.Join(", ", owners)})." : "."));
            }

            fragments.Add(new PlannedFragment(draft.Name, draft.Node, draft.Group, yaml, version, draft.Edges, draft.Units));
        }

        var plannedEdges = runEdges.Select(e => new PlannedEdge(
            e.Producer.Name, e.ProducerAlias, e.Consumer.Name, e.ConsumerAlias, e.Producer.Group, e.Consumer.Group,
            flowControl.CanSendTo(new HashSet<string> { e.Producer.Group }, new HashSet<string> { e.Consumer.Group })))
            .ToList();

        string? flowRejection = null;
        try
        {
            planRegistry.Register(new FlowPlan(planId, plannedEdges
                .Select(e => new PlanEdge($"{e.ProducerFragment}.{e.ProducerAlias} -> {e.ConsumerFragment}.{e.ConsumerAlias}",
                    e.ProducerGroup, e.ConsumerGroup))
                .ToList()));
        }
        catch (PlanRejectedException ex)
        {
            flowRejection = ex.Message;
        }

        return Result(unitViews, fragments, plannedEdges, flowRejection);
    }

    private async Task<(YamlMappingNode Remaining, Dictionary<string, YamlMappingNode> Heads)?> ApplyCutsAsync(
        string planId, string yaml, IReadOnlyList<CutRequest> cuts, List<string> errors, CancellationToken ct)
    {
        var heads = new Dictionary<string, YamlMappingNode>(StringComparer.Ordinal);
        var workDir = Path.Combine(options.StateDir, "plans", planId);
        Directory.CreateDirectory(workDir);

        var current = yaml;
        for (var i = 0; i < cuts.Count; i++)
        {
            var cut = cuts[i];
            var branches = JobYaml.Branches(JobYaml.Parse(current)).ToDictionary(b => b.Alias, b => b.Branch, StringComparer.Ordinal);
            if (heads.ContainsKey(cut.Branch))
            {
                errors.Add($"'{cut.Branch}' is cut more than once; the lab keeps one cut per branch.");
                return null;
            }
            if (!branches.TryGetValue(cut.Branch, out var branch) || !JobGraph.Describe(cut.Branch, branch).Cuttable)
            {
                errors.Add($"'{cut.Branch}' is not a branch reading a source; dtpipe split cuts only those.");
                return null;
            }

            var jobPath = Path.Combine(workDir, $"job-{i}.yaml");
            await File.WriteAllTextAsync(jobPath, current, ct);
            var prefix = Path.Combine(workDir, $"cut-{i}");
            var result = await cli.RunAsync(
                ["split", jobPath, "--branch", cut.Branch, "--at", cut.At.ToString(), "--out", prefix, "--acknowledge"],
                workDir, SplitTimeout, ct);
            if (!result.Succeeded)
            {
                errors.Add($"dtpipe split refused to cut '{cut.Branch}' after stage {cut.At}:\n{result.Diagnostic()}");
                return null;
            }

            var producer = JobYaml.Parse(await File.ReadAllTextAsync(prefix + "-producer.yaml", ct));
            heads[cut.Branch] = JobYaml.Branches(producer).Single(b => b.Alias == cut.Branch).Branch;
            current = await File.ReadAllTextAsync(prefix + "-consumer.yaml", ct);
        }

        return (JobYaml.Parse(current), heads);
    }

    private static List<Unit> BuildUnits(YamlMappingNode remaining, IReadOnlyDictionary<string, YamlMappingNode> heads)
    {
        var units = new List<Unit>();
        foreach (var (alias, branch) in JobYaml.Branches(remaining))
        {
            if (heads.TryGetValue(alias, out var head))
                units.Add(new Unit { Id = alias + "^", Key = alias, IsHead = true, Branch = head, DependsOn = [] });

            var dependsOn = JobYaml.Aliases(branch, "from").Concat(JobYaml.Aliases(branch, "ref")).ToList();
            if (heads.ContainsKey(alias)) dependsOn.Insert(0, alias + "^");
            units.Add(new Unit { Id = alias, Key = alias, IsHead = false, Branch = branch, DependsOn = dependsOn });
        }
        return units;
    }

    private List<UnitView> DescribeUnits(IReadOnlyList<Unit> units, IReadOnlyDictionary<string, string>? placement)
    {
        var hostOf = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var node in inventory.Snapshot())
            foreach (var dataset in node.Datasets)
                hostOf.TryAdd(dataset.Variable, node.Name);

        // A database the unit names decides where it can run; the first one wins. A unit naming
        // none (a join, a merge) follows its first reader, else its first source.
        var views = units.ToDictionary(u => u.Id, u => JobGraph.Describe(u.Key, u.Branch), StringComparer.Ordinal);
        var suggested = units.ToDictionary(u => u.Id,
            u => views[u.Id].Variables.Select(v => hostOf.GetValueOrDefault(v)).FirstOrDefault(n => n is not null),
            StringComparer.Ordinal);
        for (var pass = 0; pass < units.Count; pass++)
        {
            foreach (var unit in units.Where(u => suggested[u.Id] is null))
            {
                suggested[unit.Id] =
                    units.Where(r => r.DependsOn.Contains(unit.Id)).Select(r => suggested[r.Id]).FirstOrDefault(n => n is not null)
                    ?? unit.DependsOn.Select(d => suggested.GetValueOrDefault(d)).FirstOrDefault(n => n is not null);
            }
        }

        return units.Select(u =>
        {
            var chosen = placement?.GetValueOrDefault(u.Id);
            return new UnitView(u.Id, u.Key, u.IsHead, views[u.Id], u.DependsOn, suggested[u.Id],
                string.IsNullOrEmpty(chosen) ? suggested[u.Id] : chosen);
        }).ToList();
    }

    private static string Sanitize(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' ? c : '_'));
}
