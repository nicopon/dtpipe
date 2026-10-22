using System.Security.Cryptography;
using System.Text;
using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Coordinator.Server;

/// <param name="Bricks">
/// The brick (its id on the node the alias is placed on) a binding names, by alias. An alias in it is composed from that brick as the
/// catalog holds it now, whatever its text says; any other is the brick its text is on its node.
/// </param>
public sealed record PlanRequest(
    string PipelineId, string Yaml, IReadOnlyDictionary<string, string>? Placement,
    IReadOnlyDictionary<string, string>? Bricks = null);

/// <summary>
/// A placeable piece of the pipeline: one branch of the job.
/// </summary>
public sealed record UnitView(
    string Id, string YamlKey, BranchView Branch, IReadOnlyList<string> DependsOn,
    string? SuggestedNode, string? Node);

/// <summary>How a fragment reaches its node: <see cref="Bricks"/> composed by the node from its own declarations, or a <see cref="Job"/> as written.</summary>
public enum FragmentForm
{
    /// <summary>A data node's fragment, made of its bricks by id (<see cref="PlannedFragment.Composition"/>).</summary>
    Bricks,

    /// <summary>A runner's fragment, or one a lab host that accepts raw fragments runs as written.</summary>
    Job,
}

public sealed record PlannedFragment(
    string Name, string Node, IReadOnlyList<string> Groups, string Yaml, string Version, IReadOnlyList<FragmentEdge> Edges,
    IReadOnlyList<string> Units, FragmentForm Form, IReadOnlyList<FragmentUnit>? Composition = null);

public sealed record PlannedEdge(
    string ProducerFragment, string ProducerAlias, string ConsumerFragment, string ConsumerAlias,
    IReadOnlyList<string> ProducerGroups, IReadOnlyList<string> ConsumerGroups, bool Allowed)
{
    /// <summary>The edge as the flow matrix is asked about it, named the way a refusal names it.</summary>
    internal PlanEdge ToFlowEdge() =>
        new($"{ProducerFragment}.{ProducerAlias} -> {ConsumerFragment}.{ConsumerAlias}", ProducerGroups, ConsumerGroups);
}

public sealed record DistributionPlan(
    string Id, string PipelineId, IReadOnlyList<UnitView> Units,
    IReadOnlyList<PlannedFragment> Fragments, IReadOnlyList<PlannedEdge> Edges,
    IReadOnlyList<string> Errors, IReadOnlyList<string> Warnings, string? FlowRejection)
{
    public bool Deployable => Errors.Count == 0 && FlowRejection is null && Fragments.Count > 0;
}

/// <summary>Who asks for a plan, which says what the planner may do for it.</summary>
public enum PlanMode
{
    /// <summary>A plan the coordinator places itself, for a pipeline of the library: a data node's fragment is made of its bricks.</summary>
    Library,

    /// <summary>The Lab view's planning by hand: the only plan that sends a data node a fragment written as a job.</summary>
    Manual,
}

/// <summary>
/// Turns one monolithic job and a placement into one fragment per node.
/// <para>
/// The planner only regroups whole branches: a <c>from</c>/<c>ref</c> dependency that crosses two nodes becomes an
/// <c>arrow:-</c> input branch on the consumer's side and an <c>arrow:-</c> output on the
/// producer's. It never edits an option or moves a transformer.
/// </para>
/// </summary>
public sealed class PlanBuilder(
    NodeInventory inventory, BrickCatalog bricks, IPlanRegistry planRegistry, RightsStore rights)
{
    private const string ArrowEndpoint = "arrow:-";

    private sealed class Unit
    {
        public required string Id;
        public required string Key;
        public required YamlMappingNode Branch;
        public required IReadOnlyList<string> DependsOn;
    }

    private sealed class FragmentDraft(string name, string node, IReadOnlyList<string> groups)
    {
        public string Name { get; } = name;
        public string Node { get; } = node;
        public IReadOnlyList<string> Groups { get; } = groups;
        public List<(string Key, YamlMappingNode Branch)> Inbound { get; } = [];
        public List<(string Key, YamlMappingNode Branch)> Owned { get; } = [];
        public List<(string Key, YamlMappingNode Branch)> Relays { get; } = [];
        public List<FragmentEdge> Edges { get; } = [];
        public List<string> Units { get; } = [];

        public bool HasKey(string key) => Inbound.Concat(Owned).Concat(Relays).Any(b => b.Key == key);
        public YamlMappingNode Branch(string key) => Owned.First(b => b.Key == key).Branch;
    }

    /// <summary>
    /// Plans <paramref name="request"/>. A data node's fragment is made of its bricks by id; one that is not
    /// is an error naming why, unless the plan is <see cref="PlanMode.Manual"/> and the node announces that it
    /// accepts raw fragments, when it goes as a job as written.
    /// </summary>
    public DistributionPlan Build(PlanRequest request, PlanMode mode)
    {
        var planId = $"{DateTime.UtcNow:HHmmss}-{Guid.NewGuid().ToString("N")[..6]}";
        var pipelineId = Sanitize(request.PipelineId);
        var errors = new List<string>();
        var warnings = new List<string>();
        DistributionPlan Result(IReadOnlyList<UnitView> units, IReadOnlyList<PlannedFragment>? fragments = null,
            IReadOnlyList<PlannedEdge>? edges = null, string? flowRejection = null) =>
            new(planId, pipelineId, units, fragments ?? [], edges ?? [], errors, warnings, flowRejection);

        YamlMappingNode job;
        try { job = JobYaml.Parse(request.Yaml); }
        catch (Exception ex)
        {
            errors.Add($"The job is not readable: {ex.Message}");
            return Result([]);
        }

        // 1. Units, and where each one goes.
        var units = BuildUnits(job);
        var unitViews = DescribeUnits(units, request.Placement);
        var nodes = inventory.Snapshot().ToDictionary(n => n.Name, StringComparer.Ordinal);
        var placement = new Dictionary<string, string>(StringComparer.Ordinal);
        // The groups a node holds are the rights', now: the inventory keeps those of its last announcement, which a change of the
        // rights leaves behind, and a node whose identity is disabled or gone may not connect, so nothing is placed on it.
        var groupsOf = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        foreach (var view in unitViews)
        {
            if (view.Node is null) errors.Add($"'{view.Id}' is not placed on a node.");
            else if (!nodes.ContainsKey(view.Node)) errors.Add($"'{view.Id}' is placed on unknown node '{view.Node}'.");
            else
            {
                try
                {
                    if (!groupsOf.ContainsKey(view.Node)) groupsOf[view.Node] = rights.GroupsOf(view.Node);
                    placement[view.Id] = view.Node;
                }
                catch (ConflictException ex)
                {
                    errors.Add($"'{view.Id}' cannot be placed: {ex.Message}");
                }
            }
        }
        if (errors.Count > 0) return Result(unitViews);

        // 2. One fragment per node, whole units copied in.
        var drafts = new Dictionary<string, FragmentDraft>(StringComparer.Ordinal);
        FragmentDraft DraftOf(string node) => drafts.TryGetValue(node, out var d)
            ? d
            : drafts[node] = new FragmentDraft($"{pipelineId}@{node}", node, groupsOf[node]);

        foreach (var unit in units)
        {
            var draft = DraftOf(placement[unit.Id]);
            draft.Owned.Add((unit.Key, JobYaml.CloneBranch(unit.Branch)));
            draft.Units.Add(unit.Id);
        }

        var runEdges = new List<(FragmentDraft Producer, string ProducerAlias, FragmentDraft Consumer, string ConsumerAlias)>();
        void Connect(FragmentDraft producer, string producerAlias, FragmentDraft consumer, string consumerAlias)
        {
            producer.Edges.Add(new FragmentEdge(producerAlias, FragmentEdgeDirection.Outbound));
            consumer.Edges.Add(new FragmentEdge(consumerAlias, FragmentEdgeDirection.Inbound));
            runEdges.Add((producer, producerAlias, consumer, consumerAlias));
        }

        // 2a. A from/ref dependency that crosses nodes becomes an arrow edge.
        var producerOf = units.ToDictionary(u => u.Key, StringComparer.Ordinal);
        var crossings = new SortedDictionary<(string Alias, string ProducerNode), SortedSet<string>>();
        foreach (var unit in units)
        {
            foreach (var alias in unit.DependsOn)
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
            var sendDirectly = !JobYaml.Has(producerBranch, "output") && consumerNodes.Count == 1 && !readLocally;

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

        // 3. Assemble, then check what the planner can check before anything is deployed.
        var fragments = new List<PlannedFragment>();
        foreach (var draft in drafts.Values.OrderBy(d => d.Node, StringComparer.Ordinal))
        {
            var root = new YamlMappingNode();
            foreach (var (key, branch) in draft.Inbound.Concat(draft.Owned).Concat(draft.Relays))
                root.Add(key, branch);
            var yaml = JobYaml.Serialize(root);
            var form = FragmentForm.Job;
            IReadOnlyList<FragmentUnit>? composition = null;
            var node = nodes[draft.Node];
            if (node.Role != NodeRole.Runner)
            {
                if (UnitsOf(draft, request.Bricks) is { } pieces && FragmentJob.Compose(pieces, (id, kind) => BranchOf(draft.Node, id, kind)) is { Yaml: { } composed })
                {
                    (yaml, form, composition) = (composed, FragmentForm.Bricks, pieces);
                }
                else if (!(mode == PlanMode.Manual && node.Capabilities.Contains(NodeAnnouncement.RawFragments)))
                {
                    errors.Add($"{draft.Name}: {BrickRules.FragmentRefusal(yaml, [], requireBricks: true) ?? "this node runs bricks by id, and this fragment is not made of them."}");
                    continue;
                }
            }
            // A fragment of bricks is versioned by the versions of its bricks, the only thing of them the coordinator holds.
            // dtpipe does not read brick:, so a reference leaves the coordinator only as a brick its own node composes.
            // The planner has added the wiring (an arrow output, a sink's feed) by now, so a reference is read by the endpoint that holds it.
            var refused = false;
            foreach (var (key, branch) in draft.Owned)
            {
                if (new[] { "input", "output" }.Select(k => JobYaml.Scalar(branch, k)).FirstOrDefault(v => v?.StartsWith(BrickRef.Scheme, StringComparison.Ordinal) == true) is not { } input) continue;
                if (!BrickRef.TryParse(input, out var refNode, out var refBrick))
                    errors.Add($"{draft.Name}: '{key}' has a malformed brick reference '{input}'.");
                else if (refNode != draft.Node || form != FragmentForm.Bricks)
                    errors.Add($"{draft.Name}: '{key}' references {refNode}/{refBrick}, which only {refNode} composes into a fragment of bricks.");
                else continue;
                refused = true;
            }
            if (refused) continue;
            var version = composition is not null ? FragmentJob.VersionOf(composition, (id, _) => bricks.Find($"{draft.Node}/{id}")?.Version) : FragmentJob.VersionOf(yaml);

            if (draft.Edges.Count == 0)
                errors.Add($"{draft.Name} exchanges nothing with another node. A pipeline node completes only once every one of its edges is wired: cut the pipeline, or spread its branches over several nodes.");

            foreach (var refusal in VariableTokens.Refusals(yaml)) errors.Add($"{draft.Name}: {refusal}");
            var hosted = nodes[draft.Node].Datasets.Select(d => d.Variable).ToHashSet(StringComparer.Ordinal);
            foreach (var variable in JobYaml.Variables(yaml).Where(v => !hosted.Contains(v)))
            {
                var owners = nodes.Values.Where(n => n.Datasets.Any(d => d.Variable == variable)).Select(n => n.Name).ToList();
                errors.Add($"{draft.Name} uses ${{{{{variable}}}}}, which {draft.Node} does not host" +
                           (owners.Count > 0 ? $" (hosted by {string.Join(", ", owners)})." : "."));
            }

            fragments.Add(new PlannedFragment(draft.Name, draft.Node, draft.Groups, yaml, version, draft.Edges, draft.Units, form, composition));
        }

        // Allowed starts false: the matrix decides it, once, for the whole plan.
        var asked = runEdges.Select(e => new PlannedEdge(
            e.Producer.Name, e.ProducerAlias, e.Consumer.Name, e.ConsumerAlias, e.Producer.Groups, e.Consumer.Groups, Allowed: false))
            .ToList();
        var (plannedEdges, flowRejection) = PlanFlow.Judge(planRegistry, planId, asked);

        return Result(unitViews, fragments, plannedEdges, flowRejection);
    }

    /// <summary>The pieces of a data node's fragment in the closed vocabulary of ids, or <c>null</c> when a branch is no brick of that node.</summary>
    private List<FragmentUnit>? UnitsOf(FragmentDraft draft, IReadOnlyDictionary<string, string>? named)
    {
        var units = new List<FragmentUnit>();
        foreach (var (key, _) in draft.Inbound) units.Add(new FragmentUnit(key, FragmentUnitKind.Inbound));
        foreach (var (key, branch) in draft.Owned)
        {
            var brick = named?.TryGetValue(key, out var id) == true ? bricks.Find($"{draft.Node}/{id}") : null;
            if (brick is null) return null;
            if (brick.Kind == BrickKind.Source)
            {
                var sends = JobYaml.Scalar(branch, "output") == ArrowEndpoint;
                units.Add(new FragmentUnit(key, sends ? FragmentUnitKind.SourceSend : FragmentUnitKind.Source, brick.Id));
            }
            else if (JobYaml.Aliases(branch, "from") is [var feed])
            {
                units.Add(new FragmentUnit(key, FragmentUnitKind.Sink, brick.Id, feed));
            }
            else return null;
        }
        foreach (var (key, branch) in draft.Relays)
            units.Add(new FragmentUnit(key, FragmentUnitKind.Relay, From: JobYaml.Scalar(branch, "from")));
        return units;
    }

    private YamlMappingNode? BranchOf(string node, string id, BrickKind kind) =>
        bricks.Find($"{node}/{id}") is { } view && view.Kind == kind ? bricks.BranchOf(view) : null;

    private static List<Unit> BuildUnits(YamlMappingNode job)
    {
        var units = new List<Unit>();
        foreach (var (alias, branch) in JobYaml.Branches(job))
        {
            var dependsOn = JobYaml.Aliases(branch, "from").Concat(JobYaml.Aliases(branch, "ref")).ToList();
            units.Add(new Unit { Id = alias, Key = alias, Branch = branch, DependsOn = dependsOn });
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
            return new UnitView(u.Id, u.Key, views[u.Id], u.DependsOn, suggested[u.Id],
                string.IsNullOrEmpty(chosen) ? suggested[u.Id] : chosen);
        }).ToList();
    }

    private static string Sanitize(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) || c is '-' or '_' ? c : '_'));
}
