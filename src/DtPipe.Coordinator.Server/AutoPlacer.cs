using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// Places a job with the help of its bindings: a branch that is bound runs on the node its binding names, every branch that is
/// neither bound nor a brick on the runner, which a binding never names. A bound branch is never sent to the runner: it is
/// placed on its node or the plan is refused. It only chooses a placement; <see cref="PlanBuilder"/> builds the fragments from
/// it exactly as for a placement the user picked, and cuts nothing. A source brick wired straight into a sink brick therefore
/// crosses no runner.
/// </summary>
public sealed class AutoPlacer(BrickCatalog bricks, NodeInventory inventory, PlanBuilder planner, RightsStore rights)
{
    /// <summary>Why a branch that reaches <paramref name="nodes"/>' data without being one of their bricks is refused.</summary>
    public static string NotABrick(string alias, IEnumerable<string> nodes) =>
        $"'{alias}' reads or writes {string.Join(", ", nodes)} without being one of its bricks. Rebuild it from the node's bricks.";

    /// <summary>Why a branch that is neither bound nor a brick is refused when no runner is connected to take it.</summary>
    public static string NeedsRunner(string alias) => $"'{alias}' needs a runner, and none is connected.";

    /// <summary>A binding as the messages name it: its brick, when it has one, and its node.</summary>
    private static string Bound(Binding binding) =>
        binding.Brick is { } brick ? $"brick '{brick}' on {binding.Node}" : $"a brick on {binding.Node}";

    /// <summary>
    /// Which brick each branch of <paramref name="yaml"/> references, by alias: what a page needs to name a placed unit.
    /// Empty for a job that is not readable.
    /// </summary>
    public IReadOnlyDictionary<string, string> BrickAliases(string yaml)
    {
        var brickOf = new Dictionary<string, string>(StringComparer.Ordinal);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
                if (BrickRef.Of(alias, branch) is { } reference && bricks.Find(reference.Key) is { } referenced) brickOf[alias] = referenced.Key;
        }
        catch (Exception) { }
        return brickOf;
    }

    /// <summary>The nodes that offer each brick branch of <paramref name="yaml"/>, by alias: the choice a binding makes. Empty for a job that is not readable.</summary>
    public IReadOnlyDictionary<string, IReadOnlyList<string>> Offers(string yaml)
    {
        var offers = new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
            {
                if (BrickRef.Of(alias, branch) is { } reference) offers[alias] = [reference.Node];
            }
        }
        catch (Exception) { }
        return offers;
    }

    /// <summary>
    /// Whether <paramref name="bindings"/> hold for <paramref name="yaml"/> now: every branch that references a brick names a node that
    /// exists and offers it (<see cref="CheckReference"/>), and the bindings name nothing else: a binding of a branch that references no
    /// brick is refused, since a bound branch never runs on the runner. Empty for a job that is not readable, which planning reports.
    /// </summary>
    public IReadOnlyList<BindingProblem> Check(string yaml, IReadOnlyDictionary<string, Binding> bindings)
    {
        var problems = new List<BindingProblem>();
        var nodes = inventory.Snapshot().ToDictionary(n => n.Name, StringComparer.Ordinal);
        var aliases = new HashSet<string>(StringComparer.Ordinal);
        bindings = BrickReferences.Expand(yaml, bindings);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
            {
                aliases.Add(alias);
                if (BrickRef.Misuse(alias, branch) is { } misuse)
                {
                    problems.Add(new(alias, BindingCauses.NotABrick, misuse, true));
                    continue;
                }
                if (BrickRef.Of(alias, branch) is { } reference)
                {
                    if (CheckReference(reference, bindings.GetValueOrDefault(alias), nodes) is { } referenced) problems.Add(referenced);
                    continue;
                }
                if (bindings.TryGetValue(alias, out var binding))
                    problems.Add(new(alias, BindingCauses.NotABrick,
                        $"'{alias}' is bound to {Bound(binding)}, but it references no brick: a bound branch never runs on the runner.", true));
            }
        }
        catch (Exception)
        {
            return [];
        }
        problems.AddRange(bindings.Keys.Where(a => !aliases.Contains(a)).Order(StringComparer.Ordinal)
            .Select(a => new BindingProblem(a, BindingCauses.NoBranch, $"The bindings name '{a}', which is no branch of the job.", true)));
        return problems;
    }

    /// <summary>
    /// Why a referenced brick does not hold, or null. A reference names its node and its brick, so there is no text to match:
    /// the node must be known, enabled and online, offer a brick of that id and of the kind the reference's place says (a source read by
    /// <c>input</c>, a sink written by <c>output</c>), and the version a check pinned must still be the
    /// brick's (strict), or the mode's own pin must hold what it compares (another mode).
    /// </summary>
    private BindingProblem? CheckReference(BrickReference reference, Binding? binding, IReadOnlyDictionary<string, NodeView> nodes)
    {
        var (alias, mode) = (reference.Alias, binding is null ? BindingModes.Strict : Bindings.ModeOf(binding));
        var bound = $"brick '{reference.Brick}' on {reference.Node}";
        var identity = rights.NodeIdentity(reference.Node);
        if (!BindingModes.All.Contains(mode))
            return new(alias, BindingCauses.UnsupportedMode, $"'{alias}' references {bound} in mode '{mode}': the modes are {string.Join(", ", BindingModes.All)}.", true);
        if (identity is null)
            return new(alias, BindingCauses.UnknownNode, $"'{alias}' references {bound}, which is no node of this coordinator.", true);
        if (!identity.Enabled)
            return new(alias, BindingCauses.UnknownNode, $"'{alias}' references {bound}, whose identity is disabled.", true);
        if (!nodes.TryGetValue(reference.Node, out var view))
            return new(alias, BindingCauses.Offline, $"'{alias}' references {bound}, which has no connected replica.", false);
        if (bricks.Find(reference.Key) is not { } brick)
            return view.Online
                ? new(alias, BindingCauses.NotOffered, $"'{alias}' references {bound}, which that node does not offer.", true)
                : new(alias, BindingCauses.Offline, $"'{alias}' references {bound}, which has no connected replica.", false);
        if (brick.Kind != (reference.IsSink ? BrickKind.Sink : BrickKind.Source))
            return new(alias, BindingCauses.NotABrick,
                $"'{alias}' references {bound}, which is {(reference.IsSink ? "not a sink: only a sink is written" : "not a source: only a source is read")} by reference.", true);
        if (mode != BindingModes.Strict) return CheckHeld(alias, binding!, bound, mode, view);
        if (binding?.Pin is not { } pin)
            return new(alias, BindingCauses.Unchecked, $"'{alias}' references {bound} and no check has pinned its version: save the pipeline again.", true);
        if (pin.Version != brick.Version)
            return new(alias, BindingCauses.BrickChanged,
                $"'{alias}' was checked against version {pin.Version} of {bound}, which is now {brick.Version}: save the pipeline again.", true);
        return view.Online ? null : new(alias, BindingCauses.Offline, $"'{alias}' references {bound}, which has no connected replica.", false);
    }

    /// <summary>
    /// Why a binding in a mode other than strict does not hold on a node that has an identity and has announced itself, or null when it
    /// does. It needs a brick that node offers and a pin a check wrote that holds what the mode compares.
    /// </summary>
    private BindingProblem? CheckHeld(string alias, Binding binding, string bound, string mode, NodeView node)
    {
        if (bricks.Find($"{binding.Node}/{binding.Brick}") is null)
            return node.Online
                ? new(alias, BindingCauses.NotOffered, $"'{alias}' is bound to {bound}, which that node does not offer.", true)
                : new(alias, BindingCauses.Offline, $"'{alias}' is bound to {bound}, which has no connected replica.", false);
        if (binding.Pin is not { } pin || (mode != BindingModes.Tolerant && pin.Contract is null))
            return new(alias, BindingCauses.Unchecked,
                $"'{alias}' is bound to {bound} in mode '{mode}' and no check has pinned it: save the pipeline or its bindings again.", true);
        return node.Online ? null : new(alias, BindingCauses.Offline, $"'{alias}' is bound to {bound}, which has no connected replica.", false);
    }

    /// <summary>
    /// The plan of <paramref name="yaml"/> under <paramref name="bindings"/>, with every branch that is neither bound nor a brick on
    /// <paramref name="runner"/> (null: none is available, and such a branch is an error, whose alias is added to
    /// <paramref name="unplaced"/>). Which runner is <see cref="LaunchPlanner"/>'s.
    /// </summary>
    public async Task<DistributionPlan> PlanAsync(
        string pipelineId, string yaml, IReadOnlyDictionary<string, Binding> bindings, NodeView? runner, CancellationToken ct,
        ICollection<string>? unplaced = null)
    {
        var errors = new List<string>();
        var placed = new Dictionary<string, string>(StringComparer.Ordinal);
        var named = new Dictionary<string, string>(StringComparer.Ordinal);
        bindings = BrickReferences.Expand(yaml, bindings);
        var nodes = inventory.Snapshot();
        var hostOf = nodes.SelectMany(n => n.Datasets.Select(d => (d.Variable, n.Name))).ToDictionary(x => x.Variable, x => x.Name, StringComparer.Ordinal);
        var problems = Check(yaml, bindings);
        var refused = problems.Where(p => p.Blocking).Select(p => p.Alias).ToHashSet(StringComparer.Ordinal);
        errors.AddRange(problems.Where(p => p.Blocking).Select(p => p.Message));

        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
            {
                // A reference the check refused, or a misuse of the scheme, never reaches a node or the runner: dtpipe does not read it.
                if (BrickRef.Misuse(alias, branch) is not null || (refused.Contains(alias) && BrickRef.Of(alias, branch) is not null)) continue;
                // A bound alias is placed on its node or not at all: it is never the runner's.
                if (bindings.TryGetValue(alias, out var binding))
                {
                    if (!refused.Contains(alias))
                    {
                        // A node with an identity that no replica has announced yet has no bricks to compose: the launch says why.
                        if (nodes.All(n => n.Name != binding.Node))
                        {
                            errors.Add(problems.FirstOrDefault(p => p.Alias == alias && p.Cause == BindingCauses.Offline)?.Message
                                ?? $"'{alias}' is bound to {Bound(binding)}, which has no connected replica.");
                            continue;
                        }
                        placed[alias] = binding.Node;
                        if (binding.Brick is { } id) named[alias] = id;
                    }
                    continue;
                }
                // A node's database is reached only through a brick its owner declared.
                var variables = JobYaml.Variables(alias, branch).Where(hostOf.ContainsKey).ToList();
                if (variables.Count > 0)
                {
                    errors.Add(NotABrick(alias, variables.Select(v => hostOf[v]).Distinct()));
                    continue;
                }
                if (runner is null)
                {
                    errors.Add(NeedsRunner(alias));
                    unplaced?.Add(alias);
                }
                else placed[alias] = runner.Name;
            }
        }
        catch (Exception ex)
        {
            errors.Add($"The job is not readable: {ex.Message}");
        }

        var plan = await planner.BuildAsync(new PlanRequest(pipelineId, yaml, [], placed, named), PlanMode.Library, ct);
        var warnings = problems.Where(p => !p.Blocking).Select(p => p.Message).ToList();
        return plan with { Errors = [.. errors, .. plan.Errors], Warnings = [.. warnings, .. plan.Warnings] };
    }
}
