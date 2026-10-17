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
public sealed class AutoPlacer(BrickCatalog bricks, NodeInventory inventory, PlanBuilder planner)
{
    private static readonly IReadOnlyDictionary<string, Binding> NoBindings = new Dictionary<string, Binding>();

    /// <summary>Why a branch that reaches <paramref name="nodes"/>' data without being one of their bricks is refused.</summary>
    public static string NotABrick(string alias, IEnumerable<string> nodes) =>
        $"'{alias}' reads or writes {string.Join(", ", nodes)} without being one of its bricks. Rebuild it from the node's bricks.";

    /// <summary>A binding as the messages name it: its brick, when it has one, and its node.</summary>
    private static string Bound(Binding binding) =>
        binding.Brick is { } brick ? $"brick '{brick}' on {binding.Node}" : $"a brick on {binding.Node}";

    /// <summary>
    /// Which brick each branch of <paramref name="yaml"/> is, by alias, on the node <paramref name="bindings"/> names for it:
    /// what a page needs to name a placed unit. Empty for a job that is not readable.
    /// </summary>
    public IReadOnlyDictionary<string, string> BrickAliases(string yaml, IReadOnlyDictionary<string, Binding>? bindings = null)
    {
        bindings ??= NoBindings;
        var brickOf = new Dictionary<string, string>(StringComparer.Ordinal);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
            {
                if (!bindings.TryGetValue(alias, out var binding)) continue;
                // A binding that is not strict names its brick, whatever the branch's text is now.
                if (!Bindings.IsStrict(binding) && binding.Brick is { } id && bricks.Find($"{binding.Node}/{id}") is { } named) brickOf[alias] = named.Key;
                else if (bricks.MatchAll(branch).FirstOrDefault(b => b.Node == binding.Node) is { } brick) brickOf[alias] = brick.Key;
            }
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
                if (bricks.MatchAll(branch) is { Count: > 0 } offered)
                    offers[alias] = offered.Select(b => b.Node).Distinct().Order(StringComparer.Ordinal).ToList();
        }
        catch (Exception) { }
        return offers;
    }

    /// <summary>
    /// <paramref name="bindings"/> with the brick each one lacks filled in: the brick the branch's text designates on the bound
    /// node. A binding that names a brick is kept as it is, and so is one whose node does not offer the branch: both are
    /// <see cref="Check"/>'s to report.
    /// </summary>
    public IReadOnlyDictionary<string, Binding> Resolve(string yaml, IReadOnlyDictionary<string, Binding> bindings)
    {
        var resolved = new Dictionary<string, Binding>(bindings, StringComparer.Ordinal);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
                if (resolved.TryGetValue(alias, out var binding) && binding.Brick is null
                    && bricks.MatchAll(branch).FirstOrDefault(b => b.Node == binding.Node) is { } brick)
                    resolved[alias] = binding with { Brick = brick.Id };
        }
        catch (Exception) { }
        return resolved;
    }

    /// <summary>
    /// Whether <paramref name="bindings"/> hold for <paramref name="yaml"/> now: every branch that is a brick is bound to a node that
    /// exists and offers it, the brick a binding names is the one the branch's text designates there, and the bindings name nothing
    /// else. Empty for a job that is not readable, which planning reports.
    /// </summary>
    public IReadOnlyList<BindingProblem> Check(string yaml, IReadOnlyDictionary<string, Binding> bindings)
    {
        var problems = new List<BindingProblem>();
        var nodes = inventory.Snapshot().ToDictionary(n => n.Name, StringComparer.Ordinal);
        var aliases = new HashSet<string>(StringComparer.Ordinal);
        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
            {
                aliases.Add(alias);
                var offered = bricks.MatchAll(branch);
                if (!bindings.TryGetValue(alias, out var binding))
                {
                    if (offered.Count > 0)
                        problems.Add(new(alias, BindingCauses.Unassigned,
                            $"'{alias}' is a brick of {string.Join(", ", offered.Select(b => b.Node).Distinct())}: bind it to the node it runs on.", true));
                    continue;
                }
                var bound = Bound(binding);
                var here = offered.Where(b => b.Node == binding.Node).ToList();
                var mode = Bindings.ModeOf(binding);
                if (!BindingModes.All.Contains(mode))
                    problems.Add(new(alias, BindingCauses.UnsupportedMode,
                        $"'{alias}' is bound to {bound} in mode '{mode}': the modes are {string.Join(", ", BindingModes.All)}.", true));
                else if (mode != BindingModes.Strict)
                {
                    // The brick is held by its id: the branch's text is whatever it was when the binding was checked.
                    nodes.TryGetValue(binding.Node, out var view);
                    if (CheckHeld(alias, binding, bound, mode, view) is { } held) problems.Add(held);
                }
                else if (offered.Count == 0)
                    problems.Add(new(alias, BindingCauses.NotABrick,
                        $"'{alias}' is bound to {bound}, but its text is no longer that brick: a bound branch never runs on the runner.", true));
                else if (!nodes.TryGetValue(binding.Node, out var strictView))
                    problems.Add(new(alias, BindingCauses.UnknownNode, $"'{alias}' is bound to {bound}, which is no node of this coordinator.", true));
                else if (here.Count == 0)
                    problems.Add(new(alias, BindingCauses.NotOffered,
                        $"'{alias}' is bound to {bound}, which does not offer this brick (offered by {string.Join(", ", offered.Select(b => b.Node).Distinct())}).", true));
                else if (here.All(b => b.Id != binding.Brick))
                    problems.Add(new(alias, BindingCauses.BrickMismatch,
                        $"'{alias}' is bound to {bound}, but its text is {string.Join(" or ", here.Select(b => $"brick '{b.Id}'"))} there.", true));
                else if (!strictView.Online)
                    problems.Add(new(alias, BindingCauses.Offline, $"'{alias}' is bound to {bound}, which has no connected replica.", false));
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
    /// Why a binding in a mode other than strict does not hold, or null when it does. It needs a node that exists, a brick that node
    /// offers and a pin a check wrote that holds what the mode compares. The inventory lists a node once a replica has announced it:
    /// until then, after a restart, the cause is <see cref="BindingCauses.UnknownNode"/>, whatever the rights say.
    /// </summary>
    private BindingProblem? CheckHeld(string alias, Binding binding, string bound, string mode, NodeView? node)
    {
        if (node is null)
            return new(alias, BindingCauses.UnknownNode, $"'{alias}' is bound to {bound}, which is no node of this coordinator.", true);
        if (binding.Brick is null)
            return new(alias, BindingCauses.NotABrick,
                $"'{alias}' is bound to {bound} in mode '{mode}', which names no brick: save it again while its text is a brick of that node.", true);
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
    /// <paramref name="runner"/> (null: none is available, and such a branch is an error). Which runner is <see cref="LaunchPlanner"/>'s.
    /// </summary>
    public async Task<DistributionPlan> PlanAsync(
        string pipelineId, string yaml, IReadOnlyDictionary<string, Binding> bindings, NodeView? runner, CancellationToken ct)
    {
        var errors = new List<string>();
        var placed = new Dictionary<string, string>(StringComparer.Ordinal);
        var named = new Dictionary<string, string>(StringComparer.Ordinal);
        var nodes = inventory.Snapshot();
        var hostOf = nodes.SelectMany(n => n.Datasets.Select(d => (d.Variable, n.Name))).ToDictionary(x => x.Variable, x => x.Name, StringComparer.Ordinal);
        var problems = Check(yaml, bindings);
        var refused = problems.Where(p => p.Blocking).Select(p => p.Alias).ToHashSet(StringComparer.Ordinal);
        errors.AddRange(problems.Where(p => p.Blocking).Select(p => p.Message));

        try
        {
            foreach (var (alias, branch) in JobYaml.Branches(JobYaml.Parse(yaml)))
            {
                // A bound alias is placed on its node or not at all, and so is a brick nobody bound: neither is the runner's.
                var bound = bindings.TryGetValue(alias, out var binding);
                if (bound || bricks.MatchAll(branch).Count > 0)
                {
                    if (bound && !refused.Contains(alias))
                    {
                        placed[alias] = binding!.Node;
                        // Its brick is the one the binding names. The text says it only for a strict binding, which Check judged.
                        if (binding.Brick is { } id) named[alias] = id;
                    }
                    continue;
                }
                // A node's database is reached only through a brick its owner declared.
                var variables = JobYaml.Variables(JobYaml.Serialize(new() { { alias, branch } })).Where(hostOf.ContainsKey).ToList();
                if (variables.Count > 0)
                {
                    errors.Add(NotABrick(alias, variables.Select(v => hostOf[v]).Distinct()));
                    continue;
                }
                if (runner is null) errors.Add($"'{alias}' needs a runner, and none is connected.");
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
