using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The plan a launch makes, and the one place a runner is chosen. <see cref="Plan"/> is the first that passes, or, when none
/// does, the first candidate's with <see cref="Refusals"/> added to its errors. <see cref="Runner"/> is the runner it uses, null
/// when no branch of the job needs one.
/// </summary>
public sealed record Launch(DistributionPlan Plan, string? Runner, IReadOnlyList<string> Refusals, IReadOnlyList<LaunchCause>? Causes = null)
{
    public bool Ok => Plan.Deployable;

    /// <summary>Why the launch is refused, in one sentence per candidate; empty when it is not.</summary>
    public string Refusal => string.Join(" ", Plan.Errors.Append(Plan.FlowRejection ?? "").Where(s => s.Length > 0));
}

/// <summary>Why a launch is refused, which codes are <see cref="LaunchCauses"/>: <paramref name="Fragment"/> and <paramref name="Node"/> say where, when it is somewhere.</summary>
public sealed record LaunchCause(string Code, string Message, string? Node = null, string? Fragment = null);

/// <summary>What keeps a launch from being made, as a code a health read reports. The binding problems are <see cref="BindingCauses"/>'.</summary>
public static class LaunchCauses
{
    /// <summary>A branch needs a runner and none is online.</summary>
    public const string NoRunner = "no-runner";

    /// <summary>The runners online were tried and none can take the launch: a runner's own fragment has no replica that could run it.</summary>
    public const string NoAdmissibleRunner = "no-admissible-runner";

    /// <summary>The flow matrix forbids an edge of the plan.</summary>
    public const string FlowRejected = "flow-rejected";

    /// <summary>A fragment of a data node has no replica that could run it now.</summary>
    public const string NoEligibleReplica = "no-eligible-replica";

    /// <summary>The planner refused the plan for a reason none of the others names.</summary>
    public const string PlanRefused = "plan-refused";
}

/// <summary>
/// Plans a library pipeline when it is launched. The runner is stateless and interchangeable, so it is not part of what a person
/// decides: the runners online are tried by runs in flight, then by name, and the first whose plan the flow matrix allows and
/// whose every fragment has an eligible replica is the one. A runner that fails is skipped, and a launch that none can serve names
/// each one's reason. The planner already judges the plan's edges against the matrix (<see cref="DistributionPlan.FlowRejection"/>),
/// which is how a runner in the wrong groups is skipped.
/// </summary>
internal sealed class LaunchPlanner(AutoPlacer placer, BrickCatalog bricks, NodeInventory inventory, ReplicaAdmission admission, RightsStore rights)
{
    /// <summary>The plan's fragments as a run names them, read from the rights as they are now.</summary>
    public IReadOnlyList<RunFragment> RunFragmentsOf(DistributionPlan plan) =>
        plan.Fragments.Select(f => new RunFragment(SpecOf(f), rights.SubjectOf(f.Node), rights.GroupsOf(f.Node))).ToList();

    /// <summary>The fragment as a replica is asked to run it.</summary>
    public static FragmentSpec SpecOf(PlannedFragment fragment) => new(
        fragment.Name, fragment.Version, fragment.Edges, fragment.Composition, fragment.Form == FragmentForm.Bricks ? null : fragment.Yaml);

    /// <summary>The online runners in the order a launch tries them: the fewest runs in flight first, then by name.</summary>
    public IReadOnlyList<NodeView> Runners() => inventory.Snapshot()
        .Where(n => n.Role == NodeRole.Runner && n.Online)
        .OrderBy(n => n.Replicas.Where(r => r.Connected).Sum(r => r.Runs)).ThenBy(n => n.Name, StringComparer.Ordinal)
        .ToList();

    public async Task<Launch> ChooseAsync(string pipelineId, string yaml, IReadOnlyDictionary<string, Binding> bindings, CancellationToken ct)
    {
        var candidates = Runners();
        var own = placer.Check(yaml, bindings).Select(p => p.Message).ToHashSet(StringComparer.Ordinal);
        if (candidates.Count == 0)
        {
            // No runner is online: a job that needs none still launches, and one that needs it says so.
            var unplaced = new List<string>();
            var plan = await placer.PlanAsync(pipelineId, yaml, bindings, null, ct, unplaced);
            var causes = plan.Deployable ? [] : CausesOfPlan(plan, own, unplaced);
            if (plan.Deployable)
            {
                var (refusal, found) = await EligibilityAsync(plan);
                if (refusal is not null) (plan, causes) = (Refused(plan, [refusal]), found);
            }
            return new Launch(plan, null, [], causes);
        }

        DistributionPlan? first = null;
        var refusals = new List<string>();
        var reasons = new List<LaunchCause>();
        foreach (var runner in candidates)
        {
            var plan = await placer.PlanAsync(pipelineId, yaml, bindings, runner, ct);
            first ??= plan;
            var uses = plan.Fragments.Any(f => f.Node == runner.Name);
            var (refusal, found) = plan.Deployable ? await EligibilityAsync(plan) : (null, []);
            var why = plan.Deployable ? refusal : Reasons(plan);
            if (why is null) return new Launch(plan, uses ? runner.Name : null, [], []);
            var causes = plan.Deployable ? found : CausesOfPlan(plan, own, []);
            // A plan that does not use the runner fails whichever runner is chosen: the others would only repeat it.
            if (!uses) return new Launch(plan.Deployable ? Refused(plan, [why]) : plan, null, plan.Deployable ? [why] : [], causes);
            refusals.Add($"Runner {runner.Name}: {why}");
            reasons.AddRange(causes);
        }
        return new Launch(Refused(first!, refusals), null, refusals, [.. reasons.DistinctBy(c => (c.Code, c.Fragment, c.Message))]);
    }

    /// <summary>
    /// The causes a plan that is not deployable gives, other than the binding problems <paramref name="own"/> already names: a flow
    /// rejection, a branch no runner took, and whatever else the planner said.
    /// </summary>
    private static List<LaunchCause> CausesOfPlan(DistributionPlan plan, IReadOnlySet<string> own, IReadOnlyList<string> unplaced)
    {
        var causes = new List<LaunchCause>();
        if (plan.FlowRejection is { Length: > 0 } rejection) causes.Add(new(LaunchCauses.FlowRejected, rejection));
        var needing = unplaced.Select(AutoPlacer.NeedsRunner).ToHashSet(StringComparer.Ordinal);
        if (unplaced.Count > 0)
            causes.Add(new(LaunchCauses.NoRunner, $"No runner is connected, and {string.Join(", ", unplaced.Select(a => $"'{a}'"))} need{(unplaced.Count == 1 ? "s" : "")} one."));
        causes.AddRange(plan.Errors.Where(e => !own.Contains(e) && !needing.Contains(e)).Select(e => new LaunchCause(LaunchCauses.PlanRefused, e)));
        return causes;
    }

    /// <summary>
    /// The binding and fragment versions of a plan that is about to run, as the run journals them. <paramref name="verdicts"/> are the
    /// modes' findings by alias; a binding with none is strict, which found its text equal to the brick's.
    /// </summary>
    public RunPlan Describe(
        DistributionPlan plan, string? runner, IReadOnlyDictionary<string, Binding> bindings, IReadOnlyDictionary<string, BindingVerdict> verdicts) => new(
        plan.Id, runner,
        plan.Fragments.Select(f => new RunPlanFragment(f.Name, f.Node, f.Version)).ToList(),
        bindings.OrderBy(b => b.Key, StringComparer.Ordinal).Select(b =>
            new RunPlanBinding(b.Key, b.Value.Node, b.Value.Brick ?? "", bricks.Find($"{b.Value.Node}/{b.Value.Brick}")?.Version ?? "",
                Bindings.ModeOf(b.Value), verdicts.GetValueOrDefault(b.Key)?.Verdict ?? BindingVerdicts.Exact)).ToList());

    private static DistributionPlan Refused(DistributionPlan plan, IEnumerable<string> refusals) =>
        plan with { Errors = [.. plan.Errors, .. refusals] };

    private static string Reasons(DistributionPlan plan) =>
        string.Join(" ", plan.Errors.Append(plan.FlowRejection ?? "").Where(s => s.Length > 0));

    /// <summary>For each fragment of the plan, the replicas that could run it now; null when a node of the plan has no usable identity.</summary>
    public async Task<IReadOnlyDictionary<string, FragmentEligibility>?> EligibilityOfAsync(DistributionPlan plan)
    {
        try { return await admission.EvaluateAsync(RunFragmentsOf(plan)); }
        catch (ConflictException) { return null; }
    }

    /// <summary>The fragments of a plan with no replica that could run them now: why in a sentence, and as causes. Null when each has one.</summary>
    private async Task<(string? Refusal, List<LaunchCause> Causes)> EligibilityAsync(DistributionPlan plan)
    {
        IReadOnlyDictionary<string, FragmentEligibility> eligibility;
        try { eligibility = await admission.EvaluateAsync(RunFragmentsOf(plan)); }
        catch (ConflictException ex) { return (ex.Message, [new(LaunchCauses.PlanRefused, ex.Message)]); }
        var missing = plan.Fragments.Where(f => eligibility[f.Name].Eligible.Count == 0).ToList();
        if (missing.Count == 0) return (null, []);
        var runners = inventory.Snapshot().Where(n => n.Role == NodeRole.Runner).Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
        var refusal = string.Join(" ", missing.Select(f => $"{f.Name} has no replica that could run it ({eligibility[f.Name].Reason ?? "none is eligible"})."));
        var causes = missing.Select(f => new LaunchCause(
            runners.Contains(f.Node) ? LaunchCauses.NoAdmissibleRunner : LaunchCauses.NoEligibleReplica,
            $"{f.Name} has no replica that could run it ({eligibility[f.Name].Reason ?? "none is eligible"}).", f.Node, f.Name)).ToList();
        return (refusal, causes);
    }
}
