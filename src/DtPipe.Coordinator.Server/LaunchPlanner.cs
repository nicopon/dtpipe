using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server;

/// <summary>
/// The plan a launch makes, and the one place a runner is chosen. <see cref="Plan"/> is the first that passes, or, when none
/// does, the first candidate's with <see cref="Refusals"/> added to its errors. <see cref="Runner"/> is the runner it uses, null
/// when no branch of the job needs one.
/// </summary>
public sealed record Launch(DistributionPlan Plan, string? Runner, IReadOnlyList<string> Refusals)
{
    public bool Ok => Plan.Deployable;

    /// <summary>Why the launch is refused, in one sentence per candidate; empty when it is not.</summary>
    public string Refusal => string.Join(" ", Plan.Errors.Append(Plan.FlowRejection ?? "").Where(s => s.Length > 0));
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
        if (candidates.Count == 0)
        {
            // No runner is online: a job that needs none still launches, and one that needs it says so.
            var plan = await placer.PlanAsync(pipelineId, yaml, bindings, null, ct);
            return new Launch(plan.Deployable ? await WithEligibilityAsync(plan) : plan, null, []);
        }

        DistributionPlan? first = null;
        var refusals = new List<string>();
        foreach (var runner in candidates)
        {
            var plan = await placer.PlanAsync(pipelineId, yaml, bindings, runner, ct);
            first ??= plan;
            var uses = plan.Fragments.Any(f => f.Node == runner.Name);
            var why = plan.Deployable ? await EligibilityRefusalAsync(plan) : Reasons(plan);
            if (why is null) return new Launch(plan, uses ? runner.Name : null, []);
            // A plan that does not use the runner fails whichever runner is chosen: the others would only repeat it.
            if (!uses) return new Launch(plan.Deployable ? Refused(plan, [why]) : plan, null, plan.Deployable ? [why] : []);
            refusals.Add($"Runner {runner.Name}: {why}");
        }
        return new Launch(Refused(first!, refusals), null, refusals);
    }

    /// <summary>The binding and fragment versions of a plan that is about to run, as the run journals them.</summary>
    public RunPlan Describe(DistributionPlan plan, string? runner, IReadOnlyDictionary<string, Binding> bindings) => new(
        plan.Id, runner,
        plan.Fragments.Select(f => new RunPlanFragment(f.Name, f.Node, f.Version)).ToList(),
        bindings.OrderBy(b => b.Key, StringComparer.Ordinal).Select(b =>
            new RunPlanBinding(b.Key, b.Value.Node, b.Value.Brick ?? "", bricks.Find($"{b.Value.Node}/{b.Value.Brick}")?.Version ?? "",
                Bindings.ModeOf(b.Value), BindingVerdicts.Exact)).ToList());

    private static DistributionPlan Refused(DistributionPlan plan, IEnumerable<string> refusals) =>
        plan with { Errors = [.. plan.Errors, .. refusals] };

    private static string Reasons(DistributionPlan plan) =>
        string.Join(" ", plan.Errors.Append(plan.FlowRejection ?? "").Where(s => s.Length > 0));

    private async Task<DistributionPlan> WithEligibilityAsync(DistributionPlan plan) =>
        await EligibilityRefusalAsync(plan) is { } why ? Refused(plan, [why]) : plan;

    /// <summary>Why a fragment of the plan has no replica that could run it now, or null when each has one.</summary>
    private async Task<string?> EligibilityRefusalAsync(DistributionPlan plan)
    {
        IReadOnlyDictionary<string, FragmentEligibility> eligibility;
        try { eligibility = await admission.EvaluateAsync(RunFragmentsOf(plan)); }
        catch (ConflictException ex) { return ex.Message; }
        var missing = plan.Fragments.Where(f => eligibility[f.Name].Eligible.Count == 0).ToList();
        return missing.Count == 0 ? null
            : string.Join(" ", missing.Select(f => $"{f.Name} has no replica that could run it ({eligibility[f.Name].Reason ?? "none is eligible"})."));
    }
}
