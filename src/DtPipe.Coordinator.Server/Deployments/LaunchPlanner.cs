using DtPipe.Coordinator;
using DtPipe.Coordinator.Contracts;
using DtPipe.Coordinator.Server.Access;
using DtPipe.Coordinator.Server.Library;
using DtPipe.Coordinator.Server.Nodes;
using DtPipe.Coordinator.Server.Runs;
using DtPipe.Jobs.Coordinator;

namespace DtPipe.Coordinator.Server.Deployments;

/// <summary>
/// The plan a launch makes, and the one place a runner is chosen. <see cref="Plan"/> is the first that passes, or, when none
/// does, the first candidate's with <see cref="Refusals"/> added to its errors. <see cref="Runner"/> is the runner it uses, null
/// when no branch of the job needs one. <see cref="EdgeProtections"/> is what the replicas connected now say of each edge of
/// <see cref="Plan"/>, judged once by <see cref="ReplicaAdmission.EdgeJudgments"/>: the plan of a launch that is not deployable has none
/// judged, and null reads as that.
/// </summary>
public sealed record Launch(
    DistributionPlan Plan, string? Runner, IReadOnlyList<string> Refusals, IReadOnlyList<LaunchCause>? Causes = null,
    IReadOnlyList<EdgeJudgment>? EdgeProtections = null)
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

    /// <summary>The two ends of an edge protect the data plane so that no pair of their nodes' connected replicas joins.</summary>
    public const string ProtectionMismatch = "protection-mismatch";

    /// <summary>The planner refused the plan for a reason none of the others names.</summary>
    public const string PlanRefused = "plan-refused";
}

/// <summary>
/// Plans a library pipeline when it is launched. The runner is stateless and interchangeable, so it is not part of what a person
/// decides: the runners online are tried by runs in flight, then by name, and the first whose plan the flow matrix allows, whose
/// every fragment has an eligible replica and whose every edge has a pair of connected replicas that join
/// (<see cref="ReplicaAdmission.EdgeRefusals"/>) is the one. A runner that fails is skipped, and a launch that none can serve names
/// each one's reason. The planner already judges the plan's edges against the matrix (<see cref="DistributionPlan.FlowRejection"/>),
/// which is how a runner in the wrong groups is skipped; a runner that protects the data plane otherwise than the nodes it joins
/// is skipped the same way.
/// </summary>
internal sealed class LaunchPlanner(AutoPlacer placer, BrickCatalog bricks, NodeInventory inventory, ReplicaAdmission admission, RightsStore rights)
{
    /// <summary>The plan's fragments as a run names them, read from the rights as they are now.</summary>
    public IReadOnlyList<RunFragment> RunFragmentsOf(DistributionPlan plan) =>
        plan.Fragments.Select(f => new RunFragment(SpecOf(f), rights.SubjectOf(f.Node), rights.GroupsOf(f.Node))).ToList();

    /// <summary>The fragment as a replica is asked to run it.</summary>
    public static FragmentSpec SpecOf(PlannedFragment fragment) => new(
        fragment.Name, fragment.Version, fragment.Edges, fragment.Composition, fragment.Form == FragmentForm.Bricks ? null : fragment.Yaml);

    /// <summary>The plan's edges as a run names them: each pairs the fragment that sends with the fragment that receives.</summary>
    public static IReadOnlyList<RunEdge> RunEdgesOf(DistributionPlan plan) =>
        plan.Edges.Select(e => new RunEdge(e.ProducerFragment, e.ProducerAlias, e.ConsumerFragment, e.ConsumerAlias)).ToList();

    /// <summary>
    /// The edges of the plan that no pair of the replicas connected now could carry, a sentence each
    /// (<see cref="ReplicaAdmission.EdgeRefusals"/>). A node with no usable identity is not judged here: the check that follows names it.
    /// </summary>
    public IReadOnlyList<string> ProtectionRefusals(DistributionPlan plan)
    {
        try { return admission.EdgeRefusals(RunFragmentsOf(plan), RunEdgesOf(plan)); }
        catch (ConflictException) { return []; }
    }

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
            var plan = placer.Plan(pipelineId, yaml, bindings, null, unplaced);
            var causes = plan.Deployable ? [] : CausesOfPlan(plan, own, unplaced);
            IReadOnlyList<EdgeJudgment> judged = [];
            if (plan.Deployable)
            {
                var (refusal, found, edges) = await EligibilityAsync(plan);
                judged = edges;
                if (refusal is not null) (plan, causes) = (Refused(plan, [refusal]), found);
            }
            return new Launch(plan, null, [], causes, judged);
        }

        DistributionPlan? first = null;
        IReadOnlyList<EdgeJudgment> firstJudged = [];
        var refusals = new List<string>();
        var reasons = new List<LaunchCause>();
        foreach (var runner in candidates)
        {
            var plan = placer.Plan(pipelineId, yaml, bindings, runner);
            var uses = plan.Fragments.Any(f => f.Node == runner.Name);
            var (refusal, found, judged) = plan.Deployable ? await EligibilityAsync(plan) : (null, [], []);
            if (first is null) (first, firstJudged) = (plan, judged);
            var why = plan.Deployable ? refusal : Reasons(plan);
            if (why is null) return new Launch(plan, uses ? runner.Name : null, [], [], judged);
            var causes = plan.Deployable ? found : CausesOfPlan(plan, own, []);
            // A plan that does not use the runner fails whichever runner is chosen: the others would only repeat it.
            if (!uses) return new Launch(plan.Deployable ? Refused(plan, [why]) : plan, null, plan.Deployable ? [why] : [], causes, judged);
            refusals.Add($"Runner {runner.Name}: {why}");
            reasons.AddRange(causes);
        }
        return new Launch(Refused(first!, refusals), null, refusals, [.. reasons.DistinctBy(c => (c.Code, c.Fragment, c.Message))], firstJudged);
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

    /// <summary>
    /// What keeps a plan from running on the replicas connected now: the fragments with no replica that could run them, and the edges
    /// no pair of replicas could carry (<see cref="ReplicaAdmission.EdgeJudgments"/>, whose refusals are the edges' sentences). Why in a
    /// sentence, and as causes, null when neither is found; and the judgment of each edge, which is empty when a node of the plan has no
    /// usable identity, since none is then judged.
    /// </summary>
    private async Task<(string? Refusal, List<LaunchCause> Causes, IReadOnlyList<EdgeJudgment> Edges)> EligibilityAsync(DistributionPlan plan)
    {
        IReadOnlyList<RunFragment> fragments;
        IReadOnlyDictionary<string, FragmentEligibility> eligibility;
        try
        {
            fragments = RunFragmentsOf(plan);
            eligibility = await admission.EvaluateAsync(fragments);
        }
        catch (ConflictException ex) { return (ex.Message, [new(LaunchCauses.PlanRefused, ex.Message)], []); }
        var missing = plan.Fragments.Where(f => eligibility[f.Name].Eligible.Count == 0).ToList();
        var judged = admission.EdgeJudgments(fragments, RunEdgesOf(plan));
        var apart = EdgeJudgment.RefusalsOf(judged);
        if (missing.Count == 0 && apart.Count == 0) return (null, [], judged);
        var runners = inventory.Snapshot().Where(n => n.Role == NodeRole.Runner).Select(n => n.Name).ToHashSet(StringComparer.Ordinal);
        var refusal = string.Join(" ", missing.Select(f => CannotRun(f, eligibility[f.Name])).Concat(apart));
        var causes = missing.Select(f => new LaunchCause(
            runners.Contains(f.Node) ? LaunchCauses.NoAdmissibleRunner : LaunchCauses.NoEligibleReplica,
            CannotRun(f, eligibility[f.Name]), f.Node, f.Name))
            .Concat(apart.Select(edge => new LaunchCause(LaunchCauses.ProtectionMismatch, edge))).ToList();
        return (refusal, causes, judged);
    }

    /// <summary>The refusal of one fragment, its node the subject of the sentence: the node is what is missing, never the runner that tried.</summary>
    private static string CannotRun(PlannedFragment fragment, FragmentEligibility eligibility)
    {
        var why = (eligibility.Reason ?? "none of its replicas is eligible").Trim();
        return $"Node '{fragment.Node}' cannot run {fragment.Name}: {(why.EndsWith('.') ? why : why + ".")}";
    }
}
