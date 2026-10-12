namespace DtPipe.Coordinator.Server;

/// <summary>
/// The flow matrix's word on the edges of a plan, asked once through <see cref="IPlanRegistry"/>: which edges it allows and,
/// when it refuses any, the sentence that names them. The planner and a deployment both ask here, so a plan is judged the
/// same way when it is made and when it is deployed against the matrix of that day.
/// </summary>
internal static class PlanFlow
{
    /// <summary>
    /// <paramref name="edges"/> with <see cref="PlannedEdge.Allowed"/> set from the matrix, and the refusal when any edge is
    /// not allowed (<c>null</c> when all are).
    /// </summary>
    public static (IReadOnlyList<PlannedEdge> Edges, string? Rejection) Judge(IPlanRegistry registry, string planId, IReadOnlyList<PlannedEdge> edges)
    {
        var asked = edges.Select(e => e.ToFlowEdge()).ToList();
        try
        {
            registry.Register(new FlowPlan(planId, asked));
            return (edges.Select(e => e with { Allowed = true }).ToList(), null);
        }
        catch (PlanRejectedException refused)
        {
            var forbidden = refused.ForbiddenEdges.ToHashSet();
            return (edges.Select((e, i) => e with { Allowed = !forbidden.Contains(asked[i]) }).ToList(), refused.Message);
        }
    }
}
