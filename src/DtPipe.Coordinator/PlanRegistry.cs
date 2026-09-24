using TransportR.Interfaces;

namespace DtPipe.Coordinator;

/// <summary>
/// Refused registration of a <see cref="FlowPlan"/>: at least one edge is not permitted by the
/// flow control matrix.
/// </summary>
public sealed class PlanRejectedException : Exception
{
    public IReadOnlyList<PlanEdge> ForbiddenEdges { get; }

    public PlanRejectedException(IReadOnlyList<PlanEdge> forbiddenEdges)
        : base(BuildMessage(forbiddenEdges))
    {
        ForbiddenEdges = forbiddenEdges;
    }

    private static string BuildMessage(IReadOnlyList<PlanEdge> edges) =>
        "Plan rejected, edge(s) not permitted by the flow control matrix: " +
        string.Join(", ", edges.Select(e => $"{e.Name} ({e.ProducerGroup} -> {e.ConsumerGroup})"));
}

/// <summary>Verifies a plan's edges against the flow control matrix before a run can open any of them.</summary>
public interface IPlanRegistry
{
    /// <exception cref="PlanRejectedException">At least one edge is not permitted; names all of them.</exception>
    void Register(FlowPlan plan);
}

public sealed class PlanRegistry : IPlanRegistry
{
    private readonly IFlowControlService _flowControl;

    public PlanRegistry(IFlowControlService flowControl)
    {
        _flowControl = flowControl;
    }

    public void Register(FlowPlan plan)
    {
        var forbidden = plan.Edges
            .Where(edge => !_flowControl.CanSendTo(
                new HashSet<string> { edge.ProducerGroup },
                new HashSet<string> { edge.ConsumerGroup }))
            .ToList();

        if (forbidden.Count > 0)
        {
            throw new PlanRejectedException(forbidden);
        }
    }
}
