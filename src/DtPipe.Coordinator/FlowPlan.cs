namespace DtPipe.Coordinator;

/// <summary>An edge a plan declares: a named flow from the groups of its producer to the groups of its consumer.</summary>
public sealed record PlanEdge(string Name, IReadOnlyCollection<string> ProducerGroups, IReadOnlyCollection<string> ConsumerGroups);

/// <summary>The edges a plan wants to run, checked against the flow control matrix before anything opens.</summary>
public sealed record FlowPlan(string Name, IReadOnlyList<PlanEdge> Edges);
