namespace DtPipe.Coordinator;

/// <summary>An edge a plan declares: a named flow from a producer group to a consumer group.</summary>
public sealed record PlanEdge(string Name, string ProducerGroup, string ConsumerGroup);

/// <summary>The edges a plan wants to run, checked against the flow control matrix before anything opens.</summary>
public sealed record FlowPlan(string Name, IReadOnlyList<PlanEdge> Edges);
