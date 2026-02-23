using DtPipe.Core.Pipelines.Dag;

namespace DtPipe.Core.Abstractions.Dag;

/// <summary>
/// Responsible for executing a predefined Directed Acyclic Graph (DAG) of pipeline branches.
/// The orchestrator wires up the necessary in-memory channels and manages the concurrent
/// lifecycle of all branches, ensuring graceful cancellation and error propagation.
/// </summary>
public interface IDagOrchestrator
{
    /// <summary>
    /// Executes the specified DAG definition asynchronously.
    /// </summary>
    /// <param name="dag">The parsed DAG definition to execute.</param>
    /// <param name="branchExecutor">A callback to invoke for executing a single branch's arguments.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>The overall process exit code (0 for success, non-zero for failure).</returns>
    Task<int> ExecuteAsync(JobDagDefinition dag, Func<string[], CancellationToken, Task<int>> branchExecutor, CancellationToken cancellationToken = default);

    /// <summary>
    /// Optional delegate to receive real-time execution logs (e.g., for console output).
    /// </summary>
    Action<string>? OnLogEvent { get; set; }
}
