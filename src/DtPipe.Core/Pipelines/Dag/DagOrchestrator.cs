using System.Threading.Channels;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Executes a DAG of pipeline branches by spawning concurrent Tasks for each branch
/// and wiring their memory channels for zero-copy data flow.
/// </summary>
public class DagOrchestrator : IDagOrchestrator
{
    private readonly ILogger<DagOrchestrator> _logger;
    private readonly IMemoryChannelRegistry _channelRegistry;

    public Action<string>? OnLogEvent { get; set; }

    public DagOrchestrator(ILogger<DagOrchestrator> logger, IMemoryChannelRegistry channelRegistry)
    {
        _logger = logger;
        _channelRegistry = channelRegistry;
    }

    public async Task<int> ExecuteAsync(JobDagDefinition dag, Func<string[], CancellationToken, Task<int>> branchExecutor, CancellationToken cancellationToken = default)
    {
        if (dag.Branches.Count == 0)
        {
            _logger.LogWarning("Cannot execute an empty DAG.");
            return 1;
        }

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var effectiveCt = linkedCts.Token;

        var tasks = new List<Task<int>>();

        _logger.LogInformation("Orchestrating DAG execution with {BranchCount} branches.", dag.Branches.Count);
        OnLogEvent?.Invoke($"[bold blue]Orchestrating DAG execution with {dag.Branches.Count} branches...[/]");

        try
        {
            foreach (var branch in dag.Branches)
            {
                // In a real execution environment, each branch would need its own DI scope
                // and a full reconstruction of the `DumpOptions` from `branch.Arguments`.
                // For now, we simulate the orchestration structure.

                tasks.Add(ExecuteBranchAsync(branch, branchExecutor, effectiveCt));
            }

            // Wait for all branches to finish or for any one to fail early.
            while (tasks.Count > 0)
            {
                var finishedTask = await Task.WhenAny(tasks);

                if (finishedTask.IsFaulted)
                {
                    _logger.LogError(finishedTask.Exception, "A branch failed. Cancelling the entire DAG.");
                    await linkedCts.CancelAsync();
                    return 1;
                }

                if (finishedTask.IsCanceled)
                {
                    _logger.LogWarning("A branch was canceled.");
                    await linkedCts.CancelAsync();
                    return 1;
                }

                // If a branch explicitly returns a non-zero exit code (like a validation error)
                var exitCode = await finishedTask;
                if (exitCode != 0)
                {
                    _logger.LogError("Branch returned exit code {ExitCode}. Cancelling DAG.", exitCode);
                    await linkedCts.CancelAsync();
                    return exitCode;
                }

                tasks.Remove(finishedTask);
            }

            _logger.LogInformation("DAG execution completed successfully.");
            OnLogEvent?.Invoke("[bold green]✓ DAG execution completed successfully.[/]");
            return 0;
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("DAG execution was canceled.");
            OnLogEvent?.Invoke("[bold yellow]⚠ DAG execution was canceled.[/]");
            return 1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error during DAG orchestration.");
            OnLogEvent?.Invoke($"[bold red]✖ Unexpected error during DAG orchestration: {ex.Message.Replace("[", "[[").Replace("]", "]]")}[/]");
            return 1;
        }
    }

    private async Task<int> ExecuteBranchAsync(BranchDefinition branch, Func<string[], CancellationToken, Task<int>> branchExecutor, CancellationToken ct)
    {
        _logger.LogInformation("Starting branch '{Alias}' [IsXStreamer={IsXStreamer}]", branch.Alias, branch.IsXStreamer);
        string role = branch.IsXStreamer ? "[magenta]XStreamer[/]" : "[blue]Linear Branch[/]";
        OnLogEvent?.Invoke($"  [grey]>[/] Starting {role} '{branch.Alias}'");

        try
        {
            // Here, we would intercept the branch's output destination.
            // If it doesn't specify an explicit output (e.g., `-o file.csv`),
            // the Orchestrator forces it into a MemoryChannel.

            // Channel Configuration
            var channel = Channel.CreateBounded<IReadOnlyList<object?[]>>(new BoundedChannelOptions(100)
            {
                SingleWriter = true,
                SingleReader = true, // XStreamers usually join 1-to-1 or N-to-N, but each source is read once.
                FullMode = BoundedChannelFullMode.Wait
            });

            // Register it so downstream branches can find it
            // Null for columns for now until we properly extract schema detection logic
            _channelRegistry.RegisterChannel(branch.Alias, channel, Array.Empty<PipeColumnInfo>());

            // Inject memory channel output if the branch doesn't explicitly define one
            var argsList = branch.Arguments.ToList();
            if (!argsList.Contains("-o", StringComparer.OrdinalIgnoreCase) && !argsList.Contains("--output", StringComparer.OrdinalIgnoreCase))
            {
                argsList.Add("-o");
                argsList.Add($"memory:{branch.Alias}");

                // Disable stats for internal memory-bound branches to prevent Spectre.Console concurrency deadlocks
                if (!argsList.Contains("--no-stats", StringComparer.OrdinalIgnoreCase))
                {
                    argsList.Add("--no-stats");
                }
            }

            // Execute the actual pipeline dynamically using the provided callback
            var branchCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

            try
            {
                int exitCode = await branchExecutor(argsList.ToArray(), branchCts.Token);

                if (exitCode != 0)
                {
                    _logger.LogError("Branch '{Alias}' failed with exit code {ExitCode}.", branch.Alias, exitCode);
                    OnLogEvent?.Invoke($"  [red]✖[/] Branch '{branch.Alias}' failed with exit code {exitCode}.");
                    channel.Writer.TryComplete(new Exception($"Branch failed with exit code {exitCode}"));
                    return exitCode;
                }

                // Successfully finished processing
                channel.Writer.TryComplete();
                _logger.LogInformation("Branch '{Alias}' completed.", branch.Alias);
                OnLogEvent?.Invoke($"  [green]✓[/] Branch '{branch.Alias}' completed.");
                return 0;
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Branch '{Alias}' encountered a fatal error.", branch.Alias);

            // Mark the channel as broken so downstream consumers don't hang
            var storedChannel = _channelRegistry.GetChannel(branch.Alias);
            storedChannel?.Channel.Writer.TryComplete(ex);

            throw;
        }
    }
}
