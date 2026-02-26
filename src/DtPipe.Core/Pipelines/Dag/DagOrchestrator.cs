using System.Threading.Channels;
using Apache.Arrow;
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
    private readonly List<IXStreamerFactory> _xstreamerFactories;

    public Action<string>? OnLogEvent { get; set; }

    public DagOrchestrator(
        ILogger<DagOrchestrator> logger,
        IMemoryChannelRegistry channelRegistry,
        IEnumerable<IXStreamerFactory> xstreamerFactories)
    {
        _logger = logger;
        _channelRegistry = channelRegistry;
        _xstreamerFactories = xstreamerFactories.ToList();
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

                tasks.Add(ExecuteBranchAsync(dag, branch, branchExecutor, effectiveCt));
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

    private async Task<int> ExecuteBranchAsync(JobDagDefinition dag, BranchDefinition branch, Func<string[], CancellationToken, Task<int>> branchExecutor, CancellationToken ct)
    {
        // CRITICAL: Ensure we yield immediately to allow the orchestration loop to start other branches
        await Task.Yield();
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
            _channelRegistry.RegisterChannel(branch.Alias, channel, System.Array.Empty<PipeColumnInfo>());

            // Inject memory channel output if the branch doesn't explicitly define one
            var argsList = branch.Arguments.ToList();
            if (!argsList.Contains("-o", StringComparer.OrdinalIgnoreCase) && !argsList.Contains("--output", StringComparer.OrdinalIgnoreCase))
            {
                var mode = GetRequiredChannelMode(dag, branch.Alias);

                if (mode == XStreamerChannelMode.Arrow)
                {
                    var arrowChannel = Channel.CreateUnbounded<Apache.Arrow.RecordBatch>();
                    _channelRegistry.RegisterArrowChannel(branch.Alias, arrowChannel, new Schema(System.Array.Empty<Field>(), null));
                    argsList.Add("-o");
                    argsList.Add($"arrow-memory:{branch.Alias}");
                    _logger.LogInformation("Branch '{Alias}' → Arrow memory channel (DuckXStreamer consumer).", branch.Alias);
                    OnLogEvent?.Invoke($"  [grey]↳ Branch '{branch.Alias}' → [italic]Arrow memory channel[/] (DuckXStreamer)[/]");
                }
                else
                {
                    // Comportement actuel inchangé — Intermediate DAG buffer
                    _channelRegistry.RegisterChannel(branch.Alias, Channel.CreateUnbounded<IReadOnlyList<object?[]>>(), []);
                    argsList.Add("-o");
                    argsList.Add($"memory:{branch.Alias}");
                    _logger.LogInformation("Branch '{Alias}' → Native memory channel (intermediate DAG buffer).", branch.Alias);
                    OnLogEvent?.Invoke($"  [grey]↳ Branch '{branch.Alias}' → [italic]memory channel[/] (intermediate DAG buffer)[/]");
                }

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

            // Also check Arrow channels
            var arrowChannel = _channelRegistry.GetArrowChannel(branch.Alias);
            arrowChannel?.Channel.Writer.TryComplete(ex);

            throw;
        }
    }

    private XStreamerChannelMode GetRequiredChannelMode(JobDagDefinition dag, string alias)
    {
        foreach (var branch in dag.Branches.Where(b => b.IsXStreamer))
        {
            bool isConsumer =
                ExtractArgValue(branch.Arguments, "--main") == alias ||
                ExtractAllArgValues(branch.Arguments, "--ref").Contains(alias);

            if (!isConsumer) continue;

            // Identifier la factory via -x <componentName> or --xstreamer <componentName>
            var xFlag = ExtractArgValue(branch.Arguments, "-x") ?? ExtractArgValue(branch.Arguments, "--xstreamer");
            if (xFlag == null) continue;

            var factory = _xstreamerFactories
                .FirstOrDefault(f => f.ComponentName.Equals(xFlag, StringComparison.OrdinalIgnoreCase));

            if (factory != null)
                return factory.ChannelMode;
        }

        return XStreamerChannelMode.Native; // défaut si non trouvé
    }

    private string? ExtractArgValue(IEnumerable<string> args, string argName)
    {
        var list = args.ToList();
        int idx = list.FindIndex(a => a.Equals(argName, StringComparison.OrdinalIgnoreCase));
        if (idx >= 0 && idx + 1 < list.Count)
        {
            return list[idx + 1];
        }
        return null;
    }

    private IEnumerable<string> ExtractAllArgValues(IEnumerable<string> args, string argName)
    {
        var values = new List<string>();
        var list = args.ToList();
        for (int i = 0; i < list.Count - 1; i++)
        {
            if (list[i].Equals(argName, StringComparison.OrdinalIgnoreCase))
            {
                var val = list[i + 1];
                if (val.Contains(','))
                {
                    values.AddRange(val.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()));
                }
                else
                {
                    values.Add(val);
                }
            }
        }
        return values;
    }
}
