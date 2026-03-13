using System.Threading.Channels;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Abstractions.Dag;
using Microsoft.Extensions.Logging;

namespace DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Executes a DAG of pipeline branches by spawning concurrent Tasks for each branch
/// and wiring their memory channels for zero-copy data flow.
///
/// Fan-out branches (declared via <c>--from &lt;alias&gt;</c>) are supported through a Broadcast
/// Multiplexer: if N branches consume the same upstream alias, the orchestrator creates N
/// distinct sub-channels and a background <c>BroadcastTask</c> that reads the source channel
/// once and clones each message into all N sub-channels. This is analogous to Unix <c>tee</c>.
/// </summary>
public class DagOrchestrator : IDagOrchestrator
{
    private readonly ILogger<DagOrchestrator> _logger;
    private readonly IMemoryChannelRegistry _channelRegistry;
    private readonly List<IProcessorFactory> _processorFactories;
    private readonly List<IStreamReaderFactory> _readerFactories;
    private static readonly object _schemaLock = new();
    private static readonly Schema _emptySchema = new Schema(System.Array.Empty<Field>(), null);
    
    // Constants for DAG execution
    private const int DefaultNativeChannelCapacity = 100;
    private const int DefaultArrowChannelCapacity = 64;
    private const int TaskLaunchDelayMs = 50;

    public Action<string>? OnLogEvent { get; set; }

    public DagOrchestrator(
        ILogger<DagOrchestrator> logger,
        IMemoryChannelRegistry channelRegistry,
        IEnumerable<IProcessorFactory> processorFactories,
        IEnumerable<IStreamReaderFactory> readerFactories)
    {
        _logger = logger;
        _channelRegistry = channelRegistry;
        _processorFactories = processorFactories.ToList();
        _readerFactories = readerFactories.ToList();
    }

    private sealed record BranchTaskMetadata(
        string Id,
        List<string> Consumes,
        List<string> Produces,
        CancellationTokenSource Cts,
        bool IsTerminal
    );

    public async Task<int> ExecuteAsync(JobDagDefinition dag, Func<BranchDefinition, CancellationToken, Task<int>> branchExecutor, CancellationToken cancellationToken = default)
    {
        if (dag.Branches.Count == 0)
        {
            _logger.LogWarning("Cannot execute an empty DAG.");
            return 1;
        }

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var effectiveCt = linkedCts.Token;

        // ──────────────────────────────────────────────────────────────────
        // 1. Dependency Analysis & Lifecycle Tracking
        // ──────────────────────────────────────────────────────────────────
        
        var branchCts = new Dictionary<string, CancellationTokenSource>(StringComparer.OrdinalIgnoreCase);
        var activeConsumerCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var producerToAlias = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var taskToMetadata = new Dictionary<Task, BranchTaskMetadata>();
        
        // Tracking broadcast tasks separately as they are also producers/consumers
        var broadcastSubAliases = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var fromConsumerCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var branch in dag.Branches)
        {
            var inputs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (!string.IsNullOrEmpty(branch.FromAlias)) inputs.Add(branch.FromAlias);
            if (!string.IsNullOrEmpty(branch.MainAlias)) inputs.Add(branch.MainAlias);
            foreach (var r in branch.RefAliases) inputs.Add(r);

            foreach (var input in inputs)
            {
                fromConsumerCounts[input] = fromConsumerCounts.GetValueOrDefault(input) + 1;
            }
        }

        foreach (var (alias, count) in fromConsumerCounts.Where(kv => kv.Value > 1))
        {
            var subs = new List<string>(count);
            for (int i = 0; i < count; i++)
                subs.Add($"{alias}__fan_{i}");
            broadcastSubAliases[alias] = subs;
        }

        // Initialize active consumer counts for all registry channels
        foreach (var branch in dag.Branches)
        {
            var inputs = new List<string>();
            if (!string.IsNullOrEmpty(branch.FromAlias)) inputs.Add(branch.FromAlias);
            if (!string.IsNullOrEmpty(branch.MainAlias)) inputs.Add(branch.MainAlias);
            foreach (var r in branch.RefAliases) inputs.Add(r);

            foreach (var input in inputs)
            {
                activeConsumerCounts[input] = activeConsumerCounts.GetValueOrDefault(input) + 1;
            }
        }

        // ──────────────────────────────────────────────────────────────────
        // 2. Launching Tasks
        // ──────────────────────────────────────────────────────────────────

        var tasks = new List<Task<int>>();
        var broadcastAssignmentCursor = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var alias in broadcastSubAliases.Keys)
            broadcastAssignmentCursor[alias] = 0;

        _logger.LogInformation("Orchestrating DAG execution with {BranchCount} branches.", dag.Branches.Count);
        OnLogEvent?.Invoke($"[bold blue]Orchestrating DAG execution with {dag.Branches.Count} branches...[/]");

        try
        {
            // 2.a Pre-register all output channels and fan-out buffers
            foreach (var branch in dag.Branches)
            {
                if (string.IsNullOrEmpty(branch.Output))
                {
                    var mode = GetRequiredChannelMode(dag, branch.Alias);
                    if (mode == ChannelMode.Arrow)
                    {
                        var arrowChannel = Channel.CreateBounded<Apache.Arrow.RecordBatch>(new BoundedChannelOptions(DefaultArrowChannelCapacity) { FullMode = BoundedChannelFullMode.Wait });
                        _channelRegistry.RegisterArrowChannel(branch.Alias, arrowChannel, _emptySchema);
                    }
                    else
                    {
                        var channel = Channel.CreateBounded<IReadOnlyList<object?[]>>(new BoundedChannelOptions(DefaultNativeChannelCapacity) { SingleWriter = true, SingleReader = true, FullMode = BoundedChannelFullMode.Wait });
                        _channelRegistry.RegisterChannel(branch.Alias, channel, System.Array.Empty<PipeColumnInfo>());
                    }
                }
            }

            foreach (var (sourceAlias, subs) in broadcastSubAliases)
            {
                var mode = GetRequiredChannelMode(dag, sourceAlias);
                foreach (var subAlias in subs)
                {
                    if (mode == ChannelMode.Arrow)
                    {
                        var sub = Channel.CreateBounded<Apache.Arrow.RecordBatch>(new BoundedChannelOptions(DefaultArrowChannelCapacity) { SingleWriter = true, SingleReader = true, FullMode = BoundedChannelFullMode.Wait });
                        _channelRegistry.RegisterArrowChannel(subAlias, sub, _emptySchema);
                    }
                    else
                    {
                        var sub = Channel.CreateBounded<IReadOnlyList<object?[]>>(new BoundedChannelOptions(DefaultNativeChannelCapacity) { SingleWriter = true, SingleReader = true, FullMode = BoundedChannelFullMode.Wait });
                        _channelRegistry.RegisterChannel(subAlias, sub, System.Array.Empty<PipeColumnInfo>());
                    }
                }
            }

            // Start all branches
            foreach (var branch in dag.Branches)
            {
                string? resolvedFromAlias = null;
                if (!string.IsNullOrEmpty(branch.FromAlias))
                {
                    resolvedFromAlias = ResolveInputAlias(branch.FromAlias, broadcastSubAliases, broadcastAssignmentCursor);
                }

                // Also need to resolve MainAlias and RefAliases for the branch itself
                var actualMainAlias = branch.MainAlias != null 
                    ? ResolveInputAlias(branch.MainAlias, broadcastSubAliases, broadcastAssignmentCursor) 
                    : null;
                
                var actualRefAliases = branch.RefAliases
                    .Select(a => ResolveInputAlias(a, broadcastSubAliases, broadcastAssignmentCursor))
                    .ToArray();

                // If they were resolved to something else (fan-out), we MUST update the branch definition 
                // temporarily or pass them to ExecuteBranchAsync
                var updatedBranch = branch with { 
                    MainAlias = actualMainAlias,
                    RefAliases = actualRefAliases
                };

                var bCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveCt);
                branchCts[branch.Alias] = bCts;

                // Identify producers/consumers for this branch
                var consumes = new List<string>();
                if (!string.IsNullOrEmpty(branch.FromAlias)) consumes.Add(branch.FromAlias);
                if (!string.IsNullOrEmpty(branch.MainAlias)) consumes.Add(branch.MainAlias);
                consumes.AddRange(branch.RefAliases);

                var produces = new List<string>();
                if (string.IsNullOrEmpty(branch.Output)) produces.Add(branch.Alias);

                var metadata = new BranchTaskMetadata(
                    Id: $"branch:{branch.Alias}",
                    Consumes: consumes,
                    Produces: produces,
                    Cts: bCts,
                    IsTerminal: !string.IsNullOrEmpty(branch.Output)
                );

                var task = ExecuteBranchAsync(dag, branch, resolvedFromAlias, broadcastSubAliases, branchExecutor, bCts.Token);
                taskToMetadata[task] = metadata;
                tasks.Add(task);

                await Task.Delay(TaskLaunchDelayMs, effectiveCt);
            }

            // Start Broadcast Tasks
            foreach (var (sourceAlias, subs) in broadcastSubAliases)
            {
                var bCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveCt);
                Task broadcastTask;
                
                if (GetRequiredChannelMode(dag, sourceAlias) == ChannelMode.Arrow)
                    broadcastTask = StartArrowBroadcastAsync(sourceAlias, subs, bCts.Token);
                else
                    broadcastTask = StartNativeBroadcastAsync(sourceAlias, subs, bCts.Token);

                // Broadcast task consumes sourceAlias and produces all sub-aliases
                var metadata = new BranchTaskMetadata(
                    Id: $"broadcast:{sourceAlias}",
                    Consumes: new List<string> { sourceAlias },
                    Produces: new List<string>(subs),
                    Cts: bCts,
                    IsTerminal: false
                );

                // For simplicity, we want broadcast tasks to be in the same 'tasks' list.
                // We wrap it to match the Func<Task<int>> signature.
                var wrappedTask = broadcastTask.ContinueWith(t => {
                    if (t.IsFaulted) throw t.Exception!;
                    return 0; 
                }, effectiveCt);

                taskToMetadata[wrappedTask] = metadata;
                tasks.Add(wrappedTask);

                // Also, broadcast produces sub-aliases, and branches consume them.
                // We need to initialize consumer counts for sub-aliases.
                foreach (var sub in subs)
                {
                    // Each sub-alias is produced by the broadcast task
                    // and consumed by exactly 1 branch (as resolved during branch start)
                    activeConsumerCounts[sub] = 1;
                }
            }

            // ──────────────────────────────────────────────────────────────────
            // 3. Monitor Loop with Selective Termination
            // ──────────────────────────────────────────────────────────────────

            _logger.LogInformation("All tasks started, waiting for completion...");
            while (tasks.Count > 0)
            {
                var finishedTask = await Task.WhenAny(tasks);
                tasks.Remove(finishedTask);

                if (finishedTask.IsFaulted)
                {
                    _logger.LogError(finishedTask.Exception, "A task failed. Cancelling the entire DAG.");
                    await linkedCts.CancelAsync();
                    return 1;
                }

                if (finishedTask.IsCanceled)
                {
                    // Regular cancellation is handled by catching OperationCanceledException
                    // but we should check if it was an unexpected cancellation.
                    continue;
                }

                var exitCode = await finishedTask;
                if (exitCode != 0)
                {
                    _logger.LogError("Task {Id} returned exit code {ExitCode}. Cancelling DAG.", taskToMetadata[finishedTask].Id, exitCode);
                    await linkedCts.CancelAsync();
                    return exitCode;
                }

                // Normal completion: Update consumer counts and terminate orphaned producers
                var finishedMeta = taskToMetadata[finishedTask];
                _logger.LogDebug("Task {Id} finished. Updating dependencies...", finishedMeta.Id);

                foreach (var inputAlias in finishedMeta.Consumes)
                {
                    if (activeConsumerCounts.ContainsKey(inputAlias))
                    {
                        activeConsumerCounts[inputAlias]--;
                        if (activeConsumerCounts[inputAlias] <= 0)
                        {
                            _logger.LogInformation("Alias '{Alias}' has no more consumers. Triggering producer termination check.", inputAlias);
                            
                            // Find the producer for this alias and check if it can stop
                            foreach (var meta in taskToMetadata.Values)
                            {
                                if (meta.Produces.Contains(inputAlias, StringComparer.OrdinalIgnoreCase))
                                {
                                    // Check if ALL aliases produced by this producer are now orphaned
                                    bool allOrphaned = meta.Produces.All(p => activeConsumerCounts.GetValueOrDefault(p) <= 0);
                                    if (allOrphaned && !meta.IsTerminal)
                                    {
                                        _logger.LogInformation("Producer {Id} is now orphaned (all produced aliases [{Aliases}] have no consumers). Cancelling.", 
                                            meta.Id, string.Join(", ", meta.Produces));
                                        await meta.Cts.CancelAsync();
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
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


    private async Task<int> ExecuteBranchAsync(
        JobDagDefinition dag,
        BranchDefinition branch,
        string? resolvedFromAlias,
        Dictionary<string, List<string>> broadcastSubAliases,
        Func<BranchDefinition, CancellationToken, Task<int>> branchExecutor,
        CancellationToken ct)
    {
        await Task.Yield();
        _logger.LogInformation("Starting branch '{Alias}' [IsProcessor={IsProcessor}, FromAlias={FromAlias}]",
            branch.Alias, branch.IsProcessor, branch.FromAlias ?? "(none)");

        string role = branch.IsProcessor ? "[magenta]Processor[/]"
                    : !string.IsNullOrEmpty(branch.FromAlias) ? "[cyan]Fan-out (tee)[/]"
                    : "[blue]Linear Branch[/]";
        OnLogEvent?.Invoke($"  [grey]>[/] Starting {role} '{branch.Alias}'");

        try
        {
            var argsList = branch.Arguments.ToList();

            if (branch.Processor == ProcessorKind.Sql)
            {
                // Ensure --sql is in args with the correct engine if it was implicit
                var sqlEngine = ExtractArgValue(argsList, "--sql");
                if (string.IsNullOrEmpty(sqlEngine) && !string.IsNullOrEmpty(branch.Input))
                {
                    // If --sql was present without value or implicitly mapped from Input (provider name)
                    // We need to make sure the args reflect the processor choice.
                    // For now, if Input is provided (e.g. "fusion-engine"), we inject it.
                    int sqlIdx = argsList.FindIndex(a => a.Equals("--sql", StringComparison.OrdinalIgnoreCase));
                    if (sqlIdx >= 0 && sqlIdx + 1 < argsList.Count && argsList[sqlIdx+1].StartsWith("-"))
                    {
                         argsList.Insert(sqlIdx + 1, branch.Input);
                    }
                    else if (sqlIdx < 0)
                    {
                         argsList.Insert(0, branch.Input);
                         argsList.Insert(0, "--sql");
                    }
                }
            }

            if (branch.Processor == ProcessorKind.Sql)
            {
                // Ensure --sql is in args with the correct engine if it was implicit
                var sqlEngine = ExtractArgValue(argsList, "--sql");
                if (string.IsNullOrEmpty(sqlEngine) && !string.IsNullOrEmpty(branch.Input))
                {
                    // If --sql was present without value or implicitly mapped from Input (provider name)
                    // We need to make sure the args reflect the processor choice.
                    // For now, if Input is provided (e.g. "fusion-engine"), we inject it.
                    int sqlIdx = argsList.FindIndex(a => a.Equals("--sql", StringComparison.OrdinalIgnoreCase));
                    if (sqlIdx >= 0 && sqlIdx + 1 < argsList.Count && argsList[sqlIdx+1].StartsWith("-"))
                    {
                         argsList.Insert(sqlIdx + 1, branch.Input);
                    }
                    else if (sqlIdx < 0)
                    {
                         argsList.Insert(0, branch.Input);
                         argsList.Insert(0, "--sql");
                    }
                }
            }

            if (!branch.IsProcessor && string.IsNullOrEmpty(branch.Input))
            {
                if (!string.IsNullOrEmpty(resolvedFromAlias))
                {
                    var prefix = GetRequiredChannelMode(dag, branch.FromAlias!) == ChannelMode.Arrow
                        ? "arrow-memory"
                        : "mem";
                    argsList.Insert(0, $"{prefix}:{resolvedFromAlias}");
                    argsList.Insert(0, "-i");
                }
                else if (!string.IsNullOrEmpty(branch.MainAlias))
                {
                    // Check if MainAlias is a branch alias or a provider
                    var isBranchAlias = dag.Branches.Any(b => b.Alias.Equals(branch.MainAlias, StringComparison.OrdinalIgnoreCase));
                    if (isBranchAlias)
                    {
                        var prefix = GetRequiredChannelMode(dag, branch.MainAlias) == ChannelMode.Arrow
                            ? "arrow-memory"
                            : "mem";
                        argsList.Insert(0, $"{prefix}:{branch.MainAlias}");
                        argsList.Insert(0, "-i");
                    }
                    else
                    {
                        // It's likely a provider name (e.g. generate:100)
                        argsList.Insert(0, branch.MainAlias);
                        argsList.Insert(0, "-i");
                    }
                }
            }

            if (string.IsNullOrEmpty(branch.Output))
            {
                var mode = GetRequiredChannelMode(dag, branch.Alias);
                argsList.Add("-o");
                argsList.Add($"{(mode == ChannelMode.Arrow ? "arrow-memory" : "mem")}:{branch.Alias}");
                OnLogEvent?.Invoke($"  [grey]↳ Branch '{branch.Alias}' → [italic]{(mode == ChannelMode.Arrow ? "Arrow memory channel" : "memory channel")}[/][/]");

                if (!argsList.Contains("--no-stats", StringComparer.OrdinalIgnoreCase))
                {
                    argsList.Add("--no-stats");
                }
            }

            try
            {
                var updatedBranch = branch with { Arguments = argsList.ToArray() };
                int exitCode = await branchExecutor(updatedBranch, ct);

                if (exitCode != 0)
                {
                    _logger.LogError("Branch '{Alias}' failed with exit code {ExitCode}.", branch.Alias, exitCode);
                    OnLogEvent?.Invoke($"  [red]✖[/] Branch '{branch.Alias}' failed with exit code {exitCode}.");
                    return exitCode;
                }

                _channelRegistry.GetChannel(branch.Alias)?.Channel.Writer.TryComplete();
                _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete();

                _logger.LogInformation("Branch '{Alias}' completed.", branch.Alias);
                OnLogEvent?.Invoke($"  [green]✓[/] Branch '{branch.Alias}' completed.");
                return 0;
            }
            catch (OperationCanceledException)
            {
                // Graceful termination when all consumers are done
                _channelRegistry.GetChannel(branch.Alias)?.Channel.Writer.TryComplete();
                _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete();
                _logger.LogInformation("Branch '{Alias}' terminated due to cancellation (orphaned producer).", branch.Alias);
                OnLogEvent?.Invoke($"  [grey]✓[/] Branch '{branch.Alias}' stopped (no more consumers).");
                return 0;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Branch '{Alias}' encountered a fatal error.", branch.Alias);
            _channelRegistry.GetChannel(branch.Alias)?.Channel.Writer.TryComplete(ex);
            _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete(ex);
            throw;
        }
    }

    private Task StartNativeBroadcastAsync(string sourceAlias, IReadOnlyList<string> subAliases, CancellationToken ct)
    {
        var sourceTuple = _channelRegistry.GetChannel(sourceAlias)
            ?? throw new InvalidOperationException($"Broadcast: source channel '{sourceAlias}' not found in registry.");

        var subChannels = subAliases
            .Select(subAlias => _channelRegistry.GetChannel(subAlias)?.Channel ?? throw new InvalidOperationException($"Broadcast: sub channel '{subAlias}' not found."))
            .ToList();

        _logger.LogInformation("Starting broadcast multiplexer for '{SourceAlias}' → [{Subs}]",
            sourceAlias, string.Join(", ", subAliases));

        return Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in sourceTuple.Channel.Reader.ReadAllAsync(ct))
                {
                    foreach (var sub in subChannels)
                    {
                        await sub.Writer.WriteAsync(batch, ct);
                    }
                }
            }
            finally
            {
                foreach (var sub in subChannels)
                    sub.Writer.TryComplete();
                _logger.LogInformation("Broadcast multiplexer for '{SourceAlias}' completed.", sourceAlias);
            }
        }, ct);
    }

    private Task StartArrowBroadcastAsync(string sourceAlias, IReadOnlyList<string> subAliases, CancellationToken ct)
    {
        var sourceTuple = _channelRegistry.GetArrowChannel(sourceAlias)
            ?? throw new InvalidOperationException($"Arrow Broadcast: source channel '{sourceAlias}' not found in registry.");

        var subChannels = subAliases
            .Select(subAlias => _channelRegistry.GetArrowChannel(subAlias)?.Channel ?? throw new InvalidOperationException($"Arrow Broadcast: sub channel '{subAlias}' not found."))
            .ToList();

        _logger.LogInformation("Starting Arrow broadcast multiplexer for '{SourceAlias}' → [{Subs}]",
            sourceAlias, string.Join(", ", subAliases));

        return Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in sourceTuple.Channel.Reader.ReadAllAsync(ct))
                {
                    foreach (var sub in subChannels)
                    {
                        await sub.Writer.WriteAsync(batch.Clone(), ct);
                    }
                    // We must NOT Dispose(batch) here because Arrow .NET clones share the SAME Column objects.
                    // Disposing the original would invalidate all clones being processed in parallel branches.
                    // The native memory will be freed when the final clones are disposed in their respective branches.
                }
            }
            finally
            {
                foreach (var sub in subChannels)
                    sub.Writer.TryComplete();
                _logger.LogInformation("Arrow Broadcast multiplexer for '{SourceAlias}' completed.", sourceAlias);
            }
        }, ct);
    }

    private ChannelMode GetRequiredChannelMode(JobDagDefinition dag, string alias)
    {
        foreach (var branch in dag.Branches.Where(b => b.IsProcessor))
        {
            bool isConsumer = (branch.MainAlias != null && branch.MainAlias.Equals(alias, StringComparison.OrdinalIgnoreCase)) ||
                              branch.RefAliases.Contains(alias, StringComparer.OrdinalIgnoreCase);

            if (!isConsumer) continue;

            var xFlag = branch.Input;
            if (xFlag == null) continue;

            var factory = _processorFactories
                .FirstOrDefault(f => f.ComponentName.Equals(xFlag, StringComparison.OrdinalIgnoreCase));

            if (factory != null)
                return factory.ChannelMode;
        }

        var producerBranch = dag.Branches.FirstOrDefault(b =>
            b.Alias.Equals(alias, StringComparison.OrdinalIgnoreCase) && !b.IsProcessor);

        if (producerBranch != null && !string.IsNullOrEmpty(producerBranch.Input))
        {
            var readerFactory = _readerFactories.FirstOrDefault(f =>
                producerBranch.Input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase) ||
                producerBranch.Input.Equals(f.ComponentName, StringComparison.OrdinalIgnoreCase) ||
                f.CanHandle(producerBranch.Input));

            if (readerFactory?.YieldsColumnarOutput == true)
                return ChannelMode.Arrow;
        }

        return ChannelMode.Native;
    }

    private string ResolveInputAlias(string alias, Dictionary<string, List<string>> broadcastSubAliases, Dictionary<string, int> broadcastAssignmentCursor)
    {
        if (broadcastSubAliases.TryGetValue(alias, out var subs))
        {
            var idx = broadcastAssignmentCursor[alias];
            var resolved = subs[idx];
            broadcastAssignmentCursor[alias] = idx + 1;
            return resolved;
        }
        return alias;
    }

    private string? ExtractArgValue(IEnumerable<string> args, string argName)
    {
        var list = args.ToList();
        int idx = list.FindIndex(a => a.Equals(argName, StringComparison.OrdinalIgnoreCase));
        if (idx >= 0 && idx + 1 < list.Count)
        {
            var val = list[idx + 1];
            if (val.StartsWith('-')) return null;
            return val;
        }
        return null;
    }
}
