using System.Threading.Channels;
using Apache.Arrow;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Infrastructure.Arrow;
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
///
/// Stream-transformer branches (<c>--sql</c> / <c>--merge</c>) read from Arrow channels directly
/// via their <see cref="IStreamTransformerFactory"/>. The orchestrator does NOT inject <c>-i</c>
/// for these branches; instead it registers their upstream channels as Arrow channels.
/// </summary>
public class DagOrchestrator : IDagOrchestrator
{
    private readonly ILogger<DagOrchestrator> _logger;
    private readonly IMemoryChannelRegistry _channelRegistry;
    private static readonly Schema _emptySchema = new Schema(System.Array.Empty<Field>(), null);

    // Constants for DAG execution
    private const int DefaultArrowChannelCapacity = 8;

    public Action<DagLogEvent>? OnLogEvent { get; set; }

    public DagOrchestrator(
        ILogger<DagOrchestrator> logger,
        IMemoryChannelRegistry channelRegistry)
    {
        _logger = logger;
        _channelRegistry = channelRegistry;
    }

    private sealed record BranchTaskMetadata(
        string Id,
        List<string> Consumes,
        List<string> Produces,
        CancellationTokenSource Cts,
        bool IsTerminal
    );

    public async Task<int> ExecuteAsync(JobDagDefinition dag, Func<BranchDefinition, BranchChannelContext, CancellationToken, Task<int>> branchExecutor, CancellationToken cancellationToken = default)
    {
        if (dag.Branches.Count == 0)
        {
            _logger.LogWarning("Cannot execute an empty DAG.");
            return 1;
        }

        // A run starts on an empty registry. Clearing it is safe only because a process runs one run at a
        // time (JobExecutor): a second run beside this one would clear its channels and trade its rows.
        _channelRegistry.Clear();

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var effectiveCt = linkedCts.Token;

        // ──────────────────────────────────────────────────────────────────
        // 1. Dependency Analysis & Lifecycle Tracking
        // ──────────────────────────────────────────────────────────────────

        var branchCts = new Dictionary<string, CancellationTokenSource>(StringComparer.OrdinalIgnoreCase);
        var activeConsumerCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var taskToMetadata = new Dictionary<Task, BranchTaskMetadata>();

        // Tracking broadcast tasks separately as they are also producers/consumers
        var broadcastSubAliases = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var fromConsumerCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var branch in dag.Branches)
        {
            var inputs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var a in branch.StreamingAliases) inputs.Add(a);
            foreach (var r in branch.RefAliases) inputs.Add(r);

            foreach (var input in inputs)
                fromConsumerCounts[input] = fromConsumerCounts.GetValueOrDefault(input) + 1;
        }

        foreach (var (alias, count) in fromConsumerCounts.Where(kv => kv.Value > 1))
        {
            var subs = new List<string>(count);
            for (int i = 0; i < count; i++)
                subs.Add($"{alias}{IChannelNaming.FanPrefix}{i}");
            broadcastSubAliases[alias] = subs;
        }

        // Initialize active consumer counts for all registry channels
        foreach (var branch in dag.Branches)
        {
            var inputs = new List<string>();
            foreach (var a in branch.StreamingAliases) inputs.Add(a);
            foreach (var r in branch.RefAliases) inputs.Add(r);

            foreach (var input in inputs)
                activeConsumerCounts[input] = activeConsumerCounts.GetValueOrDefault(input) + 1;
        }

        // ──────────────────────────────────────────────────────────────────
        // 2. Launching Tasks
        // ──────────────────────────────────────────────────────────────────

        var tasks = new List<Task<int>>();
        // Every task the run started, waited for at the exit whatever the verdict.
        var started = new List<Task>();
        var broadcastAssignmentCursor = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var alias in broadcastSubAliases.Keys)
            broadcastAssignmentCursor[alias] = 0;

        _logger.LogInformation("Orchestrating DAG execution with {BranchCount} branches.", dag.Branches.Count);

        try
        {
            // 2.a Pre-register all output channels and fan-out buffers
            foreach (var branch in dag.Branches)
            {
                if (string.IsNullOrEmpty(branch.Output))
                {
                    var arrowChannel = Channel.CreateBounded<Apache.Arrow.RecordBatch>(new BoundedChannelOptions(DefaultArrowChannelCapacity) { FullMode = BoundedChannelFullMode.Wait });
                    _channelRegistry.RegisterArrowChannel(branch.Alias, arrowChannel, _emptySchema);
                }
            }

            foreach (var (sourceAlias, subs) in broadcastSubAliases)
            {
                foreach (var subAlias in subs)
                {
                    var subArrow = Channel.CreateBounded<Apache.Arrow.RecordBatch>(new BoundedChannelOptions(DefaultArrowChannelCapacity) { SingleWriter = true, SingleReader = true, FullMode = BoundedChannelFullMode.Wait });
                    _channelRegistry.RegisterArrowChannel(subAlias, subArrow, _emptySchema);
                }
            }

            // Start all branches
            foreach (var branch in dag.Branches)
            {
                // Resolve streaming/ref aliases to their fan-out sub-aliases when applicable.
                var actualStreamingAliases = branch.StreamingAliases
                    .Select(a => ResolveInputAlias(a, broadcastSubAliases, broadcastAssignmentCursor))
                    .ToArray();

                var actualRefAliases = branch.RefAliases
                    .Select(a => ResolveInputAlias(a, broadcastSubAliases, broadcastAssignmentCursor))
                    .ToArray();

                // Build the logical→physical alias map for this branch.
                // All stream-transformer branches (SQL, merge, …) keep logical aliases in their args;
                // the physical channel alias is resolved at factory time via this map.
                // Fan-out consumer branches have their --from arg rewritten to the physical alias.
                var aliasMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < branch.StreamingAliases.Count && i < actualStreamingAliases.Length; i++)
                    if (!string.Equals(branch.StreamingAliases[i], actualStreamingAliases[i], StringComparison.OrdinalIgnoreCase))
                        aliasMap[branch.StreamingAliases[i]] = actualStreamingAliases[i];
                for (int i = 0; i < branch.RefAliases.Count && i < actualRefAliases.Length; i++)
                    if (!string.Equals(branch.RefAliases[i], actualRefAliases[i], StringComparison.OrdinalIgnoreCase))
                        aliasMap[branch.RefAliases[i]] = actualRefAliases[i];
                var branchCtx = new BranchChannelContext { AliasMap = aliasMap };

                // No argv mutation. Stream-transformer branches keep logical aliases in
                // their args (factories resolve physical channels via AliasMap); fan-out
                // consumers receive the physical sub-channel through their typed endpoint.
                var updatedBranch = branch with { RefAliases = actualRefAliases };

                var bCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveCt);
                branchCts[branch.Alias] = bCts;

                var consumes = new List<string>(branch.StreamingAliases);
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

                var actualFromAlias = actualStreamingAliases.Length > 0 ? actualStreamingAliases[0] : null;
                var task = ExecuteBranchAsync(dag, updatedBranch, actualFromAlias, broadcastSubAliases, branchExecutor, bCts.Token, branchCtx);
                taskToMetadata[task] = metadata;
                tasks.Add(task);
                started.Add(task);
            }

            // Start Broadcast Tasks
            foreach (var (sourceAlias, subs) in broadcastSubAliases)
            {
                var bCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveCt);
                Task broadcastTask = StartArrowBroadcastAsync(sourceAlias, subs, bCts.Token);

                var metadata = new BranchTaskMetadata(
                    Id: $"broadcast:{sourceAlias}",
                    Consumes: new List<string> { sourceAlias },
                    Produces: new List<string>(subs),
                    Cts: bCts,
                    IsTerminal: false
                );

                var wrappedTask = broadcastTask.ContinueWith(t =>
                {
                    if (t.IsFaulted) throw t.Exception!;
                    return 0;
                }, effectiveCt);

                taskToMetadata[wrappedTask] = metadata;
                tasks.Add(wrappedTask);
                // The wrapper is canceled with the run while the multiplexer may still be reading.
                started.Add(broadcastTask);
                started.Add(wrappedTask);

                foreach (var sub in subs)
                    activeConsumerCounts[sub] = 1;
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
                    // Preserve the full causal chain in logs instead of only the outermost message.
                    var fault = finishedTask.Exception!;
                    _logger.LogError(fault, "A task failed. Cancelling the entire DAG.\n{Chain}",
                        Infrastructure.Diagnostics.ExceptionChainFlattener.Format(fault));
                    var innerMsg = fault.GetBaseException().Message;
                    OnLogEvent?.Invoke(new DagLogEvent(DagLogSeverity.Error, innerMsg));
                    await linkedCts.CancelAsync();
                    return 1;
                }

                if (finishedTask.IsCanceled)
                    continue;

                var exitCode = await finishedTask;
                if (exitCode != 0)
                {
                    _logger.LogError("Task {Id} returned exit code {ExitCode}. Cancelling DAG.", taskToMetadata[finishedTask].Id, exitCode);
                    await linkedCts.CancelAsync();
                    return exitCode;
                }

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

                            foreach (var meta in taskToMetadata.Values)
                            {
                                if (meta.Produces.Contains(inputAlias, StringComparer.OrdinalIgnoreCase))
                                {
                                    bool allOrphaned = meta.Produces.All(p => activeConsumerCounts.GetValueOrDefault(p) <= 0);
                                    if (allOrphaned && !meta.IsTerminal)
                                    {
                                        _logger.LogInformation("Producer {Id} is now orphaned. Cancelling.", meta.Id);
                                        await meta.Cts.CancelAsync();
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            // Cancellation that raced with branch completion must not be reported as success:
            // if every branch was orphaned by a global cancel, the run did not complete cleanly.
            if (linkedCts.IsCancellationRequested)
            {
                _logger.LogWarning("DAG canceled during branch completion; returning 1.");
                return 1;
            }

            _logger.LogInformation("DAG execution completed successfully.");
            return 0;
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("DAG execution was canceled.");
            OnLogEvent?.Invoke(new DagLogEvent(DagLogSeverity.Warning, "DAG execution was canceled."));
            return 1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error during DAG orchestration.");
            OnLogEvent?.Invoke(new DagLogEvent(DagLogSeverity.Error, $"Unexpected error during DAG orchestration: {ex.Message}"));
            return 1;
        }
        finally
        {
            // A verdict reached while a branch still runs would let the next run clear the registry under that
            // branch, which completes its channel by alias: it returns only once every task has ended.
            if (started.Exists(t => !t.IsCompleted))
                await linkedCts.CancelAsync();
            await WaitForEndAsync(started);
        }
    }

    /// <summary>
    /// Waits for every task to end, however it ends. The verdict is already given, so a task that fails or is
    /// canceled now does not change it; its exception is observed here instead of surfacing at finalization.
    /// </summary>
    private async Task WaitForEndAsync(IReadOnlyList<Task> started)
    {
        foreach (var task in started)
        {
            try
            {
                await task;
            }
            catch (OperationCanceledException)
            {
                // Cancellation is how the verdict stops the remaining tasks.
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "A task ended with an error after the DAG gave its verdict.");
            }
        }
    }

    private async Task<int> ExecuteBranchAsync(
        JobDagDefinition dag,
        BranchDefinition branch,
        string? resolvedFromAlias,
        Dictionary<string, List<string>> broadcastSubAliases,
        Func<BranchDefinition, BranchChannelContext, CancellationToken, Task<int>> branchExecutor,
        CancellationToken ct,
        BranchChannelContext ctx)
    {
        await Task.Yield();

        _logger.LogInformation("Starting branch '{Alias}' [HasStreamTransformer={HasST}, From={From}]",
            branch.Alias, branch.HasStreamTransformer, branch.StreamingAliases.FirstOrDefault() ?? "(none)");

        try
        {
            // Typed channel endpoints instead of a routing plan the CLI re-interprets.
            InternalChannelEndpoint? inputEndpoint = null;
            if (!branch.HasStreamTransformer && string.IsNullOrEmpty(branch.Input) && !string.IsNullOrEmpty(resolvedFromAlias))
                inputEndpoint = new InternalChannelEndpoint(resolvedFromAlias, InternalChannelKind.Arrow);

            InternalChannelEndpoint? outputEndpoint = null;
            bool suppressStats = false;
            if (string.IsNullOrEmpty(branch.Output))
            {
                outputEndpoint = new InternalChannelEndpoint(branch.Alias, InternalChannelKind.Arrow);
                suppressStats = true;
            }

            var enrichedCtx = ctx with
            {
                InputEndpoint = inputEndpoint,
                OutputEndpoint = outputEndpoint,
                SuppressStats = suppressStats
            };

            try
            {
                int exitCode = await branchExecutor(branch, enrichedCtx, ct);

                if (exitCode != 0)
                {
                    _logger.LogError("Branch '{Alias}' failed with exit code {ExitCode}.", branch.Alias, exitCode);
                    OnLogEvent?.Invoke(new DagLogEvent(DagLogSeverity.Error, $"Branch '{branch.Alias}' failed with exit code {exitCode}.", branch.Alias));
                    return exitCode;
                }

                _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete();

                _logger.LogInformation("Branch '{Alias}' completed.", branch.Alias);
                return 0;
            }
            catch (OperationCanceledException)
            {
                _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete();
                _logger.LogInformation("Branch '{Alias}' terminated due to cancellation (orphaned producer).", branch.Alias);
                return 0;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Branch '{Alias}' encountered a fatal error.", branch.Alias);
            _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete(ex);
            throw;
        }
    }

    private Task StartArrowBroadcastAsync(string sourceAlias, IReadOnlyList<string> subAliases, CancellationToken ct)
    {
        var source = (_channelRegistry.GetArrowChannel(sourceAlias)
            ?? throw new InvalidOperationException($"Arrow Broadcast: source channel '{sourceAlias}' not found in registry.")).Channel.Reader;
        var targets = subAliases
            .Select(s => (_channelRegistry.GetArrowChannel(s)?.Channel ?? throw new InvalidOperationException($"Arrow Broadcast: sub channel '{s}' not found.")).Writer)
            .ToList();
        _logger.LogInformation("Starting broadcast multiplexer for '{SourceAlias}' → [{Subs}]",
            sourceAlias, string.Join(", ", subAliases));
        return BroadcastAsync(source, targets, sourceAlias, _logger, ct);
    }

    /// <summary>
    /// Reads each upstream batch once and hands every consumer its own independently-disposable
    /// view via <see cref="ArrowOwnership.RetainAll"/> (a reference-count bump, not a deep copy),
    /// then disposes the broadcaster's own reference. Each consumer disposes the batch it receives
    /// (.claude/rules/engine-invariants.md › "RecordBatch ownership").
    /// </summary>
    private static Task BroadcastAsync(
        ChannelReader<RecordBatch> source,
        IReadOnlyList<ChannelWriter<RecordBatch>> targets,
        string sourceAlias,
        ILogger logger,
        CancellationToken ct)
        => Task.Run(async () =>
        {
            try
            {
                await foreach (var item in source.ReadAllAsync(ct))
                {
                    foreach (var t in targets)
                        await t.WriteAsync(ArrowOwnership.RetainAll(item), ct);
                    item.Dispose();
                }
            }
            finally
            {
                foreach (var t in targets) t.TryComplete();
                logger.LogInformation("Broadcast multiplexer for '{Alias}' completed.", sourceAlias);
            }
        }, ct);

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
}
