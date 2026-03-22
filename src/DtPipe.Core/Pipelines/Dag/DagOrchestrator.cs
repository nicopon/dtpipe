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
///
/// Stream-transformer branches (<c>--sql</c> / <c>--merge</c>) read from Arrow channels directly
/// via their <see cref="IStreamTransformerFactory"/>. The orchestrator does NOT inject <c>-i</c>
/// for these branches; instead it registers their upstream channels as Arrow channels.
/// </summary>
public class DagOrchestrator : IDagOrchestrator
{
    private readonly ILogger<DagOrchestrator> _logger;
    private readonly IMemoryChannelRegistry _channelRegistry;
    private readonly List<IStreamReaderFactory> _readerFactories;
    private static readonly object _schemaLock = new();
    private static readonly Schema _emptySchema = new Schema(System.Array.Empty<Field>(), null);

    // Constants for DAG execution
    private const int DefaultNativeChannelCapacity = 100;
    private const int DefaultArrowChannelCapacity = 64;

    public Action<string>? OnLogEvent { get; set; }

    public DagOrchestrator(
        ILogger<DagOrchestrator> logger,
        IMemoryChannelRegistry channelRegistry,
        IEnumerable<IStreamReaderFactory> readerFactories)
    {
        _logger = logger;
        _channelRegistry = channelRegistry;
        _readerFactories = readerFactories.ToList();
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
            if (!string.IsNullOrEmpty(branch.FromAlias)) inputs.Add(branch.FromAlias);
            foreach (var r in branch.RefAliases) inputs.Add(r);
            foreach (var m in branch.MergeAliases) inputs.Add(m);

            foreach (var input in inputs)
                fromConsumerCounts[input] = fromConsumerCounts.GetValueOrDefault(input) + 1;
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
            foreach (var r in branch.RefAliases) inputs.Add(r);
            foreach (var m in branch.MergeAliases) inputs.Add(m);

            foreach (var input in inputs)
                activeConsumerCounts[input] = activeConsumerCounts.GetValueOrDefault(input) + 1;
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
                // Resolve from/ref/merge aliases to their fan-out sub-aliases when applicable.
                var actualFromAlias = !string.IsNullOrEmpty(branch.FromAlias)
                    ? ResolveInputAlias(branch.FromAlias, broadcastSubAliases, broadcastAssignmentCursor)
                    : null;

                var actualRefAliases = branch.RefAliases
                    .Select(a => ResolveInputAlias(a, broadcastSubAliases, broadcastAssignmentCursor))
                    .ToArray();

                var actualMergeAliases = branch.MergeAliases
                    .Select(a => ResolveInputAlias(a, broadcastSubAliases, broadcastAssignmentCursor))
                    .ToArray();

                // Build the logical→physical alias map for this branch.
                // SQL branches keep logical aliases in their args (for SQL table names);
                // the physical channel alias is resolved at factory time via this map.
                // Merge/native branches have their args rewritten to physical aliases directly.
                var aliasMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                if (!string.IsNullOrEmpty(branch.FromAlias) && actualFromAlias != null
                    && !string.Equals(branch.FromAlias, actualFromAlias, StringComparison.OrdinalIgnoreCase))
                    aliasMap[branch.FromAlias] = actualFromAlias;
                for (int i = 0; i < branch.RefAliases.Count && i < actualRefAliases.Length; i++)
                    if (!string.Equals(branch.RefAliases[i], actualRefAliases[i], StringComparison.OrdinalIgnoreCase))
                        aliasMap[branch.RefAliases[i]] = actualRefAliases[i];
                for (int i = 0; i < branch.MergeAliases.Count && i < actualMergeAliases.Length; i++)
                    if (!string.Equals(branch.MergeAliases[i], actualMergeAliases[i], StringComparison.OrdinalIgnoreCase))
                        aliasMap[branch.MergeAliases[i]] = actualMergeAliases[i];
                var branchCtx = new BranchChannelContext { AliasMap = aliasMap };

                // For SQL branches: preserve logical aliases in args so the SQL query table names
                // remain correct. Physical channel lookup happens via branchCtx.AliasMap.
                // For merge/native branches: rewrite --from/--ref/--merge to physical aliases.
                string[] resolvedArgs;
                if (branch.SqlQuery != null)
                    resolvedArgs = branch.Arguments;
                else
                    resolvedArgs = ReplaceAliasesInArgs(
                        branch.Arguments,
                        branch.FromAlias, actualFromAlias,
                        branch.RefAliases, actualRefAliases,
                        branch.MergeAliases, actualMergeAliases);

                var updatedBranch = branch with
                {
                    RefAliases = actualRefAliases,
                    MergeAliases = actualMergeAliases,
                    Arguments = resolvedArgs
                };

                var bCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveCt);
                branchCts[branch.Alias] = bCts;

                var consumes = new List<string>();
                if (!string.IsNullOrEmpty(branch.FromAlias)) consumes.Add(branch.FromAlias);
                consumes.AddRange(branch.RefAliases);
                consumes.AddRange(branch.MergeAliases);

                var produces = new List<string>();
                if (string.IsNullOrEmpty(branch.Output)) produces.Add(branch.Alias);

                var metadata = new BranchTaskMetadata(
                    Id: $"branch:{branch.Alias}",
                    Consumes: consumes,
                    Produces: produces,
                    Cts: bCts,
                    IsTerminal: !string.IsNullOrEmpty(branch.Output)
                );

                var task = ExecuteBranchAsync(dag, updatedBranch, actualFromAlias, broadcastSubAliases, branchExecutor, bCts.Token, branchCtx);
                taskToMetadata[task] = metadata;
                tasks.Add(task);
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
                    _logger.LogError(finishedTask.Exception, "A task failed. Cancelling the entire DAG.");
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
        Func<BranchDefinition, BranchChannelContext, CancellationToken, Task<int>> branchExecutor,
        CancellationToken ct,
        BranchChannelContext ctx)
    {
        await Task.Yield();

        string role = branch.HasStreamTransformer ? "[magenta]Stream Transformer[/]"
                    : !string.IsNullOrEmpty(branch.FromAlias) ? "[cyan]Fan-out (tee)[/]"
                    : "[blue]Linear Branch[/]";

        _logger.LogInformation("Starting branch '{Alias}' [HasStreamTransformer={HasST}, FromAlias={FromAlias}]",
            branch.Alias, branch.HasStreamTransformer, branch.FromAlias ?? "(none)");
        OnLogEvent?.Invoke($"  [grey]>[/] Starting {role} '{branch.Alias}'");

        try
        {
            var argsList = branch.Arguments.ToList();

            // For fan-out branches (not stream transformers): inject -i to route input from the channel.
            if (!branch.HasStreamTransformer && string.IsNullOrEmpty(branch.Input) && !string.IsNullOrEmpty(resolvedFromAlias))
            {
                var prefix = GetRequiredChannelMode(dag, branch.FromAlias!) == ChannelMode.Arrow
                    ? "arrow-memory"
                    : "mem";
                argsList.Insert(0, $"{prefix}:{resolvedFromAlias}");
                argsList.Insert(0, "-i");
            }

            if (string.IsNullOrEmpty(branch.Output))
            {
                var mode = GetRequiredChannelMode(dag, branch.Alias);
                argsList.Add("-o");
                argsList.Add($"{(mode == ChannelMode.Arrow ? "arrow-memory" : "mem")}:{branch.Alias}");
                OnLogEvent?.Invoke($"  [grey]↳ Branch '{branch.Alias}' → [italic]{(mode == ChannelMode.Arrow ? "Arrow memory channel" : "memory channel")}[/][/]");

                if (!argsList.Contains("--no-stats", StringComparer.OrdinalIgnoreCase))
                    argsList.Add("--no-stats");
            }

            var updatedBranch = branch with { Arguments = argsList.ToArray() };

            try
            {
                int exitCode = await branchExecutor(updatedBranch, ctx, ct);

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
            .Select(s => _channelRegistry.GetChannel(s)?.Channel ?? throw new InvalidOperationException($"Broadcast: sub channel '{s}' not found."))
            .ToList();

        _logger.LogInformation("Starting broadcast multiplexer for '{SourceAlias}' → [{Subs}]",
            sourceAlias, string.Join(", ", subAliases));

        return Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in sourceTuple.Channel.Reader.ReadAllAsync(ct))
                    foreach (var sub in subChannels)
                        await sub.Writer.WriteAsync(batch, ct);
            }
            finally
            {
                foreach (var sub in subChannels) sub.Writer.TryComplete();
                _logger.LogInformation("Broadcast multiplexer for '{SourceAlias}' completed.", sourceAlias);
            }
        }, ct);
    }

    private Task StartArrowBroadcastAsync(string sourceAlias, IReadOnlyList<string> subAliases, CancellationToken ct)
    {
        var sourceTuple = _channelRegistry.GetArrowChannel(sourceAlias)
            ?? throw new InvalidOperationException($"Arrow Broadcast: source channel '{sourceAlias}' not found in registry.");

        var subChannels = subAliases
            .Select(s => _channelRegistry.GetArrowChannel(s)?.Channel ?? throw new InvalidOperationException($"Arrow Broadcast: sub channel '{s}' not found."))
            .ToList();

        _logger.LogInformation("Starting Arrow broadcast multiplexer for '{SourceAlias}' → [{Subs}]",
            sourceAlias, string.Join(", ", subAliases));

        return Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in sourceTuple.Channel.Reader.ReadAllAsync(ct))
                    foreach (var sub in subChannels)
                        await sub.Writer.WriteAsync(batch.Clone(), ct);
            }
            finally
            {
                foreach (var sub in subChannels) sub.Writer.TryComplete();
                _logger.LogInformation("Arrow Broadcast multiplexer for '{SourceAlias}' completed.", sourceAlias);
            }
        }, ct);
    }

    /// <summary>
    /// Determines whether a branch's output channel must use Arrow (RecordBatch) or Native (object[]) protocol.
    /// Arrow is required when the channel is consumed by a stream-transformer branch.
    /// Arrow is also used when the producer branch has a reader that yields columnar output natively.
    /// </summary>
    private ChannelMode GetRequiredChannelMode(JobDagDefinition dag, string alias)
    {
        // Check if alias is consumed by any stream-transformer branch (SQL, merge, etc.)
        foreach (var branch in dag.Branches.Where(b => b.HasStreamTransformer))
        {
            bool isConsumed =
                (branch.FromAlias != null && branch.FromAlias.Equals(alias, StringComparison.OrdinalIgnoreCase)) ||
                branch.RefAliases.Contains(alias, StringComparer.OrdinalIgnoreCase) ||
                branch.MergeAliases.Contains(alias, StringComparer.OrdinalIgnoreCase);

            if (isConsumed) return ChannelMode.Arrow;
        }

        // Check if the producer branch has a reader that yields columnar output
        var producerBranch = dag.Branches.FirstOrDefault(b =>
            b.Alias.Equals(alias, StringComparison.OrdinalIgnoreCase) && !b.HasStreamTransformer);

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

    /// <summary>
    /// Returns a copy of <paramref name="args"/> where <c>--from</c>, <c>--ref</c>, and <c>--merge</c>
    /// values are replaced with their fan-out sub-aliases when the source alias was broadcast.
    /// </summary>
    private static string[] ReplaceAliasesInArgs(
        string[] args,
        string? oldFromAlias, string? newFromAlias,
        IReadOnlyList<string> oldRefAliases, string[] newRefAliases,
        IReadOnlyList<string> oldMergeAliases, string[] newMergeAliases)
    {
        bool fromChanged = !string.IsNullOrEmpty(oldFromAlias)
            && !string.IsNullOrEmpty(newFromAlias)
            && !string.Equals(oldFromAlias, newFromAlias, StringComparison.OrdinalIgnoreCase);

        bool refsChanged = oldRefAliases.Count > 0
            && !oldRefAliases.SequenceEqual(newRefAliases, StringComparer.OrdinalIgnoreCase);

        bool mergesChanged = oldMergeAliases.Count > 0
            && !oldMergeAliases.SequenceEqual(newMergeAliases, StringComparer.OrdinalIgnoreCase);

        if (!fromChanged && !refsChanged && !mergesChanged) return args;

        var result = args.ToList();
        int refIdx = 0, mergeIdx = 0;
        bool fromDone = !fromChanged;

        for (int i = 0; i < result.Count - 1; i++)
        {
            if (!fromDone && result[i].Equals("--from", StringComparison.OrdinalIgnoreCase)
                && result[i + 1].Equals(oldFromAlias, StringComparison.OrdinalIgnoreCase))
            {
                result[i + 1] = newFromAlias!;
                fromDone = true;
            }
            else if (refsChanged && result[i].Equals("--ref", StringComparison.OrdinalIgnoreCase)
                && refIdx < oldRefAliases.Count
                && result[i + 1].Equals(oldRefAliases[refIdx], StringComparison.OrdinalIgnoreCase))
            {
                result[i + 1] = newRefAliases[refIdx];
                refIdx++;
            }
            else if (mergesChanged && result[i].Equals("--merge", StringComparison.OrdinalIgnoreCase)
                && mergeIdx < oldMergeAliases.Count
                && result[i + 1].Equals(oldMergeAliases[mergeIdx], StringComparison.OrdinalIgnoreCase))
            {
                result[i + 1] = newMergeAliases[mergeIdx];
                mergeIdx++;
            }
        }

        return result.ToArray();
    }
}
