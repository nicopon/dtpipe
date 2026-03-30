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
    private readonly object _schemaLock = new();
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
            foreach (var a in branch.StreamingAliases) inputs.Add(a);
            foreach (var r in branch.RefAliases) inputs.Add(r);

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
            foreach (var a in branch.StreamingAliases) inputs.Add(a);
            foreach (var r in branch.RefAliases) inputs.Add(r);

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

                // Stream-transformer branches (SQL, merge, …): preserve logical aliases in args
                // so table names and alias references remain correct; factories resolve via AliasMap.
                // Fan-out consumer branches: rewrite --from value to the physical alias.
                string[] resolvedArgs;
                if (branch.HasStreamTransformer)
                    resolvedArgs = branch.Arguments;
                else
                    resolvedArgs = ReplaceAliasesInArgs(
                        branch.Arguments,
                        branch.StreamingAliases.Count > 0 ? branch.StreamingAliases[0] : null,
                        actualStreamingAliases.Length > 0 ? actualStreamingAliases[0] : null,
                        branch.RefAliases, actualRefAliases);

                var updatedBranch = branch with
                {
                    RefAliases = actualRefAliases,
                    Arguments = resolvedArgs
                };

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
                    var innerMsg = finishedTask.Exception?.InnerException?.Message
                        ?? finishedTask.Exception?.Message
                        ?? "Unknown error";
                    OnLogEvent?.Invoke($"✖ {innerMsg}");
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
            return 0;
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("DAG execution was canceled.");
            OnLogEvent?.Invoke("⚠ DAG execution was canceled.");
            return 1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error during DAG orchestration.");
            OnLogEvent?.Invoke($"✖ Unexpected error during DAG orchestration: {ex.Message}");
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
                    : branch.StreamingAliases.Count > 0 ? "[cyan]Fan-out (tee)[/]"
                    : "[blue]Linear Branch[/]";

        _logger.LogInformation("Starting branch '{Alias}' [HasStreamTransformer={HasST}, From={From}]",
            branch.Alias, branch.HasStreamTransformer, branch.StreamingAliases.FirstOrDefault() ?? "(none)");

        try
        {
            // Build the channel injection plan — communicate routing to the CLI layer without
            // embedding CLI flag syntax (-i, -o, mem:, arrow-memory:, --no-stats) in Core.
            (ChannelMode Mode, string Alias)? inputChannel = null;
            if (!branch.HasStreamTransformer && string.IsNullOrEmpty(branch.Input) && !string.IsNullOrEmpty(resolvedFromAlias))
            {
                var logicalAlias = branch.StreamingAliases.Count > 0 ? branch.StreamingAliases[0] : resolvedFromAlias;
                inputChannel = (GetRequiredChannelMode(dag, logicalAlias), resolvedFromAlias);
            }

            (ChannelMode Mode, string Alias)? outputChannel = null;
            bool suppressStats = false;
            if (string.IsNullOrEmpty(branch.Output))
            {
                outputChannel = (GetRequiredChannelMode(dag, branch.Alias), branch.Alias);
                suppressStats = true;
            }

            var plan = (inputChannel.HasValue || outputChannel.HasValue)
                ? new ChannelInjectionPlan { InputChannel = inputChannel, OutputChannel = outputChannel, SuppressStats = suppressStats }
                : null;

            var enrichedCtx = plan != null ? ctx with { ChannelInjection = plan } : ctx;

            try
            {
                int exitCode = await branchExecutor(branch, enrichedCtx, ct);

                if (exitCode != 0)
                {
                    _logger.LogError("Branch '{Alias}' failed with exit code {ExitCode}.", branch.Alias, exitCode);
                    OnLogEvent?.Invoke($"  ✖ Branch '{branch.Alias}' failed with exit code {exitCode}.");
                    return exitCode;
                }

                _channelRegistry.GetChannel(branch.Alias)?.Channel.Writer.TryComplete();
                _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete();

                _logger.LogInformation("Branch '{Alias}' completed.", branch.Alias);
                return 0;
            }
            catch (OperationCanceledException)
            {
                _channelRegistry.GetChannel(branch.Alias)?.Channel.Writer.TryComplete();
                _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete();
                _logger.LogInformation("Branch '{Alias}' terminated due to cancellation (orphaned producer).", branch.Alias);
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
        var source = (_channelRegistry.GetChannel(sourceAlias)
            ?? throw new InvalidOperationException($"Broadcast: source channel '{sourceAlias}' not found in registry.")).Channel.Reader;
        var targets = subAliases
            .Select(s => (_channelRegistry.GetChannel(s)?.Channel ?? throw new InvalidOperationException($"Broadcast: sub channel '{s}' not found.")).Writer)
            .ToList();
        _logger.LogInformation("Starting broadcast multiplexer for '{SourceAlias}' → [{Subs}]",
            sourceAlias, string.Join(", ", subAliases));
        return BroadcastAsync(source, targets, transform: null, sourceAlias, _logger, ct);
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
        return BroadcastAsync(source, targets, transform: b => b.Clone(), sourceAlias, _logger, ct);
    }

    private static Task BroadcastAsync<T>(
        ChannelReader<T> source,
        IReadOnlyList<ChannelWriter<T>> targets,
        Func<T, T>? transform,
        string sourceAlias,
        ILogger logger,
        CancellationToken ct)
        => Task.Run(async () =>
        {
            try
            {
                await foreach (var item in source.ReadAllAsync(ct))
                {
                    var toWrite = transform != null ? transform(item) : item;
                    foreach (var t in targets)
                        await t.WriteAsync(toWrite, ct);
                }
            }
            finally
            {
                foreach (var t in targets) t.TryComplete();
                logger.LogInformation("Broadcast multiplexer for '{Alias}' completed.", sourceAlias);
            }
        }, ct);

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
                branch.StreamingAliases.Contains(alias, StringComparer.OrdinalIgnoreCase) ||
                branch.RefAliases.Contains(alias, StringComparer.OrdinalIgnoreCase);

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
    /// Returns a copy of <paramref name="args"/> where <c>--from</c> and <c>--ref</c> values
    /// are replaced with their fan-out sub-aliases when the source alias was broadcast.
    /// Only called for fan-out consumer branches (non-stream-transformer branches).
    /// </summary>
    private static string[] ReplaceAliasesInArgs(
        string[] args,
        string? oldFromAlias, string? newFromAlias,
        IReadOnlyList<string> oldRefAliases, string[] newRefAliases)
    {
        bool fromChanged = !string.IsNullOrEmpty(oldFromAlias)
            && !string.IsNullOrEmpty(newFromAlias)
            && !string.Equals(oldFromAlias, newFromAlias, StringComparison.OrdinalIgnoreCase);

        bool refsChanged = oldRefAliases.Count > 0
            && !oldRefAliases.SequenceEqual(newRefAliases, StringComparer.OrdinalIgnoreCase);

        if (!fromChanged && !refsChanged) return args;

        var result = args.ToList();
        int refIdx = 0;
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
        }

        return result.ToArray();
    }
}
