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
    private readonly List<IXStreamerFactory> _xstreamerFactories;
    private readonly List<IStreamReaderFactory> _readerFactories;
    private static readonly object _schemaLock = new();
    private static readonly Schema _emptySchema = new Schema(System.Array.Empty<Field>(), null);

    public Action<string>? OnLogEvent { get; set; }

    public DagOrchestrator(
        ILogger<DagOrchestrator> logger,
        IMemoryChannelRegistry channelRegistry,
        IEnumerable<IXStreamerFactory> xstreamerFactories,
        IEnumerable<IStreamReaderFactory> readerFactories)
    {
        _logger = logger;
        _channelRegistry = channelRegistry;
        _xstreamerFactories = xstreamerFactories.ToList();
        _readerFactories = readerFactories.ToList();
    }

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
        // Pre-scan: count how many branches consume each alias via --from.
        // Aliases with more than 1 consumer require a Broadcast channel split.
        // ──────────────────────────────────────────────────────────────────
        var fromConsumerCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var branch in dag.Branches)
        {
            if (!string.IsNullOrEmpty(branch.FromAlias))
                fromConsumerCounts[branch.FromAlias] = fromConsumerCounts.GetValueOrDefault(branch.FromAlias) + 1;
        }

        // For each alias consumed by N > 1 --from branches, track the sub-aliases in order
        // so each consumer gets a distinct sub-channel (alias__fan_0, alias__fan_1, …).
        var broadcastSubAliases = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (alias, count) in fromConsumerCounts.Where(kv => kv.Value > 1))
        {
            var subs = new List<string>(count);
            for (int i = 0; i < count; i++)
                subs.Add($"{alias}__fan_{i}");
            broadcastSubAliases[alias] = subs;
            _logger.LogInformation("Broadcast fan-out detected for alias '{Alias}': {Count} consumers → sub-channels {Subs}",
                alias, count, string.Join(", ", subs));
            OnLogEvent?.Invoke($"  [grey]🔀 Broadcast '{alias}' → {count} sub-channels[/]");
        }

        // For aliases with exactly 1 consumer, no sub-channel is needed; the consumer reads directly.
        // Track assignment cursors for broadcast aliases.
        var broadcastAssignmentCursor = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var alias in broadcastSubAliases.Keys)
            broadcastAssignmentCursor[alias] = 0;

        var tasks = new List<Task<int>>();

        _logger.LogInformation("Orchestrating DAG execution with {BranchCount} branches.", dag.Branches.Count);
        OnLogEvent?.Invoke($"[bold blue]Orchestrating DAG execution with {dag.Branches.Count} branches...[/]");

        try
        {
            foreach (var branch in dag.Branches)
            {
                // Resolve the effective alias for --from branches that require a broadcast sub-channel
                string? resolvedFromAlias = null;
                if (!string.IsNullOrEmpty(branch.FromAlias))
                {
                    if (broadcastSubAliases.TryGetValue(branch.FromAlias, out var subs))
                    {
                        var idx = broadcastAssignmentCursor[branch.FromAlias];
                        resolvedFromAlias = subs[idx];
                        broadcastAssignmentCursor[branch.FromAlias] = idx + 1;
                    }
                    else
                    {
                        // Single consumer: read directly from the source channel
                        resolvedFromAlias = branch.FromAlias;
                    }
                }

                tasks.Add(ExecuteBranchAsync(dag, branch, resolvedFromAlias, broadcastSubAliases, branchExecutor, effectiveCt));

                // Stagger starts to avoid race conditions in native library loading or CLI parsing
                await Task.Delay(50, effectiveCt);
            }

            _logger.LogInformation("DEBUG: All branches started, waiting for completion...");
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

    private async Task<int> ExecuteBranchAsync(
        JobDagDefinition dag,
        BranchDefinition branch,
        string? resolvedFromAlias,
        Dictionary<string, List<string>> broadcastSubAliases,
        Func<BranchDefinition, CancellationToken, Task<int>> branchExecutor,
        CancellationToken ct)
    {
        // CRITICAL: Ensure we yield immediately to allow the orchestration loop to start other branches
        await Task.Yield();
        _logger.LogInformation("Starting branch '{Alias}' [IsXStreamer={IsXStreamer}, FromAlias={FromAlias}]",
            branch.Alias, branch.IsXStreamer, branch.FromAlias ?? "(none)");

        string role = branch.IsXStreamer ? "[magenta]XStreamer[/]"
                    : !string.IsNullOrEmpty(branch.FromAlias) ? "[cyan]Fan-out (tee)[/]"
                    : "[blue]Linear Branch[/]";
        OnLogEvent?.Invoke($"  [grey]>[/] Starting {role} '{branch.Alias}'");

        try
        {
            var argsList = branch.Arguments.ToList();

            // ── XStreamer: ensure engine name is injected ──────────────────
            if (branch.IsXStreamer)
            {
                var engineInArgs = ExtractArgValue(argsList, "-x") ?? ExtractArgValue(argsList, "--xstreamer");
                if (string.IsNullOrEmpty(engineInArgs) && !string.IsNullOrEmpty(branch.Input))
                {
                    var xFlags = new[] { "-x", "--xstreamer" };
                    for (int i = 0; i < argsList.Count; i++)
                    {
                        if (xFlags.Contains(argsList[i], StringComparer.OrdinalIgnoreCase))
                        {
                            argsList.Insert(i + 1, branch.Input);
                            break;
                        }
                    }
                }
            }

            // ── Linear branch without explicit -i: implicit input injection ──
            // Covers two cases:
            //   (a) --from branches (fan-out / tee) → inject broadcast sub-channel
            //   (b) legacy: non-XStreamer branches with MainAlias (kept for YAML compat)
            if (!branch.IsXStreamer && string.IsNullOrEmpty(branch.Input))
            {
                if (!string.IsNullOrEmpty(resolvedFromAlias))
                {
                    // Fan-out branch: inject the resolved broadcast sub-channel as -i
                    var prefix = GetRequiredChannelMode(dag, branch.FromAlias!) == XStreamerChannelMode.Arrow
                        ? "arrow-memory"
                        : "mem";
                    argsList.Insert(0, $"{prefix}:{resolvedFromAlias}");
                    argsList.Insert(0, "-i");
                    _logger.LogInformation(
                        "Fan-out branch '{BranchAlias}': injected '-i {Prefix}:{ChannelAlias}' (broadcast from '{FromAlias}')",
                        branch.Alias, prefix, resolvedFromAlias, branch.FromAlias);
                }
                else if (!string.IsNullOrEmpty(branch.MainAlias))
                {
                    // Legacy / YAML-compat: non-XStreamer branch with MainAlias
                    var prefix = GetRequiredChannelMode(dag, branch.MainAlias) == XStreamerChannelMode.Arrow
                        ? "arrow-memory"
                        : "mem";
                    argsList.Insert(0, $"{prefix}:{branch.MainAlias}");
                    argsList.Insert(0, "-i");
                    _logger.LogInformation(
                        "Injected implicit input channel '{Prefix}:{Alias}' for branch '{BranchAlias}'",
                        prefix, branch.MainAlias, branch.Alias);
                }
            }

            // ── Output channel: must go to memory if the alias is consumed downstream ──
            if (string.IsNullOrEmpty(branch.Output))
            {
                var mode = GetRequiredChannelMode(dag, branch.Alias);

                if (mode == XStreamerChannelMode.Arrow)
                {
                    var arrowChannel = Channel.CreateBounded<Apache.Arrow.RecordBatch>(new BoundedChannelOptions(64)
                    {
                        FullMode = BoundedChannelFullMode.Wait
                    });
                    _channelRegistry.RegisterArrowChannel(branch.Alias, arrowChannel, _emptySchema);
                    argsList.Add("-o");
                    argsList.Add($"arrow-memory:{branch.Alias}");
                    _logger.LogInformation("Branch '{Alias}' → Arrow memory channel.", branch.Alias);
                    OnLogEvent?.Invoke($"  [grey]↳ Branch '{branch.Alias}' → [italic]Arrow memory channel[/][/]");

                    if (broadcastSubAliases.TryGetValue(branch.Alias, out var subs))
                    {
                        _ = StartArrowBroadcastAsync(branch.Alias, subs, ct);
                    }
                }
                else
                {
                    var channel = Channel.CreateBounded<IReadOnlyList<object?[]>>(new BoundedChannelOptions(100)
                    {
                        SingleWriter = true,
                        SingleReader = true, // We will use multiple readers ONLY via broadcast sub-channels
                        FullMode = BoundedChannelFullMode.Wait
                    });

                    _channelRegistry.RegisterChannel(branch.Alias, channel, System.Array.Empty<PipeColumnInfo>());
                    argsList.Add("-o");
                    argsList.Add($"mem:{branch.Alias}");
                    _logger.LogInformation("Branch '{Alias}' → Native memory channel.", branch.Alias);
                    OnLogEvent?.Invoke($"  [grey]↳ Branch '{branch.Alias}' → [italic]memory channel[/][/]");

                    // If this alias is consumed by multiple --from branches, start the broadcast multiplexer task.
                    if (broadcastSubAliases.TryGetValue(branch.Alias, out var subs))
                    {
                        _ = StartNativeBroadcastAsync(branch.Alias, subs, ct);
                    }
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
                var updatedBranch = branch with { Arguments = argsList.ToArray() };
                int exitCode = await branchExecutor(updatedBranch, branchCts.Token);

                if (exitCode != 0)
                {
                    _logger.LogError("Branch '{Alias}' failed with exit code {ExitCode}.", branch.Alias, exitCode);
                    OnLogEvent?.Invoke($"  [red]✖[/] Branch '{branch.Alias}' failed with exit code {exitCode}.");

                    _channelRegistry.GetChannel(branch.Alias)?.Channel.Writer.TryComplete(new Exception($"Branch failed with exit code {exitCode}"));
                    _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete(new Exception($"Branch failed with exit code {exitCode}"));

                    return exitCode;
                }

                // Successfully finished processing
                _channelRegistry.GetChannel(branch.Alias)?.Channel.Writer.TryComplete();
                _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete();

                _logger.LogInformation("Branch '{Alias}' completed.", branch.Alias);
                OnLogEvent?.Invoke($"  [green]✓[/] Branch '{branch.Alias}' completed.");
                return 0;
            }
            catch (Exception ex)
            {
                _channelRegistry.GetChannel(branch.Alias)?.Channel.Writer.TryComplete(ex);
                _channelRegistry.GetArrowChannel(branch.Alias)?.Channel.Writer.TryComplete(ex);
                throw;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Branch '{Alias}' encountered a fatal error.", branch.Alias);

            var storedChannel = _channelRegistry.GetChannel(branch.Alias);
            storedChannel?.Channel.Writer.TryComplete(ex);

            var arrowChannel = _channelRegistry.GetArrowChannel(branch.Alias);
            arrowChannel?.Channel.Writer.TryComplete(ex);

            throw;
        }
    }

    /// <summary>
    /// Creates N sub-channels for a broadcast alias and launches a background task that reads from
    /// the source channel and fans out each item to all N sub-channels. Call this after the source
    /// branch channel is registered but before the consumer branches start reading.
    /// </summary>
    private Task StartNativeBroadcastAsync(
        string sourceAlias,
        IReadOnlyList<string> subAliases,
        CancellationToken ct)
    {
        var sourceTuple = _channelRegistry.GetChannel(sourceAlias)
            ?? throw new InvalidOperationException($"Broadcast: source channel '{sourceAlias}' not found in registry.");

        var subChannels = new List<Channel<IReadOnlyList<object?[]>>>(subAliases.Count);
        foreach (var subAlias in subAliases)
        {
            var sub = Channel.CreateBounded<IReadOnlyList<object?[]>>(new BoundedChannelOptions(100)
            {
                SingleWriter = true,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });
            _channelRegistry.RegisterChannel(subAlias, sub, sourceTuple.Columns);
            subChannels.Add(sub);
        }

        _logger.LogInformation("Starting broadcast multiplexer for '{SourceAlias}' → [{Subs}]",
            sourceAlias, string.Join(", ", subAliases));

        return Task.Run(async () =>
        {
            try
            {
                // Wait for schema to be available (upstream branch populates it after first batch)
                // We need to propagate column info to all sub-channels once available.
                await foreach (var batch in sourceTuple.Channel.Reader.ReadAllAsync(ct))
                {
                    foreach (var sub in subChannels)
                    {
                        await sub.Writer.WriteAsync(batch, ct);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Broadcast multiplexer for '{SourceAlias}' failed.", sourceAlias);
                foreach (var sub in subChannels)
                    sub.Writer.TryComplete(ex);
                return;
            }

            foreach (var sub in subChannels)
                sub.Writer.TryComplete();

            _logger.LogInformation("Broadcast multiplexer for '{SourceAlias}' completed.", sourceAlias);
        }, ct);
    }

    private Task StartArrowBroadcastAsync(
        string sourceAlias,
        IReadOnlyList<string> subAliases,
        CancellationToken ct)
    {
        var sourceTuple = _channelRegistry.GetArrowChannel(sourceAlias)
            ?? throw new InvalidOperationException($"Arrow Broadcast: source channel '{sourceAlias}' not found in registry.");

        var subChannels = new List<Channel<Apache.Arrow.RecordBatch>>(subAliases.Count);
        foreach (var subAlias in subAliases)
        {
            var sub = Channel.CreateBounded<Apache.Arrow.RecordBatch>(new BoundedChannelOptions(64)
            {
                SingleWriter = true,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait
            });
            _channelRegistry.RegisterArrowChannel(subAlias, sub, sourceTuple.Schema);
            subChannels.Add(sub);
        }

        _logger.LogInformation("Starting Arrow broadcast multiplexer for '{SourceAlias}' → [{Subs}]",
            sourceAlias, string.Join(", ", subAliases));

        return Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in sourceTuple.Channel.Reader.ReadAllAsync(ct))
                {
                    // For Arrow RecordBatches, we should technically clone the batch to ensure thread safety
                    // since multiple consumers might eventually drop memory references concurrently if
                    // using unmanaged memory. For dtpipe's managed arrow arrays this is safe enough.
                    foreach (var sub in subChannels)
                    {
                        // Note: To be perfectly safe across native DataFusion bounds,
                        // each consumer needs its own reference count or independent clone.
                        // Assuming .Clone() or just passing the reference (managed memory).
                        await sub.Writer.WriteAsync(batch.Clone(), ct);
                    }
                    batch.Dispose(); // The source reader disposes the original batch, subs took cloned references
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Arrow Broadcast multiplexer for '{SourceAlias}' failed.", sourceAlias);
                foreach (var sub in subChannels)
                    sub.Writer.TryComplete(ex);
                return;
            }

            foreach (var sub in subChannels)
                sub.Writer.TryComplete();

            _logger.LogInformation("Arrow Broadcast multiplexer for '{SourceAlias}' completed.", sourceAlias);
        }, ct);
    }

    private XStreamerChannelMode GetRequiredChannelMode(JobDagDefinition dag, string alias)
    {
        foreach (var branch in dag.Branches.Where(b => b.IsXStreamer))
        {
            bool isConsumer =
                branch.MainAlias == alias ||
                branch.RefAliases.Contains(alias);

            if (!isConsumer) continue;

            var xFlag = branch.Input;
            if (xFlag == null) continue;

            var factory = _xstreamerFactories
                .FirstOrDefault(f => f.ComponentName.Equals(xFlag, StringComparison.OrdinalIgnoreCase));

            if (factory != null)
                return factory.ChannelMode;
        }

        // New: detect if the producing branch has a columnar reader
        var producerBranch = dag.Branches.FirstOrDefault(b =>
            b.Alias.Equals(alias, StringComparison.OrdinalIgnoreCase) && !b.IsXStreamer);

        if (producerBranch != null && !string.IsNullOrEmpty(producerBranch.Input))
        {
            // Look for a reader factory that handles this input.
            var readerFactory = _readerFactories.FirstOrDefault(f =>
                producerBranch.Input.StartsWith(f.ComponentName + ":", StringComparison.OrdinalIgnoreCase) ||
                producerBranch.Input.Equals(f.ComponentName, StringComparison.OrdinalIgnoreCase) ||
                f.CanHandle(producerBranch.Input));

            if (readerFactory?.YieldsColumnarOutput == true)
            {
                _logger.LogInformation(
                    "Branch '{Alias}' uses columnar reader '{Reader}': switching to arrow-memory channel.",
                    alias, readerFactory.ComponentName);
                return XStreamerChannelMode.Arrow;
            }
        }

        return XStreamerChannelMode.Native; // default if not found
    }

    private string? ExtractArgValue(IEnumerable<string> args, string argName)
    {
        var list = args.ToList();
        int idx = list.FindIndex(a => a.Equals(argName, StringComparison.OrdinalIgnoreCase));
        if (idx >= 0 && idx + 1 < list.Count)
        {
            var val = list[idx + 1];
            if (val.StartsWith('-')) return null; // Looks like another flag
            return val;
        }
        return null;
    }
}
