using System.Collections.Concurrent;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// Unit tests for the DagOrchestrator engine — without CLI, without integration.
/// Each test builds a JobDagDefinition in C# and passes a mock branchExecutor.
/// Architectural requirement: these tests must pass before any commit to DagOrchestrator.
/// </summary>
public class DagOrchestratorTests
{
    private static DagOrchestrator BuildOrchestrator() => new(
        NullLogger<DagOrchestrator>.Instance,
        new MemoryChannelRegistry());

    // ─────────────────────────────────────────────────────────────────────────
    // Case 1: Linear pipeline — single branch, no memory channel
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Execute_SingleBranch_CallsExecutorOnceAndReturnsZero()
    {
        var dag = GoldenDagDefinitions.Linear_SingleBranch;
        var orchestrator = BuildOrchestrator();
        var called = new ConcurrentBag<string>();

        var result = await orchestrator.ExecuteAsync(dag, (b, _, _) =>
        {
            called.Add(b.Alias);
            return Task.FromResult(0);
        });

        Assert.Equal(0, result);
        Assert.Equal(["main"], called.ToArray());
    }

    [Fact]
    public async Task Execute_SingleBranch_ContextHasEmptyAliasMap()
    {
        var dag = GoldenDagDefinitions.Linear_SingleBranch;
        var orchestrator = BuildOrchestrator();
        BranchChannelContext? captured = null;

        await orchestrator.ExecuteAsync(dag, (_, ctx, _) =>
        {
            captured = ctx;
            return Task.FromResult(0);
        });

        Assert.NotNull(captured);
        Assert.Empty(captured!.AliasMap);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 2: DAG with two independent branches
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Execute_TwoBranches_CallsExecutorForEachBranch()
    {
        var dag = GoldenDagDefinitions.Dag_TwoInputs_OneOutput;
        var orchestrator = BuildOrchestrator();
        var called = new ConcurrentBag<string>();

        var result = await orchestrator.ExecuteAsync(dag, (b, _, _) =>
        {
            called.Add(b.Alias);
            return Task.FromResult(0);
        });

        Assert.Equal(0, result);
        Assert.Equal(2, called.Count);
        Assert.Contains("source1", called);
        Assert.Contains("output1", called);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 3: DAG with SQL stream transformer
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Execute_SqlProcessor_AllBranchesCalled()
    {
        var dag = GoldenDagDefinitions.Dag_SourcePlusSqlProcessor;
        var orchestrator = BuildOrchestrator();
        var called = new ConcurrentBag<string>();

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            called.Add(b.Alias);
            return Task.FromResult(0);
        });

        Assert.Equal(2, called.Count);
        Assert.Contains("src", called);
        Assert.Contains("processed", called);
    }

    [Fact]
    public async Task Execute_SqlProcessor_StreamTransformerBranchHasNoAliasRemap()
    {
        var dag = GoldenDagDefinitions.Dag_SourcePlusSqlProcessor;
        var orchestrator = BuildOrchestrator();
        BranchChannelContext? transformerCtx = null;

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (b.HasStreamTransformer) transformerCtx = ctx;
            return Task.FromResult(0);
        });

        Assert.NotNull(transformerCtx);
        // No fan-out on a single consumer — AliasMap should be empty
        Assert.Empty(transformerCtx!.AliasMap);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 4: Fan-out (tee) — one source, two consumers
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Execute_FanOut_AllThreeBranchesCalled()
    {
        var dag = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers;
        var orchestrator = BuildOrchestrator();
        var called = new ConcurrentBag<string>();

        var result = await orchestrator.ExecuteAsync(dag, (b, _, _) =>
        {
            called.Add(b.Alias);
            return Task.FromResult(0);
        });

        Assert.Equal(0, result);
        Assert.Equal(3, called.Count);
        Assert.Contains("src", called);
        Assert.Contains("consumer_a", called);
        Assert.Contains("consumer_b", called);
    }

    [Fact]
    public async Task Execute_FanOut_ConsumersReceiveDistinctPhysicalAliasesViaAliasMap()
    {
        var dag = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers;
        var orchestrator = BuildOrchestrator();
        var aliasMaps = new ConcurrentDictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            aliasMaps[b.Alias] = ctx.AliasMap;
            return Task.FromResult(0);
        });

        // Source: no remapping
        Assert.Empty(aliasMaps["src"]);

        // Consumers: each has a distinct physical sub-channel alias
        var mapA = aliasMaps.GetValueOrDefault("consumer_a");
        var mapB = aliasMaps.GetValueOrDefault("consumer_b");
        Assert.NotNull(mapA);
        Assert.NotNull(mapB);
        Assert.NotEmpty(mapA!);
        Assert.NotEmpty(mapB!);

        // The mapped values (physical aliases) must differ
        var physicalA = mapA!.Values.First();
        var physicalB = mapB!.Values.First();
        Assert.NotEqual(physicalA, physicalB);
    }

    [Fact]
    public async Task Execute_FanOut_AliasMapPropagatedToConsumers()
    {
        var dag = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers;
        var orchestrator = BuildOrchestrator();
        var aliasMaps = new ConcurrentDictionary<string, IReadOnlyDictionary<string, string>>(StringComparer.OrdinalIgnoreCase);

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            aliasMaps[b.Alias] = ctx.AliasMap;
            return Task.FromResult(0);
        });

        // Source: no remapping
        Assert.Empty(aliasMaps["src"]);

        // Consumers: non-empty AliasMap (logical -> physical)
        var mapA = aliasMaps.GetValueOrDefault("consumer_a");
        var mapB = aliasMaps.GetValueOrDefault("consumer_b");
        Assert.NotNull(mapA);
        Assert.NotNull(mapB);
        Assert.NotEmpty(mapA!);
        Assert.NotEmpty(mapB!);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Error propagation
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Execute_BranchFails_ReturnsNonZeroExitCode()
    {
        var dag = GoldenDagDefinitions.Linear_SingleBranch;
        var orchestrator = BuildOrchestrator();

        var result = await orchestrator.ExecuteAsync(dag, (_, _, _) => Task.FromResult(42));

        Assert.Equal(42, result);
    }

    // A run that returns while a branch still runs lets the next run's registry clear race that branch's
    // channel completion, so the orchestrator returns only once every branch has ended.
    private const int SlowBranchMs = 300;

    [Fact]
    public async Task Execute_BranchFailsWithCode_ReturnsOnlyAfterTheSlowSiblingEnds()
    {
        var dag = GoldenDagDefinitions.Dag_TwoInputs_OneOutput;
        var orchestrator = BuildOrchestrator();
        var siblingEnded = false;

        var result = await orchestrator.ExecuteAsync(dag, async (b, _, _) =>
        {
            if (b.Alias == "source1") return 42;
            await Task.Delay(SlowBranchMs); // does not honour the cancellation the failure raises
            siblingEnded = true;
            return 0;
        });

        Assert.Equal(42, result);
        Assert.True(siblingEnded, "ExecuteAsync returned while a sibling branch was still running.");
    }

    [Fact]
    public async Task Execute_BranchThrows_ReturnsOnlyAfterTheSlowSiblingEnds()
    {
        var dag = GoldenDagDefinitions.Dag_TwoInputs_OneOutput;
        var orchestrator = BuildOrchestrator();
        var siblingEnded = false;

        var result = await orchestrator.ExecuteAsync(dag, async (b, _, _) =>
        {
            if (b.Alias == "source1") throw new InvalidOperationException("boom");
            await Task.Delay(SlowBranchMs);
            siblingEnded = true;
            return 0;
        });

        Assert.Equal(1, result);
        Assert.True(siblingEnded, "ExecuteAsync returned while a sibling branch was still running.");
    }

    [Fact]
    public async Task Execute_SlowSiblingThrowsAfterTheFailure_KeepsTheFirstExitCode()
    {
        var dag = GoldenDagDefinitions.Dag_TwoInputs_OneOutput;
        var orchestrator = BuildOrchestrator();

        var result = await orchestrator.ExecuteAsync(dag, async (b, _, _) =>
        {
            if (b.Alias == "source1") return 42;
            await Task.Delay(SlowBranchMs);
            throw new InvalidOperationException("late");
        });

        Assert.Equal(42, result);
    }

    [Fact]
    public async Task Execute_EmptyDag_ReturnsOne()
    {
        var dag = new JobDagDefinition { Branches = Array.Empty<BranchDefinition>() };
        var orchestrator = BuildOrchestrator();

        var result = await orchestrator.ExecuteAsync(dag, (_, _, _) => Task.FromResult(0));

        Assert.Equal(1, result);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Typed channel endpoints — producer contract (F5)
    //
    // These tests verify that DagOrchestrator produces correct typed endpoints in the
    // BranchChannelContext before calling the branchExecutor. The CLI layer consumes
    // them directly — no CLI flag syntax is synthesized anywhere.
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Execute_LinearBranch_HasNoChannelInjectionPlan()
    {
        // Branch with explicit -i and -o → no injection needed.
        var dag = GoldenDagDefinitions.Linear_SingleBranch;
        var orchestrator = BuildOrchestrator();
        BranchChannelContext? captured = null;

        await orchestrator.ExecuteAsync(dag, (_, ctx, _) =>
        {
            captured = ctx;
            return Task.FromResult(0);
        });

        Assert.Null(captured?.InputEndpoint);
        Assert.Null(captured?.OutputEndpoint);
        Assert.False(captured?.SuppressStats ?? true);
    }

    [Fact]
    public async Task ReplaceAliasesInArgs_Method_No_Longer_Exists()
    {
        var method = typeof(DagOrchestrator).GetMethod("ReplaceAliasesInArgs",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Instance);
        Assert.Null(method);
    }

    [Fact]
    public async Task Execute_SourceBranch_ChannelInjectionSetsOutputSpecAndSuppressesStats()
    {
        // Source without -o → the orchestrator must fill OutputChannelSpec
        // and enable SuppressStats (intermediate branch).
        var dag = GoldenDagDefinitions.Dag_SourcePlusSqlProcessor;
        var orchestrator = BuildOrchestrator();
        BranchChannelContext? sourceCtx = null;

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (!b.HasStreamTransformer && string.IsNullOrEmpty(b.Output))
                sourceCtx = ctx;
            return Task.FromResult(0);
        });

        var ctxOut = sourceCtx;
        Assert.NotNull(ctxOut);
        Assert.NotNull(ctxOut!.OutputEndpoint);   // source writes to a memory channel
        Assert.Equal(InternalChannelKind.Arrow, ctxOut.OutputEndpoint!.Kind);
        Assert.Null(ctxOut.InputEndpoint);        // source reads its normal -i
        Assert.True(ctxOut.SuppressStats);        // non-terminal → stats suppressed
    }

    [Fact]
    public async Task Execute_SourceFeedingStreamTransformer_ChannelInjectionUsesArrowMode()
    {
        // A source consumed by a SQL/merge processor writes an Arrow channel, the
        // transport the processor reads.
        var dag = GoldenDagDefinitions.Dag_SourcePlusSqlProcessor;
        var orchestrator = BuildOrchestrator();
        BranchChannelContext? sourceCtx = null;

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (!b.HasStreamTransformer && string.IsNullOrEmpty(b.Output))
                sourceCtx = ctx;
            return Task.FromResult(0);
        });

        Assert.NotNull(sourceCtx?.OutputEndpoint);
        Assert.Equal(InternalChannelKind.Arrow, sourceCtx!.OutputEndpoint!.Kind);
    }

    [Fact]
    public async Task Execute_FanOutConsumers_EachGetDistinctInputChannelSpec()
    {
        // Two consumers of the same alias get InputChannelSpec
        // pointing to distinct physical fan-out sub-channels.
        var dag = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers;
        var orchestrator = BuildOrchestrator();
        var plans = new ConcurrentDictionary<string, BranchChannelContext?>(StringComparer.OrdinalIgnoreCase);

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            plans[b.Alias] = ctx;
            return Task.FromResult(0);
        });

        // Source → output endpoint (to memory channel), no input endpoint
        var srcCtx = plans["src"];
        Assert.NotNull(srcCtx?.OutputEndpoint);
        Assert.Null(srcCtx!.InputEndpoint);
        Assert.True(srcCtx.SuppressStats);

        // Consumers → input endpoints pointing at distinct fan-out sub-channels
        var ctxA = plans["consumer_a"];
        var ctxB = plans["consumer_b"];
        Assert.NotNull(ctxA?.InputEndpoint);
        Assert.NotNull(ctxB?.InputEndpoint);
        Assert.Null(ctxA!.OutputEndpoint);
        Assert.Null(ctxB!.OutputEndpoint);
        Assert.False(ctxA.SuppressStats);
        Assert.False(ctxB.SuppressStats);

        // The two physical sub-channels (aliases) are distinct and carry the fan prefix
        Assert.NotEqual(ctxA.InputEndpoint!.Alias, ctxB.InputEndpoint!.Alias);
        Assert.Contains(IChannelNaming.FanPrefix, ctxA.InputEndpoint.Alias);
        Assert.Contains(IChannelNaming.FanPrefix, ctxB.InputEndpoint.Alias);
        Assert.Equal(InternalChannelKind.Arrow, ctxA.InputEndpoint.Kind);
    }

    [Fact]
    public async Task Execute_SqlProcessorFeedingRowSink_ChannelInjectionUsesArrowMode()
    {
        // A SQL branch feeding into a row-mode sink (like a CSV file) MUST use
        // an Arrow channel because SQL branches only produce columnar output.
        // This was the root cause of the crash in DAG Test [8].
        var dag = new JobDagDefinition
        {
            Branches = new[]
            {
                new BranchDefinition { Alias = "joined", ProcessorName = "sql", Arguments = new[] { "--alias", "joined" } },
                new BranchDefinition { Alias = "sink", StreamingAliases = new List<string> { "joined" }, Arguments = new[] { "-o", "out.csv", "--from", "joined" }, Output = "out.csv" }
            }
        };

        var orchestrator = BuildOrchestrator();
        InternalChannelEndpoint? joinedOut = null;

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (b.Alias == "joined") joinedOut = ctx.OutputEndpoint;
            return Task.FromResult(0);
        });

        Assert.NotNull(joinedOut);
        Assert.Equal(InternalChannelKind.Arrow, joinedOut!.Kind);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Case 5: Merge (UNION ALL) — two sources, one processor
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Execute_Merge_AllBranchesCalled()
    {
        var dag = GoldenDagDefinitions.Dag_Merge_TwoSources;
        var orchestrator = BuildOrchestrator();
        var called = new ConcurrentBag<string>();

        var result = await orchestrator.ExecuteAsync(dag, (b, _, _) =>
        {
            called.Add(b.Alias);
            return Task.FromResult(0);
        });

        Assert.Equal(0, result);
        Assert.Equal(3, called.Count);
        Assert.Contains("stream_a", called);
        Assert.Contains("stream_b", called);
        Assert.Contains("merged", called);
    }

    [Fact]
    public async Task Execute_MergeSourceBranches_ChannelInjectionUsesArrowMode()
    {
        // Sources of a merge processor use Arrow transport
        // (required by MergeTransformerFactory).
        var dag = GoldenDagDefinitions.Dag_Merge_TwoSources;
        var orchestrator = BuildOrchestrator();
        var plans = new ConcurrentDictionary<string, BranchChannelContext?>(StringComparer.OrdinalIgnoreCase);

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            plans[b.Alias] = ctx;
            return Task.FromResult(0);
        });

        // Both sources feed a stream-transformer → Arrow
        Assert.Equal(InternalChannelKind.Arrow, plans["stream_a"]?.OutputEndpoint?.Kind);
        Assert.Equal(InternalChannelKind.Arrow, plans["stream_b"]?.OutputEndpoint?.Kind);

        // The merge processor has an explicit -o → no channel endpoints
        Assert.Null(plans["merged"]?.InputEndpoint);
        Assert.Null(plans["merged"]?.OutputEndpoint);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cancellation semantics (F16 — cancellation must not mask as success)
    //
    // Documented intentional exception: ExecuteBranchAsync's orphaned-producer path
    // returns 0 (producer termination when consumers complete is normal fan-out
    // operation, not failure). A GLOBAL cancellation, however, must return non-zero.
    // ─────────────────────────────────────────────────────────────────────────

    private static async Task<int> WaitUntilCanceledThenZeroAsync(CancellationToken ct)
    {
        try
        {
            await Task.Delay(Timeout.Infinite, ct);
        }
        catch (OperationCanceledException)
        {
            // Orphaned-producer contract: cancellation is a normal shutdown for this branch.
        }
        return 0;
    }

    [Fact]
    public async Task Global_Cancellation_In_3Branch_DAG_Returns_1_Not_0()
    {
        var dag = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers; // 3 branches
        var orchestrator = BuildOrchestrator();
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-canceled

        var result = await orchestrator.ExecuteAsync(dag, (_, _, ct) => Task.FromCanceled<int>(ct), cts.Token);

        Assert.Equal(1, result);
    }

    [Fact]
    public async Task PerBranch_Orphan_Cancellation_Returns_0_When_Others_Succeed()
    {
        var dag = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers;
        var orchestrator = BuildOrchestrator();
        var called = new ConcurrentBag<string>();

        var result = await orchestrator.ExecuteAsync(dag, (b, _, ct) =>
        {
            called.Add(b.Alias);
            // Source blocks until the orchestrator cancels it as an orphaned producer.
            return b.Alias == "src" ? WaitUntilCanceledThenZeroAsync(ct) : Task.FromResult(0);
        });

        Assert.Equal(0, result);
        Assert.Equal(3, called.Count);
    }

    [Fact]
    public async Task All_Branches_Orphaned_During_Global_Cancellation_Returns_1()
    {
        var dag = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers;
        var orchestrator = BuildOrchestrator();
        using var cts = new CancellationTokenSource();

        var runTask = orchestrator.ExecuteAsync(dag,
            (_, _, ct) => WaitUntilCanceledThenZeroAsync(ct), cts.Token);

        // Cancel globally while all branches are still running.
        await Task.Delay(100);
        await cts.CancelAsync();

        var result = await runTask;

        Assert.Equal(1, result);
    }
}
