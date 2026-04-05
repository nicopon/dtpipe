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
        new MemoryChannelRegistry(),
        readerFactories: []);

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

    [Fact]
    public async Task Execute_EmptyDag_ReturnsOne()
    {
        var dag = new JobDagDefinition { Branches = Array.Empty<BranchDefinition>() };
        var orchestrator = BuildOrchestrator();

        var result = await orchestrator.ExecuteAsync(dag, (_, _, _) => Task.FromResult(0));

        Assert.Equal(1, result);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // ChannelInjectionPlan — producer contract
    //
    // These tests verify that DagOrchestrator produces correct ChannelInjectionPlan
    // objects in ctx.ChannelInjection before calling the branchExecutor.
    // The CLI layer (LinearPipelineService) consumes this plan to resolve
    // Input/Output for each branch without the engine injecting CLI flags.
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

        Assert.Null(captured?.ChannelInjection);
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

        var plan = sourceCtx?.ChannelInjection;
        Assert.NotNull(plan);
        Assert.True(plan!.OutputChannel.HasValue);  // source writes to a memory channel
        Assert.False(plan.InputChannel.HasValue);   // source reads its normal -i
        Assert.True(plan.SuppressStats);            // non-terminal → stats suppressed
    }

    [Fact]
    public async Task Execute_SourceFeedingStreamTransformer_ChannelInjectionUsesArrowMode()
    {
        // A source consumed by a SQL/merge processor must use arrow-memory
        // for the Arrow IPC transport required by the processor.
        var dag = GoldenDagDefinitions.Dag_SourcePlusSqlProcessor;
        var orchestrator = BuildOrchestrator();
        BranchChannelContext? sourceCtx = null;

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (!b.HasStreamTransformer && string.IsNullOrEmpty(b.Output))
                sourceCtx = ctx;
            return Task.FromResult(0);
        });

        Assert.True(sourceCtx?.ChannelInjection?.OutputChannel.HasValue);
        Assert.Equal(ChannelMode.Arrow, sourceCtx!.ChannelInjection!.OutputChannel!.Value.Mode);
    }

    [Fact]
    public async Task Execute_FanOutConsumers_EachGetDistinctInputChannelSpec()
    {
        // Two consumers of the same alias get InputChannelSpec
        // pointing to distinct physical fan-out sub-channels.
        var dag = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers;
        var orchestrator = BuildOrchestrator();
        var plans = new ConcurrentDictionary<string, ChannelInjectionPlan?>(StringComparer.OrdinalIgnoreCase);

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            plans[b.Alias] = ctx.ChannelInjection;
            return Task.FromResult(0);
        });

        // Source → OutputChannel (to memory channel), no InputChannel
        var srcPlan = plans["src"];
        Assert.True(srcPlan?.OutputChannel.HasValue);
        Assert.False(srcPlan!.InputChannel.HasValue);
        Assert.True(srcPlan.SuppressStats);

        // Consumers → InputChannel (from fan-out sub-channel), no OutputChannel
        var planA = plans["consumer_a"];
        var planB = plans["consumer_b"];
        Assert.True(planA?.InputChannel.HasValue);
        Assert.True(planB?.InputChannel.HasValue);
        Assert.False(planA!.OutputChannel.HasValue);
        Assert.False(planB!.OutputChannel.HasValue);
        Assert.False(planA.SuppressStats);
        Assert.False(planB.SuppressStats);

        // The two physical sub-channels (aliases) are distinct
        Assert.NotEqual(planA.InputChannel!.Value.Alias, planB.InputChannel!.Value.Alias);
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
        ChannelInjectionPlan? joinedPlan = null;

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (b.Alias == "joined") joinedPlan = ctx.ChannelInjection;
            return Task.FromResult(0);
        });

        Assert.NotNull(joinedPlan);
        Assert.True(joinedPlan!.OutputChannel.HasValue);
        Assert.Equal(ChannelMode.Arrow, joinedPlan.OutputChannel!.Value.Mode);
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
        var plans = new ConcurrentDictionary<string, ChannelInjectionPlan?>(StringComparer.OrdinalIgnoreCase);

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            plans[b.Alias] = ctx.ChannelInjection;
            return Task.FromResult(0);
        });

        // Both sources feed a stream-transformer → Arrow
        Assert.Equal(ChannelMode.Arrow, plans["stream_a"]?.OutputChannel?.Mode);
        Assert.Equal(ChannelMode.Arrow, plans["stream_b"]?.OutputChannel?.Mode);

        // The merge processor has an explicit -o → no injection
        Assert.Null(plans["merged"]);
    }
}
