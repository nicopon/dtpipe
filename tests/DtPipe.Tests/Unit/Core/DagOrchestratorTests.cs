using System.Collections.Concurrent;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// Tests unitaires du moteur DagOrchestrator — sans CLI, sans intégration.
/// Chaque test construit un JobDagDefinition en C# et passe un branchExecutor fictif.
/// Obligation architecturale : ces tests doivent passer avant tout commit sur DagOrchestrator.
/// </summary>
public class DagOrchestratorTests
{
    private static DagOrchestrator BuildOrchestrator() => new(
        NullLogger<DagOrchestrator>.Instance,
        new MemoryChannelRegistry(),
        processorFactories: [],
        readerFactories: []);

    // ─────────────────────────────────────────────────────────────────────────
    // Cas 1 : Pipeline linéaire — branche unique, pas de canal mémoire
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
    public async Task Execute_SingleBranch_ContextHasNoInputOrOutputChannel()
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
        Assert.Null(captured!.InputChannelAlias);
        Assert.Null(captured.OutputChannelAlias); // Output = "csv:/tmp/out.csv" → pas de canal mémoire
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cas 2 : DAG deux branches indépendantes
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
    // Cas 3 : DAG avec processor SQL
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Execute_SqlProcessor_ProcessorBranchReceivesInputChannelAlias()
    {
        var dag = GoldenDagDefinitions.Dag_SourcePlusSqlProcessor;
        var orchestrator = BuildOrchestrator();
        BranchChannelContext? processorCtx = null;

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (b.IsProcessor) processorCtx = ctx;
            return Task.FromResult(0);
        });

        Assert.NotNull(processorCtx);
        Assert.Equal("src", processorCtx!.InputChannelAlias);
    }

    [Fact]
    public async Task Execute_SqlProcessor_SourceBranchHasOutputChannelAlias()
    {
        var dag = GoldenDagDefinitions.Dag_SourcePlusSqlProcessor;
        var orchestrator = BuildOrchestrator();
        BranchChannelContext? sourceCtx = null;

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (!b.IsProcessor) sourceCtx = ctx;
            return Task.FromResult(0);
        });

        Assert.NotNull(sourceCtx);
        // Source has no external output → canal mémoire automatiquement alloué
        Assert.Equal("src", sourceCtx!.OutputChannelAlias);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cas 4 : Fan-out (tee) — une source, deux consommateurs
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
    public async Task Execute_FanOut_ConsumersReceiveDistinctPhysicalInputAliases()
    {
        var dag = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers;
        var orchestrator = BuildOrchestrator();
        var inputAliases = new ConcurrentDictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            inputAliases[b.Alias] = ctx.InputChannelAlias;
            return Task.FromResult(0);
        });

        // La source n'a pas de canal amont
        Assert.Null(inputAliases.GetValueOrDefault("src"));

        // Les deux consommateurs ont un canal d'entrée distinct (sous-canaux broadcast)
        var aliasA = inputAliases.GetValueOrDefault("consumer_a");
        var aliasB = inputAliases.GetValueOrDefault("consumer_b");
        Assert.NotNull(aliasA);
        Assert.NotNull(aliasB);
        Assert.NotEqual(aliasA, aliasB);
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

        // La source n'a pas de remapping
        Assert.Empty(aliasMaps["src"]);

        // Les consommateurs reçoivent un AliasMap non vide (logique → physique)
        var mapA = aliasMaps.GetValueOrDefault("consumer_a");
        var mapB = aliasMaps.GetValueOrDefault("consumer_b");
        Assert.NotNull(mapA);
        Assert.NotNull(mapB);
        Assert.NotEmpty(mapA!);
        Assert.NotEmpty(mapB!);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Propagation d'erreurs
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
}
