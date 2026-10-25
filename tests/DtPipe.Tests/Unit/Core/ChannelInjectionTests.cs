using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Pipelines.Dag;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// Typed channel endpoints. The orchestrator hands branches structured routing;
/// these tests pin the endpoints each golden shape hands its branches.
/// </summary>
public class ChannelInjectionTests
{
    [Fact]
    public void FanPrefix_Constant_Has_Single_Definition()
    {
        // The literal "__fan_" must exist only in IChannelNaming — orchestrator and
        // registry both reference the constant (enforced additionally by a source grep
        // in validate_channel_endpoints.sh).
        Assert.Equal("__fan_", IChannelNaming.FanPrefix);
    }

    [Fact]
    public async Task Typed_Endpoints_Source_To_Sql()
    {
        // The source writes the channel of its own alias; the SQL branch keeps its args.
        var dag = GoldenDagDefinitions.Dag_SourcePlusSqlProcessor;
        var orchestrator = BuildOrchestrator();
        BranchChannelContext? srcCtx = null;

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (b.Alias == "src") srcCtx = ctx;
            return Task.FromResult(0);
        });

        Assert.NotNull(srcCtx?.OutputEndpoint);
        Assert.Equal("src", srcCtx!.OutputEndpoint!.Alias);
        Assert.Equal(InternalChannelKind.Arrow, srcCtx.OutputEndpoint.Kind);
        Assert.Null(srcCtx.InputEndpoint);
        Assert.True(srcCtx.SuppressStats);
    }

    [Fact]
    public async Task Typed_Endpoints_FanOut()
    {
        // Each consumer reads a fan-out sub-channel of its own: "{alias}__fan_{n}".
        var dag = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers;
        var orchestrator = BuildOrchestrator();
        var inputs = new System.Collections.Concurrent.ConcurrentDictionary<string, string>();

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (ctx.InputEndpoint is { } ep) inputs[b.Alias] = ep.Alias;
            return Task.FromResult(0);
        });

        Assert.Equal(2, inputs.Count);
        Assert.NotEqual(inputs["consumer_a"], inputs["consumer_b"]);
        Assert.StartsWith("src" + IChannelNaming.FanPrefix + "0", inputs["consumer_a"]);
        Assert.StartsWith("src" + IChannelNaming.FanPrefix + "1", inputs["consumer_b"]);
    }

    [Fact]
    public async Task Typed_Endpoints_Merge()
    {
        // Each merge source writes the channel of its own alias.
        var dag = GoldenDagDefinitions.Dag_Merge_TwoSources;
        var orchestrator = BuildOrchestrator();
        var outputs = new System.Collections.Concurrent.ConcurrentDictionary<string, string>();

        await orchestrator.ExecuteAsync(dag, (b, ctx, _) =>
        {
            if (ctx.OutputEndpoint is { } ep) outputs[b.Alias] = ep.Alias;
            return Task.FromResult(0);
        });

        Assert.Equal("stream_a", outputs["stream_a"]);
        Assert.Equal("stream_b", outputs["stream_b"]);
    }

    private static DagOrchestrator BuildOrchestrator() => new(
        Microsoft.Extensions.Logging.Abstractions.NullLogger<DagOrchestrator>.Instance,
        new MemoryChannelRegistry());
}
