using System;
using System.Collections.Generic;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Pipelines.Dag;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

/// <summary>
/// <see cref="BranchDefinition.FromJob"/> is the single projection every path uses to turn a
/// hydrated job into a branch. These fixtures pin what the four hand-written copies disagreed
/// about: the alias list, the ref list, how the processor is named, and whether the job travels
/// with the branch at all.
/// </summary>
public class BranchProjectionTests
{
    private static JobDefinition Job(string? from = null, string[]? refs = null) => new()
    {
        Input = "csv:in.csv",
        Output = "csv:out.csv",
        From = from,
        Ref = refs ?? Array.Empty<string>()
    };

    [Fact]
    public void FromJob_Carries_Alias_And_Endpoints()
    {
        var job = Job();

        var branch = BranchDefinition.FromJob("main", job);

        Assert.Equal("main", branch.Alias);
        Assert.Equal("csv:in.csv", branch.Input);
        Assert.Equal("csv:out.csv", branch.Output);
        Assert.Empty(branch.Arguments);
        Assert.Null(branch.ProcessorName);
        Assert.False(branch.HasStreamTransformer);
    }

    /// <summary>
    /// DagRenderer names a branch's stages from <see cref="BranchDefinition.PreParsedJob"/> and
    /// <c>get-dag-topology</c> emits them from it. Two of the four copies left it null, so the
    /// panel the agent drew and the panel the CLI drew for the same job disagreed about whether
    /// the branch had any stages at all.
    /// </summary>
    [Fact]
    public void FromJob_Travels_With_The_Job_It_Was_Built_From()
    {
        var job = Job();

        var branch = BranchDefinition.FromJob("main", job);

        Assert.Same(job, branch.PreParsedJob);
    }

    [Theory]
    [InlineData("a", new[] { "a" })]
    [InlineData("a,b", new[] { "a", "b" })]
    [InlineData(" a , b ", new[] { "a", "b" })]
    [InlineData("a,,b", new[] { "a", "b" })]
    public void FromJob_Splits_The_Comma_Separated_Alias_List(string from, string[] expected)
    {
        var branch = BranchDefinition.FromJob("main", Job(from));

        Assert.Equal(expected, branch.StreamingAliases);
    }

    /// <summary>No upstream is the absence of the key, not one empty alias.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void FromJob_Without_From_Has_No_Upstream(string? from)
    {
        var branch = BranchDefinition.FromJob("main", Job(from));

        Assert.Empty(branch.StreamingAliases);
    }

    [Fact]
    public void FromJob_Carries_Ref_Aliases_And_Defaults_Them_To_Empty()
    {
        Assert.Equal(new[] { "lookup" }, BranchDefinition.FromJob("m", Job(refs: new[] { "lookup" })).RefAliases);
        Assert.Empty(BranchDefinition.FromJob("m", Job()).RefAliases);
    }

    [Fact]
    public void FromJob_Names_The_Processor_The_Job_Selects()
    {
        var job = Job("src") with { ProviderOptions = new() { ["merge"] = new Dictionary<string, object?>() } };

        var branch = BranchDefinition.FromJob("m", job, new[] { new FakeProcessorFactory("merge") });

        Assert.Equal("merge", branch.ProcessorName);
        Assert.True(branch.HasStreamTransformer);
    }

    /// <summary>
    /// The CLI arguments path resolves the processor from the raw tokens, before the job carries
    /// the provider options the YAML paths match on. What it resolved wins over the catalogue.
    /// </summary>
    [Fact]
    public void FromJob_Prefers_An_Explicitly_Resolved_Processor()
    {
        var job = Job("src") with { ProviderOptions = new() { ["merge"] = new Dictionary<string, object?>() } };

        var branch = BranchDefinition.FromJob(
            "m", job, new[] { new FakeProcessorFactory("merge") }, processorName: "sql");

        Assert.Equal("sql", branch.ProcessorName);
    }

    [Fact]
    public void FromJob_Carries_The_Cli_Slice_That_Defined_The_Branch()
    {
        var args = new[] { "--from", "src", "--sql", "SELECT 1" };

        var branch = BranchDefinition.FromJob("m", Job("src"), arguments: args);

        Assert.Same(args, branch.Arguments);
    }

    private sealed class FakeProcessorFactory : IStreamTransformerFactory
    {
        public FakeProcessorFactory(string name) => ComponentName = name;

        public string ComponentName { get; }
        public string Category => "Stream Processors";
        public bool RequiresArrowChannels => true;
        public int MinStreams => 1;
        public int MaxStreams => -1;
        public int MinLookups => 0;
        public int MaxLookups => -1;
        public IReadOnlyList<(string Flag, bool IsBoolean)> CliTriggerFlags => Array.Empty<(string, bool)>();

        public bool IsApplicable(string[] branchArgs) => false;
        public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider sp)
            => throw new NotSupportedException();
        public IStreamTransformer CreateFromJob(JobDefinition job, BranchChannelContext ctx, IServiceProvider sp)
            => throw new NotSupportedException();
    }
}
