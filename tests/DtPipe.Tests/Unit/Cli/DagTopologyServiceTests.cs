using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines;
using DtPipe.Core.Pipelines.Dag;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// Voie 4 §6 (suite 2) lot E4: <see cref="DagTopologyService"/> is the one place a job file becomes
/// a DAG — the MCP validate/execute path, the Spectre topology box, the <c>get-dag-topology</c> tool
/// and the plan panel all read it from here. These fixtures pin the structured view: alias, input,
/// output, processor, from[], ref[] per branch, including multi-branch <c>--from</c>/<c>--ref</c>.
/// </summary>
public class DagTopologyServiceTests
{
    private static DagTopologyService Service(params IStreamTransformerFactory[] processors) =>
        new(processors);

    [Fact]
    public void A_Single_Branch_Job_Has_One_Branch_With_Its_Endpoints()
    {
        var topo = Service().Describe("""
            main:
              input: "csv:in.csv"
              output: "csv:out.csv"
            """);

        var branch = Assert.Single(topo.Branches);
        Assert.Equal("main", branch.Alias);
        Assert.Equal("csv:in.csv", branch.Input);
        Assert.Equal("csv:out.csv", branch.Output);
        Assert.Null(branch.Processor);
        Assert.Empty(branch.From);
        Assert.Empty(branch.Ref);
    }

    [Fact]
    public void A_Multi_Branch_Dag_Carries_From_And_Ref_Per_Branch()
    {
        var topo = Service(new FakeSqlFactory()).Describe("""
            p:
              input: "parquet:p.parquet"
            c:
              input: "csv:c.csv"
            joined:
              from: p
              ref: [c]
              provider-options:
                sql:
                  query: "SELECT * FROM p JOIN c ON p.id = c.id"
              output: "pg:out"
            """);

        Assert.Equal(new[] { "p", "c", "joined" }, topo.Branches.Select(b => b.Alias));

        var joined = topo.Branches.Single(b => b.Alias == "joined");
        Assert.Equal(new[] { "p" }, joined.From);
        Assert.Equal(new[] { "c" }, joined.Ref);
        Assert.Equal("sql", joined.Processor);
        Assert.Null(joined.Input);
        Assert.Equal("pg:out", joined.Output);

        var p = topo.Branches.Single(b => b.Alias == "p");
        Assert.Equal("parquet:p.parquet", p.Input);
        Assert.Empty(p.From);
    }

    [Fact]
    public void A_Comma_Separated_From_Splits_Into_Several_Aliases()
    {
        var topo = Service(new FakeSqlFactory("merge")).Describe("""
            a:
              input: "csv:a.csv"
            b:
              input: "csv:b.csv"
            merged:
              from: "a, b"
              provider-options:
                merge: {}
              output: "csv:out.csv"
            """);

        var merged = topo.Branches.Single(b => b.Alias == "merged");
        Assert.Equal(new[] { "a", "b" }, merged.From);
        Assert.Equal("merge", merged.Processor);
    }

    [Fact]
    public void Build_Exposes_Both_The_Jobs_And_The_Dag()
    {
        var build = Service().Build("""
            main:
              input: "csv:in.csv"
              output: "csv:out.csv"
            """);

        Assert.True(build.Jobs.ContainsKey("main"));
        Assert.Equal("csv:in.csv", build.Jobs["main"].Input);
        var branch = Assert.Single(build.Dag.Branches);
        // PreParsedJob is set so DagRenderer can reach the branch's transformers.
        Assert.Same(build.Jobs["main"], branch.PreParsedJob);
    }

    [Fact]
    public void TryDescribe_Folds_Malformed_Yaml_Into_Null()
    {
        Assert.Null(Service().TryDescribe("this: is: not: valid: yaml:"));
        Assert.Null(Service().TryDescribe("   "));
        Assert.Null(Service().TryDescribe(null));
    }

    [Fact]
    public void Describe_Propagates_A_Parse_Failure()
    {
        Assert.ThrowsAny<Exception>(() => Service().Describe("this: is: not: valid: yaml:"));
    }

    /// <summary>A stand-in stream processor: applicable when the YAML job carries its provider-options key.</summary>
    private sealed class FakeSqlFactory : IStreamTransformerFactory
    {
        public FakeSqlFactory(string name = "sql") => ComponentName = name;
        public string ComponentName { get; }
        public string Category => "Test";
        public bool RequiresArrowChannels => false;
        public int MinStreams => 0;
        public int MaxStreams => -1;
        public int MinLookups => 0;
        public int MaxLookups => -1;
        public IReadOnlyList<(string Flag, bool IsBoolean)> CliTriggerFlags => Array.Empty<(string, bool)>();
        public bool IsApplicable(string[] branchArgs) => false;
        public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider serviceProvider)
            => throw new NotSupportedException();
        public IStreamTransformer CreateFromJob(JobDefinition job, BranchChannelContext ctx, IServiceProvider serviceProvider)
            => throw new NotSupportedException();
    }
}
