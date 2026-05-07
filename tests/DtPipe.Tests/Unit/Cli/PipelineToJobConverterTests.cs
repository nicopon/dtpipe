using DtPipe.Cli.Pipeline;
using Xunit;
using System.Collections.Generic;
using System.Linq;

namespace DtPipe.Tests.Unit.Cli;

public class PipelineToJobConverterTests
{
    [Fact]
    public void Convert_LinearSimple_ReturnsOneJob()
    {
        var globals = new GlobalOptions { AllFlags = new Dictionary<string, object?>(System.StringComparer.OrdinalIgnoreCase) { { "--batch-size", "1000" } } };
        var branch = new BranchSpec { Input = "gen:10", Output = "out.csv" };
        var parsed = new ParsedPipeline(globals, new[] { branch });

        var (jobs, dag) = PipelineToJobConverter.Convert(parsed);

        Assert.Single(jobs);
        var job = jobs.Values.First();
        Assert.Equal("gen:10", job.Input);
        Assert.Equal("out.csv", job.Output);
        Assert.Equal(1000, job.BatchSize);
        Assert.Single(dag.Branches);
    }

    [Fact]
    public void Convert_TwoBranches_ReturnsTwoJobsAndDag()
    {
        var globals = new GlobalOptions();
        var b1 = new BranchSpec { Alias = "a", Input = "in1.csv" };
        var b2 = new BranchSpec { Alias = "b", Input = "in2.csv" };
        var parsed = new ParsedPipeline(globals, new[] { b1, b2 });

        var (jobs, dag) = PipelineToJobConverter.Convert(parsed);

        Assert.Equal(2, jobs.Count);
        Assert.Equal(2, dag.Branches.Count);
        Assert.Contains("a", jobs.Keys);
        Assert.Contains("b", jobs.Keys);
    }

    [Fact]
    public void Convert_InheritsGlobals()
    {
        var globals = new GlobalOptions { AllFlags = new Dictionary<string, object?>(System.StringComparer.OrdinalIgnoreCase) { { "--limit", "100" } } };
        var b1 = new BranchSpec { Input = "in1.csv" };
        var parsed = new ParsedPipeline(globals, new[] { b1 });

        var (jobs, _) = PipelineToJobConverter.Convert(parsed);

        var job = jobs.Values.First();
        Assert.Equal(100, job.Limit);
    }

    [Fact]
    public void Convert_BranchOverridesGlobal()
    {
        var globals = new GlobalOptions { AllFlags = new Dictionary<string, object?>(System.StringComparer.OrdinalIgnoreCase) { { "--limit", "100" } } };
        var branchFlags = new Dictionary<string, List<string>>(System.StringComparer.OrdinalIgnoreCase) { { "--limit", new List<string> { "50" } } };
        var b1 = new BranchSpec { Input = "in1.csv", Flags = branchFlags };
        var parsed = new ParsedPipeline(globals, new[] { b1 });

        var (jobs, _) = PipelineToJobConverter.Convert(parsed);

        var job = jobs.Values.First();
        Assert.Equal(50, job.Limit);
    }

    [Fact]
    public void Convert_AutoAliasGeneration()
    {
        var globals = new GlobalOptions();
        var b1 = new BranchSpec { Input = "in1.csv" }; // branch1
        var b2 = new BranchSpec { From = new List<string> { "branch1" } }; // stream2
        var parsed = new ParsedPipeline(globals, new[] { b1, b2 });

        var (jobs, dag) = PipelineToJobConverter.Convert(parsed);

        Assert.Contains("stream1", jobs.Keys);
        Assert.Contains("stream2", jobs.Keys);
        Assert.Equal("stream1", dag.Branches[0].Alias);
        Assert.Equal("stream2", dag.Branches[1].Alias);
    }
}
