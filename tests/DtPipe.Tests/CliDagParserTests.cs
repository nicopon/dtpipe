using DtPipe.Cli.Dag;
using Xunit;

namespace DtPipe.Tests;

public class CliDagParserTests
{
    [Fact]
    public void Parse_LinearPipeline_ReturnsSingleBranch()
    {
        var args = new[] { "-i", "data.csv", "--filter", "v>0", "-o", "out.csv" };
        var dag = CliDagParser.Parse(args);

        Assert.Single(dag.Branches);
        Assert.False(dag.IsDag);
        Assert.Equal("stream0", dag.Branches[0].Alias);
        Assert.False(dag.Branches[0].IsXStreamer);
        Assert.Equal(args, dag.Branches[0].Arguments);
    }

    [Fact]
    public void Parse_DagWithTwoBranches_AssignsAliasesAndXStreamerFlag()
    {
        var args = new[] {
            "-i", "data1.csv", "--alias", "input_one",
            "-x", "duck", "-q", "SELECT * FROM input_one", "-o", "out.csv"
        };

        var dag = CliDagParser.Parse(args);

        Assert.True(dag.IsDag);
        Assert.Equal(2, dag.Branches.Count);

        var branch1 = dag.Branches[0];
        Assert.Equal("input_one", branch1.Alias);
        Assert.False(branch1.IsXStreamer);
        Assert.Equal(new[] { "-i", "data1.csv", "--alias", "input_one" }, branch1.Arguments);

        var branch2 = dag.Branches[1];
        Assert.Equal("stream1", branch2.Alias); // Default alias
        Assert.True(branch2.IsXStreamer);
        Assert.Equal(new[] { "-x", "duck", "-q", "SELECT * FROM input_one", "-o", "out.csv" }, branch2.Arguments);
    }

    [Fact]
    public void Validate_DuplicateSingleton_ReturnsError()
    {
        var args = new[] { "-i", "data.csv", "--limit", "10", "--limit", "20" };
        var dag = CliDagParser.Parse(args);
        var errors = CliDagParser.Validate(dag);

        Assert.Contains(errors, e => e.Contains("multiple instances of singleton flag '--limit'"));
    }

    [Fact]
    public void Validate_SourceBranchWithOutputFedIntoXStreamer_ReturnsError()
    {
        var args = new[] {
            "-i", "data1.csv", "--alias", "input_one", "-o", "wrong.csv",
            "-x", "duck", "--main", "input_one", "-o", "out.csv"
        };

        var dag = CliDagParser.Parse(args);
        var errors = CliDagParser.Validate(dag);

        Assert.Contains(errors, e => e.Contains("used as an input for a downstream branch and cannot have its own '--output'"));
    }
}
