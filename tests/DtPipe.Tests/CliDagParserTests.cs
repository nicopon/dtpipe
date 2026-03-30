using DtPipe.Cli.Dag;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

public class CliDagParserTests
{
    [Fact]
    public void Parse_LinearPipeline_ReturnsSingleBranch()
    {
        var args = new[] { "-i", "data.csv", "--filter", "v>0", "-o", "out.csv" };
        var dag = CliDagParser.Parse(args);

        Assert.Single(dag.Branches);
        Assert.False(dag.IsDag);
        Assert.Equal("stream1", dag.Branches[0].Alias);
        Assert.False(dag.Branches[0].HasStreamTransformer);
        Assert.Equal(args, dag.Branches[0].Arguments);
    }

    [Fact]
    public void Parse_SqlProcessor_TwoBranches()
    {
        var args = new[]
        {
            "-i", "data1.csv", "--alias", "input_one",
            "--from", "input_one", "--sql", "SELECT * FROM input_one", "-o", "out.csv"
        };

        var dag = CliDagParser.Parse(args);

        Assert.True(dag.IsDag);
        Assert.Equal(2, dag.Branches.Count);

        var branch0 = dag.Branches[0];
        Assert.Equal("input_one", branch0.Alias);
        Assert.False(branch0.HasStreamTransformer);

        var branch1 = dag.Branches[1];
        Assert.True(branch1.HasStreamTransformer);
        Assert.Equal("input_one", branch1.StreamingAliases[0]);
        Assert.Equal("sql", branch1.ProcessorName);
        Assert.Equal("SELECT * FROM input_one", DtPipe.Cli.Dag.CliDagParser.ExtractArgValue(branch1.Arguments, "--sql"));
        Assert.Contains("--from", branch1.Arguments);
        Assert.Contains("--sql", branch1.Arguments);
    }

    [Fact]
    public void Parse_SqlProcessor_DoesNotSplitOnSqlFlag()
    {
        // --sql within a --from branch must NOT create an extra branch.
        var args = new[]
        {
            "-i", "data.csv", "--alias", "src",
            "--from", "src", "--sql", "SELECT * FROM src", "-o", "out.csv"
        };

        var dag = CliDagParser.Parse(args);

        Assert.Equal(2, dag.Branches.Count);
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
    public void Validate_SourceBranchWithOutputFedIntoProcessor_ReturnsError()
    {
        var args = new[]
        {
            "-i", "data1.csv", "--alias", "input_one", "-o", "wrong.csv",
            "--from", "input_one", "--sql", "SELECT * FROM input_one", "-o", "out.csv"
        };

        var dag = CliDagParser.Parse(args);
        var errors = CliDagParser.Validate(dag);

        Assert.Contains(errors, e => e.Contains("used as an input for a downstream branch and cannot have its own '--output'"));
    }
}
