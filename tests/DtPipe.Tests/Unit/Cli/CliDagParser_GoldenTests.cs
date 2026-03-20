using DtPipe.Cli.Dag;
using DtPipe.Tests.Unit.Core;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

public class CliDagParser_GoldenTests
{
    [Fact]
    public void Parse_LinearArgs_MatchesGoldenLinear()
    {
        var args = new[] { "-i", "generate:10", "-o", "csv:/tmp/out.csv", "--alias", "main" };
        var dag = CliDagParser.Parse(args);
        var golden = GoldenDagDefinitions.Linear_SingleBranch;

        Assert.Single(dag.Branches);
        Assert.Equal(golden.Branches[0].Alias, dag.Branches[0].Alias);
        Assert.Equal(golden.Branches[0].Input,  dag.Branches[0].Input);
        Assert.Equal(golden.Branches[0].Output, dag.Branches[0].Output);
        Assert.False(dag.Branches[0].IsProcessor);
    }

    [Fact]
    public void Parse_SqlProcessorArgs_MatchesGoldenSqlProcessor()
    {
        var args = new[]
        {
            "-i", "generate:100", "--alias", "src",
            "--from", "src", "--sql", "SELECT * FROM src LIMIT 10",
            "--alias", "processed", "-o", "csv:/tmp/processed.csv"
        };
        var dag = CliDagParser.Parse(args);
        var golden = GoldenDagDefinitions.Dag_SourcePlusSqlProcessor;

        Assert.Equal(2, dag.Branches.Count);
        Assert.Equal(golden.Branches[1].FromAlias, dag.Branches[1].FromAlias);
        Assert.Equal(golden.Branches[1].SqlQuery, dag.Branches[1].SqlQuery);
        Assert.True(dag.Branches[1].IsProcessor);
    }

    [Fact]
    public void Parse_FanOut_MatchesGoldenFanOut()
    {
        var args = new[]
        {
            "-i", "generate:50", "--alias", "src",
            "--from", "src", "--alias", "consumer_a", "-o", "csv:/tmp/consumer_a.csv",
            "--from", "src", "--alias", "consumer_b", "-o", "csv:/tmp/consumer_b.csv"
        };
        var dag = CliDagParser.Parse(args);
        var golden = GoldenDagDefinitions.Dag_FanOut_OneSourceTwoConsumers;

        Assert.Equal(3, dag.Branches.Count);
        Assert.Equal(golden.Branches[1].FromAlias, dag.Branches[1].FromAlias);
        Assert.Equal(golden.Branches[2].FromAlias, dag.Branches[2].FromAlias);
    }
}
