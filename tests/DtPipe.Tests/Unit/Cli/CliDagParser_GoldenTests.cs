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
        Assert.False(dag.Branches[0].HasStreamTransformer);
    }

    [Fact]
    public void Parse_SqlProcessorArgs_MatchesGoldenSqlProcessor()
    {
        // New syntax: --from <alias> --sql "<query>"
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
        Assert.True(dag.Branches[1].HasStreamTransformer);
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

    [Fact]
    public void Parse_FanOut_WithSqlProcessor_MatchesGolden()
    {
        // src is consumed by sink_a (fan-out) and by result (SQL transformer via --from).
        var args = new[]
        {
            "-i", "generate:50", "--alias", "src",
            "--from", "src", "--alias", "sink_a", "-o", "csv:/tmp/sink_a.csv",
            "--from", "src", "--alias", "result", "--sql", "SELECT * FROM src", "-o", "csv:/tmp/result.csv"
        };
        var dag = CliDagParser.Parse(args);
        var golden = GoldenDagDefinitions.Dag_FanOut_WithSqlProcessor;

        Assert.Equal(3, dag.Branches.Count);

        Assert.Equal(golden.Branches[0].Alias, dag.Branches[0].Alias);
        Assert.False(dag.Branches[0].HasStreamTransformer);

        Assert.Equal(golden.Branches[1].Alias,    dag.Branches[1].Alias);
        Assert.Equal(golden.Branches[1].FromAlias, dag.Branches[1].FromAlias);
        Assert.False(dag.Branches[1].HasStreamTransformer);

        Assert.Equal(golden.Branches[2].Alias,     dag.Branches[2].Alias);
        Assert.Equal(golden.Branches[2].FromAlias,  dag.Branches[2].FromAlias);
        Assert.True(dag.Branches[2].HasStreamTransformer);
    }

    [Fact]
    public void Parse_SqlProcessorWithRef_MatchesGolden()
    {
        // --from = main streaming source, --ref = materialized reference.
        var args = new[]
        {
            "-i", "generate:100", "--alias", "main_stream",
            "-i", "generate:10",  "--alias", "ref_data",
            "--from", "main_stream", "--ref", "ref_data",
            "--sql", "SELECT m.* FROM main_stream m JOIN ref_data r ON m.id = r.id",
            "--alias", "result", "-o", "csv:/tmp/result.csv"
        };
        var dag = CliDagParser.Parse(args);
        var golden = GoldenDagDefinitions.Dag_SqlProcessor_WithRef;

        Assert.Equal(3, dag.Branches.Count);
        Assert.Equal(golden.Branches[2].FromAlias,  dag.Branches[2].FromAlias);
        Assert.Equal(golden.Branches[2].RefAliases, dag.Branches[2].RefAliases);
        Assert.True(dag.Branches[2].HasStreamTransformer);
    }
}
