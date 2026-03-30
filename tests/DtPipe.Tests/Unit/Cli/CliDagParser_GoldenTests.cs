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
        Assert.Equal(golden.Branches[1].StreamingAliases, dag.Branches[1].StreamingAliases);
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
        Assert.Equal(golden.Branches[1].StreamingAliases[0], dag.Branches[1].StreamingAliases[0]);
        Assert.Equal(golden.Branches[2].StreamingAliases[0], dag.Branches[2].StreamingAliases[0]);
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

        Assert.Equal(golden.Branches[1].Alias,        dag.Branches[1].Alias);
        Assert.Equal(golden.Branches[1].StreamingAliases[0], dag.Branches[1].StreamingAliases[0]);
        Assert.False(dag.Branches[1].HasStreamTransformer);

        Assert.Equal(golden.Branches[2].Alias,         dag.Branches[2].Alias);
        Assert.Equal(golden.Branches[2].StreamingAliases[0], dag.Branches[2].StreamingAliases[0]);
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
        Assert.Equal(golden.Branches[2].StreamingAliases, dag.Branches[2].StreamingAliases);
        Assert.Equal(golden.Branches[2].RefAliases,       dag.Branches[2].RefAliases);
        Assert.True(dag.Branches[2].HasStreamTransformer);
    }

    [Fact]
    public void Parse_SqlJoin_AutoAliasDoesNotCollideWithExplicitStreamNAliases()
    {
        // Regression: branch counter used to start at 0 and increment for every branch
        // (including those with explicit --alias), causing the SQL branch to receive
        // alias "stream2" which collided with the explicit alias of the second source branch.
        var args = new[]
        {
            "--input", "generate:10", "--alias", "stream1",
            "--input", "generate:20", "--alias", "stream2",
            "--from", "stream1", "--ref", "stream2",
            "--sql", "SELECT * FROM stream1 JOIN stream2 ON id",
            "-o", "csv:/tmp/result.csv"
        };
        var dag = CliDagParser.Parse(args);

        Assert.Equal(3, dag.Branches.Count);
        Assert.Equal("stream1", dag.Branches[0].Alias);
        Assert.Equal("stream2", dag.Branches[1].Alias);

        // The SQL branch must NOT be aliased "stream2" (collision with branch[1]).
        var sqlBranch = dag.Branches[2];
        Assert.NotEqual("stream1", sqlBranch.Alias);
        Assert.NotEqual("stream2", sqlBranch.Alias);
        Assert.True(sqlBranch.HasStreamTransformer);
        Assert.Equal("stream1", sqlBranch.StreamingAliases[0]);
        Assert.Equal("stream2", sqlBranch.RefAliases[0]);
    }

    [Fact]
    public void Parse_MergeProcessor_MatchesGoldenMerge()
    {
        // --from a,b --merge declares a UNION ALL merge processor.
        var args = new[]
        {
            "-i", "generate:5", "--alias", "stream_a",
            "-i", "generate:5", "--alias", "stream_b",
            "--from", "stream_a,stream_b", "--merge",
            "--alias", "merged", "-o", "csv:/tmp/merged.csv"
        };
        var dag = CliDagParser.Parse(args);
        var golden = GoldenDagDefinitions.Dag_Merge_TwoSources;

        Assert.Equal(3, dag.Branches.Count);

        var mergeBranch = dag.Branches[2];
        Assert.Equal(golden.Branches[2].StreamingAliases, mergeBranch.StreamingAliases);
        Assert.Equal("merge", mergeBranch.ProcessorName);
        Assert.True(mergeBranch.HasStreamTransformer);
    }
}
