using DtPipe.Cli.Studio;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using DtPipe.Jobs;
using Xunit;
using YamlDotNet.RepresentationModel;

namespace DtPipe.Tests.Unit.Jobs;

/// <summary>
/// A branch's own reader and writer as cards, for <c>dtpipe studio</c>: a hand-written job opens as
/// source, transform and sink cards, and composes back into the branch it was.
/// </summary>
public sealed class LocalEndpointCardTests
{
    private static readonly DesignService Design = new(
        IBrickLookup.None,
        new ComponentEndpointRules([new Reader("csv", ".csv"), new Reader("duck", ".duckdb")], [new Writer("csv", ".csv"), new Writer("duck", ".duckdb")]));

    private const string Linear = """
        main:
          input: csv:people.csv
          limit: 5
          output: duck:archive.duckdb
          transformers:
          - type: overwrite
            mappings:
              email: hidden
          provider-options:
            csv:
              separator: ;
            duck-writer:
              table: people
              strategy: Recreate
          prefix: run1
        """;

    private static string Cards(DesignResult result) =>
        string.Join(" | ", result.Model.Steps.Select(s => $"{s.Alias}:{s.Kind}<{string.Join(',', s.From ?? [])}>"));

    /// <summary>Two branches dtpipe reads the same way, whatever the key order.</summary>
    private static void SameJob(string expected, string actual)
    {
        static string Tree(YamlNode node) => node switch
        {
            YamlMappingNode m => "{" + string.Join(",", m.Children.OrderBy(kv => ((YamlScalarNode)kv.Key).Value, StringComparer.Ordinal)
                .Select(kv => $"{((YamlScalarNode)kv.Key).Value}:{Tree(kv.Value)}")) + "}",
            YamlSequenceNode s => "[" + string.Join(",", s.Children.Select(Tree)) + "]",
            YamlScalarNode s => s.Value ?? "~",
            _ => "?",
        };
        Assert.Equal(Tree(JobYaml.Parse(expected)), Tree(JobYaml.Parse(actual)));
    }

    [Fact]
    public void A_Linear_Branch_Opens_As_Source_Transform_And_Sink()
    {
        var result = Design.Decompose(Linear);

        Assert.Empty(result.Errors);
        Assert.Equal("main_source:source<> | main:transform<main_source> | main_sink:sink<main>", Cards(result));
        var source = result.Model.Steps[0];
        Assert.Null(source.Brick);
        SameJob("b:\n  input: csv:people.csv\n  limit: 5\n  provider-options:\n    csv:\n      separator: ;\n",
            "b:\n" + string.Join('\n', source.Yaml!.TrimEnd().Split('\n').Select(l => "  " + l)));
        var sink = result.Model.Steps[2];
        SameJob("b:\n  output: duck:archive.duckdb\n  prefix: run1\n  provider-options:\n    duck-writer:\n      table: people\n      strategy: Recreate\n",
            "b:\n" + string.Join('\n', sink.Yaml!.TrimEnd().Split('\n').Select(l => "  " + l)));
    }

    [Fact]
    public void Its_Cards_Compose_Back_Into_The_Branch_It_Was()
    {
        var result = Design.Decompose(Linear);

        SameJob(Linear, result.Yaml);
        Assert.Single(JobYaml.Branches(JobYaml.Parse(result.Yaml)));
        Assert.All(result.Steps, s => Assert.Equal("main", s.Branch));
    }

    [Fact]
    public void A_Copy_With_No_Work_Opens_As_Source_And_Sink_And_Joins_Back()
    {
        const string job = "copy:\n  input: csv:a.csv\n  output: duck:b.duckdb\n";

        var result = Design.Decompose(job);

        Assert.Equal("copy:source<> | copy_sink:sink<copy>", Cards(result));
        SameJob(job, result.Yaml);
    }

    [Fact]
    public void A_Source_Read_By_Two_Steps_Stays_Its_Own_Branch()
    {
        const string job = """
            people:
              input: csv:people.csv
            left:
              from: people
              output: csv:left.csv
            right:
              from: people
              output: csv:right.csv
            """;

        var result = Design.Decompose(job);

        Assert.Equal("people:source<> | left:sink<people> | right:sink<people>", Cards(result));
        SameJob(job, result.Yaml);
        Assert.Equal(3, JobYaml.Branches(JobYaml.Parse(result.Yaml)).Count());
    }

    [Theory]
    [InlineData("  batch-size: 100\n", "an engine setting no side owns")]
    [InlineData("  metrics-path: m.json\n", "a path that describes the whole run")]
    [InlineData("  provider-options:\n    mystery:\n      x: 1\n", "a block neither component owns")]
    public void A_Branch_With_Something_No_Card_Can_Hold_Stays_As_Written(string extra, string why)
    {
        var job = "main:\n  input: csv:a.csv\n  output: csv:b.csv\n" + extra;

        var result = Design.Decompose(job);

        Assert.True(result.Model.Steps is [{ Kind: StepKinds.Branch }], why);
        SameJob(job, result.Yaml);
    }

    [Fact]
    public void A_Plain_Block_Both_Sides_Answer_To_Is_Not_Guessed_At()
    {
        const string job = "main:\n  input: csv:a.csv\n  output: csv:b.csv\n  provider-options:\n    csv:\n      separator: ;\n";

        Assert.True(Design.Decompose(job).Model.Steps is [{ Kind: StepKinds.Branch }]);
    }

    [Fact]
    public void Cards_Drawn_By_Hand_Join_Into_One_Branch()
    {
        var model = new DesignModel("Hand", null, null,
        [
            new DesignStep("read", StepKinds.Source, Yaml: "input: csv:in.csv\n"),
            new DesignStep("write", StepKinds.Sink, From: ["read"], Yaml: "output: csv:out.csv\n"),
        ]);

        var result = Design.Compose(model);

        Assert.Empty(result.Errors);
        SameJob("read:\n  input: csv:in.csv\n  output: csv:out.csv\n", result.Yaml);
        Assert.Equal(2, result.Steps.Count);
    }

    [Theory]
    [InlineData(StepKinds.Source, "output: csv:x.csv\n", "names no input")]
    [InlineData(StepKinds.Source, "input: csv:x.csv\ntransformers: []\n", "transformers belong to other cards")]
    [InlineData(StepKinds.Sink, "input: csv:x.csv\noutput: csv:y.csv\n", "input belong to other cards")]
    [InlineData(StepKinds.Sink, "", "neither a brick nor an endpoint")]
    public void An_Endpoint_Card_Holds_Only_Its_Own_Side(string kind, string yaml, string expected)
    {
        var steps = kind == StepKinds.Source
            ? new[] { new DesignStep("e", kind, Yaml: yaml), new DesignStep("w", StepKinds.Sink, From: ["e"], Yaml: "output: csv:o.csv\n") }
            : [new DesignStep("r", StepKinds.Source, Yaml: "input: csv:i.csv\n"), new DesignStep("e", kind, From: ["r"], Yaml: yaml)];

        var result = Design.Compose(new DesignModel("T", null, null, steps));

        Assert.Contains(result.Errors, e => e.Contains(expected));
    }

    private sealed class Reader(string name, string extension) : IStreamReaderFactory
    {
        public string ComponentName => name;
        public string Category => "Readers";
        public Type OptionsType => typeof(DtPipe.Adapters.Csv.CsvReaderOptions);
        public bool CanHandle(string connectionString) => connectionString.EndsWith(extension, StringComparison.OrdinalIgnoreCase);
        public IStreamReader Create(OptionsRegistry registry) => throw new NotSupportedException();
        public IEnumerable<Type> GetSupportedOptionTypes() => [OptionsType];
        public bool RequiresQuery => false;
    }

    private sealed class Writer(string name, string extension) : IDataWriterFactory
    {
        public string ComponentName => name;
        public string Category => "Writers";
        public Type OptionsType => typeof(DtPipe.Adapters.Csv.CsvWriterOptions);
        public bool CanHandle(string connectionString) => connectionString.EndsWith(extension, StringComparison.OrdinalIgnoreCase);
        public IDataWriter Create(OptionsRegistry registry) => throw new NotSupportedException();
        public IEnumerable<Type> GetSupportedOptionTypes() => [OptionsType];
    }
}
