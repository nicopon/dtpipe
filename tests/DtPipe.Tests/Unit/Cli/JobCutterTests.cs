using DtPipe.Cli.Split;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// What <c>dtpipe split --at</c> puts on each side of the cut.
///
/// <para>
/// The stages the cut moves come from a sample run, which needs a binary and lives in
/// <c>validate_split.sh</c>; what is decidable here is where each <i>value</i> of the job lands,
/// and that is the half a wrong answer leaks a credential from.
/// </para>
/// </summary>
public class JobCutterTests
{
    private static readonly IStreamReaderFactory[] Readers = { new StubCsvReaderFactory(), new StubDuckReaderFactory() };
    private static readonly IDataWriterFactory[] Writers = { new StubCsvWriterFactory(), new StubDuckWriterFactory() };

    private static CutResult Cut(Dictionary<string, JobDefinition> jobs, int at, string alias = "main")
        => JobCutter.Cut(jobs, alias, at, Readers, Writers);

    private static Dictionary<string, JobDefinition> Job(JobDefinition job, string alias = "main")
        => new(StringComparer.OrdinalIgnoreCase) { [alias] = job };

    private static TransformerConfig Step(string type) => new() { Type = type };

    // ── The link, and what crosses it ────────────────────────────────────────

    [Fact]
    public void Halves_Meet_On_The_Arrow_Link()
    {
        var result = Cut(Job(new JobDefinition { Input = "csv:in.csv", Output = "csv:out.csv" }), at: 0);

        Assert.Equal(JobCutter.Link, result.Producer["main"].Output);
        Assert.Equal(JobCutter.Link, result.Consumer["main"].Input);
        Assert.Equal("csv:in.csv", result.Producer["main"].Input);
        Assert.Equal("csv:out.csv", result.Consumer["main"].Output);
    }

    [Theory]
    [InlineData(0, 0, 3)]
    [InlineData(1, 1, 2)]
    [InlineData(2, 2, 1)]
    [InlineData(3, 3, 0)]
    public void Cut_At_K_Leaves_K_Transformers_Upstream(int at, int upstream, int downstream)
    {
        var job = new JobDefinition
        {
            Input = "csv:in.csv",
            Output = "csv:out.csv",
            Transformers = new List<TransformerConfig> { Step("Compute"), Step("Fake"), Step("Filter") },
        };

        var result = Cut(Job(job), at);

        Assert.Equal(upstream, result.Producer["main"].Transformers?.Count ?? 0);
        Assert.Equal(downstream, result.Consumer["main"].Transformers?.Count ?? 0);
    }

    [Fact]
    public void Transformers_Keep_Their_Order_On_Both_Sides()
    {
        var job = new JobDefinition
        {
            Input = "csv:in.csv",
            Output = "csv:out.csv",
            Transformers = new List<TransformerConfig> { Step("Compute"), Step("Fake"), Step("Filter") },
        };

        var result = Cut(Job(job), at: 1);

        Assert.Equal(new[] { "Compute" }, result.Producer["main"].Transformers!.Select(t => t.Type));
        Assert.Equal(new[] { "Fake", "Filter" }, result.Consumer["main"].Transformers!.Select(t => t.Type));
    }

    // ── A DAG keeps its other branches ───────────────────────────────────────

    [Fact]
    public void The_Consumer_Carries_The_Branches_The_Cut_Did_Not_Touch()
    {
        var jobs = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase)
        {
            ["a"] = new JobDefinition { Input = "csv:a.csv" },
            ["b"] = new JobDefinition { Input = "csv:b.csv" },
            ["merged"] = new JobDefinition { Output = "csv:out.csv" },
        };

        var result = Cut(jobs, at: 0, alias: "a");

        Assert.Single(result.Producer);
        Assert.Equal(new[] { "a", "b", "merged" }, result.Consumer.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal("csv:b.csv", result.Consumer["b"].Input);
        Assert.Equal(JobCutter.Link, result.Consumer["a"].Input);
    }

    // ── Which side inherits which field ──────────────────────────────────────

    [Fact]
    public void Read_Side_Fields_Go_To_The_Producer_And_Leave_The_Consumer()
    {
        var job = new JobDefinition
        {
            Input = "csv:in.csv",
            Output = "csv:out.csv",
            Cursor = "updated_at",
            State = "state.json",
            Limit = 500,
            SamplingRate = 0.1,
            SamplingSeed = 7,
            FromContract = "contract.yaml",
        };

        var result = Cut(Job(job), at: 0);

        var producer = result.Producer["main"];
        Assert.Equal("updated_at", producer.Cursor);
        Assert.Equal("state.json", producer.State);
        Assert.Equal(500, producer.Limit);
        Assert.Equal(0.1, producer.SamplingRate);
        Assert.Equal(7, producer.SamplingSeed);
        Assert.Equal("contract.yaml", producer.FromContract);

        // The consumer reads a finished stream: nothing here still has a source to bound or resume.
        var consumer = result.Consumer["main"];
        Assert.Null(consumer.Cursor);
        Assert.Null(consumer.State);
        Assert.Equal(0, consumer.Limit);
        Assert.Equal(1.0, consumer.SamplingRate);
        Assert.Null(consumer.SamplingSeed);
        Assert.Null(consumer.FromContract);
    }

    [Fact]
    public void Write_Side_Fields_Go_To_The_Consumer_And_Leave_The_Producer()
    {
        var job = new JobDefinition
        {
            Input = "csv:in.csv",
            Output = "csv:out.csv",
            Prefix = "load_",
            Checkpoint = "ck",
            ContractSave = "contract.yaml",
        };

        var result = Cut(Job(job), at: 0);

        Assert.Equal("load_", result.Consumer["main"].Prefix);
        Assert.Equal("ck", result.Consumer["main"].Checkpoint);
        Assert.Equal("contract.yaml", result.Consumer["main"].ContractSave);

        Assert.Null(result.Producer["main"].Prefix);
        Assert.Null(result.Producer["main"].Checkpoint);
        Assert.Null(result.Producer["main"].ContractSave);
    }

    [Fact]
    public void A_Diagnostic_Path_Reaches_Neither_Half_And_Is_Listed_For_It()
    {
        var job = new JobDefinition
        {
            Input = "csv:in.csv",
            Output = "csv:out.csv",
            MetricsPath = "run.json",
            LogPath = "run.log",
        };

        var result = Cut(Job(job), at: 0);

        Assert.Null(result.Producer["main"].MetricsPath);
        Assert.Null(result.Consumer["main"].MetricsPath);
        Assert.Null(result.Producer["main"].LogPath);
        Assert.Null(result.Consumer["main"].LogPath);

        var dropped = result.Placements.Where(p => p.Side == CutSide.Dropped).Select(p => p.Key).ToList();
        Assert.Contains("metrics-path", dropped);
        Assert.Contains("log-path", dropped);
    }

    // ── Provider options follow the component that owns them ─────────────────

    [Fact]
    public void Each_Provider_Options_Block_Follows_Its_Own_Side()
    {
        var job = new JobDefinition
        {
            Input = "duck:src.duckdb",
            Output = "csv:out.csv",
            ProviderOptions = new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase)
            {
                ["duck"] = new() { ["init-sql"] = "INSTALL httpfs" },
                ["csv"] = new() { ["delimiter"] = ";" },
            },
        };

        var result = Cut(Job(job), at: 0);

        Assert.Empty(result.UnplaceableOptionBlocks);
        Assert.Equal(new[] { "duck" }, result.Producer["main"].ProviderOptions!.Keys);
        Assert.Equal(new[] { "csv" }, result.Consumer["main"].ProviderOptions!.Keys);
    }

    [Fact]
    public void Suffixed_Blocks_Are_Placed_When_Both_Sides_Are_The_Same_Component()
    {
        // csv: to csv: is exactly when the exporter suffixes both entries, because a plain "csv"
        // would name two components at once.
        var job = new JobDefinition
        {
            Input = "csv:in.csv",
            Output = "csv:out.csv",
            ProviderOptions = new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase)
            {
                ["csv-reader"] = new() { ["delimiter"] = ";" },
                ["csv-writer"] = new() { ["delimiter"] = "|" },
            },
        };

        var result = Cut(Job(job), at: 0);

        Assert.Empty(result.UnplaceableOptionBlocks);
        Assert.Equal(new[] { "csv-reader" }, result.Producer["main"].ProviderOptions!.Keys);
        Assert.Equal(new[] { "csv-writer" }, result.Consumer["main"].ProviderOptions!.Keys);
    }

    [Fact]
    public void A_Plain_Block_Both_Sides_Answer_To_Is_Not_Guessed_At()
    {
        var job = new JobDefinition
        {
            Input = "csv:in.csv",
            Output = "csv:out.csv",
            ProviderOptions = new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase)
            {
                ["csv"] = new() { ["delimiter"] = ";" },
            },
        };

        var result = Cut(Job(job), at: 0);

        Assert.Equal(new[] { "csv" }, result.UnplaceableOptionBlocks);
    }

    [Fact]
    public void A_Block_Neither_Component_Owns_Is_Named_Rather_Than_Placed()
    {
        // A --duck-init mounting a third database may serve either half. The cut stops.
        var job = new JobDefinition
        {
            Input = "csv:in.csv",
            Output = "duck:out.duckdb",
            ProviderOptions = new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase)
            {
                ["csv"] = new() { ["delimiter"] = ";" },
                ["postgres"] = new() { ["schema"] = "staging" },
            },
        };

        var result = Cut(Job(job), at: 0);

        Assert.Equal(new[] { "postgres" }, result.UnplaceableOptionBlocks);
    }

    // ── The secrets gate ─────────────────────────────────────────────────────

    [Fact]
    public void A_Credential_Written_In_Full_Is_Found_On_Either_Endpoint()
    {
        var job = new JobDefinition
        {
            Input = "pg:Host=db;Username=u;Password=hunter2",
            Output = "csv:out.csv",
        };

        Assert.Equal(new[] { "main.input" }, Cut(Job(job), at: 0).Literals);
    }

    [Fact]
    public void A_Reference_Is_Not_A_Credential()
    {
        var job = new JobDefinition
        {
            Input = "pg:Host=db;Username=u;Password=${{PGPASSWORD}}",
            Output = "csv:Path=out.csv;Token=${{keyring://ci}}",
        };

        Assert.Empty(Cut(Job(job), at: 0).Literals);
    }

    [Fact]
    public void A_Credential_On_A_Branch_The_Cut_Did_Not_Touch_Is_Still_Found()
    {
        // The untouched branches are copied into the consumer file verbatim, so one of theirs lands
        // in a repository just as surely as the cut branch's own.
        var jobs = new Dictionary<string, JobDefinition>(StringComparer.OrdinalIgnoreCase)
        {
            ["a"] = new JobDefinition { Input = "csv:a.csv" },
            ["b"] = new JobDefinition { Input = "pg:Host=db;Password=hunter2" },
        };

        Assert.Equal(new[] { "b.input" }, Cut(jobs, at: 0, alias: "a").Literals);
    }

    [Fact]
    public void A_Credential_Inside_A_Provider_Options_Value_Is_Found_And_Located()
    {
        var job = new JobDefinition
        {
            Input = "duck:src.duckdb",
            Output = "csv:out.csv",
            ProviderOptions = new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase)
            {
                ["duck"] = new() { ["init-sql"] = "SET s3_secret_access_key='AKIAsecret'" },
            },
        };

        Assert.Equal(new[] { "main.provider-options.duck.init-sql" }, Cut(Job(job), at: 0).Literals);
    }

    // ── What the acknowledgement shows ───────────────────────────────────────

    [Fact]
    public void A_Connection_Is_Redacted_Where_The_Placement_Is_Built()
    {
        var job = new JobDefinition
        {
            Input = "pg:Host=db;Username=u;Password=${{PGPASSWORD}}",
            Output = "csv:out.csv",
        };

        var input = Assert.Single(Cut(Job(job), at: 0).Placements, p => p.Key == "input");
        Assert.DoesNotContain("PGPASSWORD", input.Display);
        Assert.Contains("Host=db", input.Display);
    }

    // ── Stubs ────────────────────────────────────────────────────────────────

    private sealed class StubCsvReaderFactory : IStreamReaderFactory
    {
        public string ComponentName => "csv";
        public string Category => "Readers";
        public Type OptionsType => typeof(DtPipe.Adapters.Csv.CsvReaderOptions);
        public bool CanHandle(string connectionString) => connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
        public IStreamReader Create(OptionsRegistry registry) => throw new NotSupportedException();
        public IEnumerable<Type> GetSupportedOptionTypes() => new[] { OptionsType };
        public bool RequiresQuery => false;
    }

    private sealed class StubCsvWriterFactory : IDataWriterFactory
    {
        public string ComponentName => "csv";
        public string Category => "Writers";
        public Type OptionsType => typeof(DtPipe.Adapters.Csv.CsvWriterOptions);
        public bool CanHandle(string connectionString) => connectionString.EndsWith(".csv", StringComparison.OrdinalIgnoreCase);
        public IDataWriter Create(OptionsRegistry registry) => throw new NotSupportedException();
        public IEnumerable<Type> GetSupportedOptionTypes() => new[] { OptionsType };
    }

    private sealed class StubDuckReaderFactory : IStreamReaderFactory
    {
        public string ComponentName => "duck";
        public string Category => "Readers";
        public Type OptionsType => typeof(DtPipe.Adapters.Csv.CsvReaderOptions);
        public bool CanHandle(string connectionString) => connectionString.EndsWith(".duckdb", StringComparison.OrdinalIgnoreCase);
        public IStreamReader Create(OptionsRegistry registry) => throw new NotSupportedException();
        public IEnumerable<Type> GetSupportedOptionTypes() => new[] { OptionsType };
        public bool RequiresQuery => false;
    }

    private sealed class StubDuckWriterFactory : IDataWriterFactory
    {
        public string ComponentName => "duck";
        public string Category => "Writers";
        public Type OptionsType => typeof(DtPipe.Adapters.Csv.CsvWriterOptions);
        public bool CanHandle(string connectionString) => connectionString.EndsWith(".duckdb", StringComparison.OrdinalIgnoreCase);
        public IDataWriter Create(OptionsRegistry registry) => throw new NotSupportedException();
        public IEnumerable<Type> GetSupportedOptionTypes() => new[] { OptionsType };
    }
}
