using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Configuration;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Core.Pipelines;
using DtPipe.Processors.Merge;
using DtPipe.Processors.Sql;
using Xunit;
using System.Collections.Generic;
using System.Linq;

namespace DtPipe.Tests.Unit.Configuration;

/// <summary>
/// F3 — --export-job round-trip invariant: CLI → YAML → re-parse produces a
/// semantically identical pipeline (Input, Output, From/Ref, Transformers,
/// ProviderOptions, engine fields).
/// </summary>
public class JobFileRoundTripTests
{
    private readonly PipelineLexer _lexer;

    public JobFileRoundTripTests()
    {
        var registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(registry);
        foreach (var def in new PipelineOptionsCliContributor().GetFlagDefs())
            registry.Register(def with { Stage = FlagStage.All });

        // Transformer triggers (pipeline stage)
        registry.Register(new FlagDef("--fake", new[] { "-f" }, FlagArity.Repeatable, FlagScope.PerBranch, "fake transformer", FlagStage.Pipeline));
        registry.Register(new FlagDef("--fake-locale", System.Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "fake locale", FlagStage.Pipeline));
        registry.Register(new FlagDef("--filter", System.Array.Empty<string>(), FlagArity.Repeatable, FlagScope.PerBranch, "filter", FlagStage.Pipeline));
        registry.Register(new FlagDef("--sql", System.Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch, "sql processor", FlagStage.Pipeline));
        registry.Register(new FlagDef("--merge", System.Array.Empty<string>(), FlagArity.Boolean, FlagScope.PerBranch, "merge processor", FlagStage.Pipeline));

        // CSV reader / writer flags (stage-scoped)
        foreach (var def in CliOptionBuilder.GenerateFlagDefsForType(typeof(DtPipe.Adapters.Csv.CsvReaderOptions)))
            registry.Register(def with { Stage = FlagStage.Reader });
        foreach (var def in CliOptionBuilder.GenerateFlagDefsForType(typeof(DtPipe.Adapters.Csv.CsvWriterOptions)))
            registry.Register(def with { Stage = FlagStage.Writer });

        _lexer = new PipelineLexer(registry);
    }

    private static List<IDataTransformerFactory> TransformerFactories()
    {
        var registry = new OptionsRegistry();
        return new List<IDataTransformerFactory>
        {
            new DtPipe.Transformers.Arrow.Fake.FakeDataTransformerFactory(registry),
            new DtPipe.Transformers.Arrow.Filter.FilterDataTransformerFactory(registry, new DtPipe.Transformers.Services.JsEngineProvider()),
        };
    }

    private Dictionary<string, JobDefinition> RoundTrip(string[] cliArgs, out ParsedPipeline parsed)
    {
        parsed = _lexer.Parse(cliArgs);
        var converted = PipelineToJobConverter.Convert(
            parsed,
            streamTransformerFactories: new IStreamTransformerFactory[] { new CompositeSqlTransformerFactory(), new MergeTransformerFactory() },
            secretsManager: null,
            readerFactories: new IStreamReaderFactory[] { new StubCsvReaderFactory() },
            writerFactories: new IDataWriterFactory[] { new StubCsvWriterFactory() },
            dataTransformerFactories: TransformerFactories());

        var yaml = JobFileWriter.Serialize(converted.Jobs);
        return JobFileParser.ParseContent(yaml);
    }

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

    // ── (a) linear --fake + --filter ─────────────────────────────────────────

    [Fact]
    public void RoundTrip_Linear_Fake_And_Filter()
    {
        var reparsed = RoundTrip(new[]
        {
            "-i", "generate:10",
            "--fake", "NAME:name.firstName", "--fake-locale", "fr",
            "--filter", "row.Age > 21",
            "-o", "out.csv",
        }, out _);

        var job = reparsed["main"];
        Assert.NotNull(job.Transformers);
        Assert.Equal(2, job.Transformers!.Count);

        var fake = job.Transformers[0];
        Assert.Equal("fake", fake.Type);
        Assert.NotNull(fake.Mappings);
        Assert.Equal("name.firstName", fake.Mappings!["NAME"]);
        Assert.NotNull(fake.Options);
        Assert.Equal("fr", fake.Options!["locale"]);

        var filter = job.Transformers[1];
        Assert.Equal("filter", filter.Type);
        Assert.NotNull(filter.Mappings);
        Assert.True(filter.Mappings!.ContainsKey("row.Age > 21"));
        Assert.Equal("", filter.Mappings["row.Age > 21"]);
    }

    // ── (b) DAG --merge ──────────────────────────────────────────────────────

    [Fact]
    public void RoundTrip_Dag_Merge_PreservesProcessorAndFrom()
    {
        var reparsed = RoundTrip(new[]
        {
            "-i", "generate:5", "--alias", "a",
            "-i", "generate:5", "--alias", "b",
            "--from", "a,b", "--merge", "-o", "merged.csv",
        }, out var parsed);

        Assert.True(parsed.Globals.AllFlags.ContainsKey("--merge"));

        var mergeBranch = reparsed.Values.First(j => j.ProviderOptions?.ContainsKey("merge") == true);
        Assert.Equal("a,b", mergeBranch.From);
        Assert.Null(mergeBranch.Transformers);
    }

    // ── (c) DAG --sql + --fake on the processor branch ───────────────────────

    [Fact]
    public void RoundTrip_Dag_Sql_WithFake_PreservesQueryAndTransformer()
    {
        const string query = "SELECT * FROM src WHERE Id > 1";
        var reparsed = RoundTrip(new[]
        {
            "-i", "generate:100", "--alias", "src",
            "--from", "src", "--sql", query, "--fake", "Id:random.number",
            "-o", "processed.csv",
        }, out _);

        var sqlBranch = reparsed.Values.First(j => j.ProviderOptions?.ContainsKey("sql") == true);
        Assert.Equal(query, sqlBranch.ProviderOptions!["sql"]["query"]);
        Assert.Equal("src", sqlBranch.From);

        Assert.NotNull(sqlBranch.Transformers);
        var fake = Assert.Single(sqlBranch.Transformers!);
        Assert.Equal("fake", fake.Type);
        Assert.Equal("random.number", fake.Mappings!["Id"]);
    }

    // ── (d) incremental --cursor/--state ─────────────────────────────────────

    [Fact]
    public void RoundTrip_Incremental_CursorState()
    {
        var reparsed = RoundTrip(new[]
        {
            "-i", "events.csv", "--cursor", "Id", "--state", "state.json",
            "-o", "out.csv",
        }, out _);

        var job = reparsed["main"];
        Assert.Equal("Id", job.Cursor);
        Assert.Equal("state.json", job.State);
    }

    // ── (e) provider scoping: reader vs writer csv options ───────────────────

    [Fact]
    public void RoundTrip_ProviderScoping_ReaderAndWriterSeparate()
    {
        var reparsed = RoundTrip(new[]
        {
            "-i", "in.csv", "--csv-separator", ";",
            "-o", "out.csv", "--csv-separator", "|", "--csv-decimal-separator", ",",
        }, out _);

        var job = reparsed["main"];
        Assert.NotNull(job.ProviderOptions);

        var readerOpts = job.ProviderOptions!["csv-reader"]; // shared component name → suffixed
        Assert.Equal(";", readerOpts["separator"]);
        Assert.False(readerOpts.ContainsKey("decimal-separator"));

        var writerOpts = job.ProviderOptions!["csv-writer"];
        Assert.Equal("|", writerOpts["separator"]);
        Assert.Equal(",", writerOpts["decimal-separator"]);
    }

    // ── semantic equality of engine fields ───────────────────────────────────

    [Fact]
    public void RoundTrip_EngineFields_Preserved()
    {
        var reparsed = RoundTrip(new[]
        {
            "-i", "in.csv", "--limit", "42", "--batch-size", "500",
            "--sampling-rate", "0.5", "--sampling-seed", "7",
            "-o", "out.csv",
        }, out _);

        var job = reparsed["main"];
        Assert.Equal(42, job.Limit);
        Assert.Equal(500, job.BatchSize);
        Assert.Equal(0.5, job.SamplingRate);
        Assert.Equal(7, job.SamplingSeed);
    }

    // ── (f) provider options behind a keyring indirection ────────────────────

    /// <summary>
    /// An unresolved 'keyring://alias' matches no ComponentSelector prefix and no CanHandle, so the
    /// reader factory came back null and every reader option — the query included — was dropped
    /// from the exported file, which then died on "A query is required for provider 'mssql'".
    /// </summary>
    [Fact]
    public void RoundTrip_KeyringInput_PreservesReaderProviderOptions()
    {
        var secrets = new DtPipe.Cli.Security.InMemorySecretsManager();
        secrets.SetSecret("SRC", "in.csv");

        var parsed = _lexer.Parse(new[]
        {
            "-i", "keyring://SRC", "--csv-separator", ";",
            "-o", "out.csv",
        });

        var converted = PipelineToJobConverter.Convert(
            parsed,
            streamTransformerFactories: new IStreamTransformerFactory[] { new CompositeSqlTransformerFactory() },
            secretsManager: secrets,
            readerFactories: new IStreamReaderFactory[] { new StubCsvReaderFactory() },
            writerFactories: new IDataWriterFactory[] { new StubCsvWriterFactory() },
            dataTransformerFactories: TransformerFactories());

        var job = JobFileParser.ParseContent(JobFileWriter.Serialize(converted.Jobs))["main"];

        Assert.NotNull(job.ProviderOptions);
        Assert.Equal(";", job.ProviderOptions!["csv-reader"]["separator"]);

        // The indirection is what gets written, never what it stands for.
        Assert.Equal("keyring://SRC", job.Input);
    }

    [Fact]
    public void RoundTrip_UnknownKeyringAlias_DoesNotThrow()
    {
        var parsed = _lexer.Parse(new[] { "-i", "keyring://ABSENT", "-o", "out.csv" });

        var converted = PipelineToJobConverter.Convert(
            parsed,
            streamTransformerFactories: Array.Empty<IStreamTransformerFactory>(),
            secretsManager: new DtPipe.Cli.Security.InMemorySecretsManager(),
            readerFactories: new IStreamReaderFactory[] { new StubCsvReaderFactory() },
            writerFactories: new IDataWriterFactory[] { new StubCsvWriterFactory() },
            dataTransformerFactories: TransformerFactories());

        Assert.Equal("keyring://ABSENT", converted.Jobs["main"].Input);
    }

    // ── (g) an exported job carries only what the branch has ─────────────────

    /// <summary>
    /// 'ref' defaults to Array.Empty&lt;string&gt;(), which OmitDefaults does not treat as a default
    /// (the type default is null), so every source branch was written with 'ref: []' — anchored
    /// '&amp;o0' and aliased on the next branch, because that initialiser is one shared instance.
    /// 'from' had the matching problem: string.Join over no aliases is "", not absence.
    /// </summary>
    [Fact]
    public void Export_SourceBranches_CarryNoEmptyRoutingKeys()
    {
        var parsed = _lexer.Parse(new[]
        {
            "-i", "a.csv", "--alias", "A",
            "-i", "b.csv", "--alias", "B",
            "--from", "A", "--ref", "B", "--sql", "SELECT 1", "-o", "out.csv",
        });

        var yaml = JobFileWriter.Serialize(PipelineToJobConverter.Convert(
            parsed,
            streamTransformerFactories: new IStreamTransformerFactory[] { new CompositeSqlTransformerFactory() },
            secretsManager: null,
            readerFactories: new IStreamReaderFactory[] { new StubCsvReaderFactory() },
            writerFactories: new IDataWriterFactory[] { new StubCsvWriterFactory() },
            dataTransformerFactories: TransformerFactories()).Jobs);

        Assert.DoesNotContain("ref: []", yaml);
        Assert.DoesNotContain("from: ''", yaml);
        Assert.DoesNotContain("&o0", yaml);   // no anchor, so no alias either

        // What the processor branch really has is still written.
        Assert.Contains("from: A", yaml);
        Assert.Contains("- B", yaml);
    }

    [Fact]
    public void Export_EmptyRoutingKeys_StillRoundTripToAUsablePipeline()
    {
        var reparsed = RoundTrip(new[]
        {
            "-i", "a.csv", "--alias", "A",
            "-i", "b.csv", "--alias", "B",
            "--from", "A", "--ref", "B", "--sql", "SELECT 1", "-o", "out.csv",
        }, out _);

        Assert.Null(reparsed["A"].From);
        Assert.Empty(reparsed["A"].Ref ?? Array.Empty<string>());
        Assert.Equal("A", reparsed["stream1"].From);
        Assert.Equal(new[] { "B" }, reparsed["stream1"].Ref);
    }

    // ── (h) key order follows the pipeline ───────────────────────────────────

    /// <summary>
    /// Reflection order is the order JobDefinition happens to declare its properties, which read
    /// 'output' before 'from' and scattered the routing keys through the engine controls.
    /// </summary>
    [Fact]
    public void Export_KeysOfABranch_ReadInPipelineOrder()
    {
        var parsed = _lexer.Parse(new[]
        {
            "-i", "a.csv", "--alias", "A",
            "-i", "b.csv", "--alias", "B",
            "--from", "A", "--ref", "B", "--sql", "SELECT 1", "--limit", "5", "-o", "out.csv",
        });

        var yaml = JobFileWriter.Serialize(PipelineToJobConverter.Convert(
            parsed,
            streamTransformerFactories: new IStreamTransformerFactory[] { new CompositeSqlTransformerFactory() },
            secretsManager: null,
            readerFactories: new IStreamReaderFactory[] { new StubCsvReaderFactory() },
            writerFactories: new IDataWriterFactory[] { new StubCsvWriterFactory() },
            dataTransformerFactories: TransformerFactories()).Jobs);

        // Keys of the processor branch alone — IndexOf over the whole file would find another
        // branch's engine keys first.
        var keys = yaml.Split('\n')
            .SkipWhile(l => !l.StartsWith("stream1:")).Skip(1)
            .TakeWhile(l => l.StartsWith(" "))
            .Where(l => l.StartsWith("  ") && !l.StartsWith("   ") && l.Contains(':'))
            .Select(l => l.Trim().Split(':')[0])
            .ToList();

        // Reads before it writes, writes before it is tuned, nested detail last.
        var order = string.Join(",", keys);
        Assert.Equal("from,ref,output,batch-size,limit,sampling-rate,provider-options", order);
    }
}
