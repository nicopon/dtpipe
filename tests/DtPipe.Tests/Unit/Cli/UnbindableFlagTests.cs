using System;
using System.Collections.Generic;
using System.Linq;
using DtPipe.Cli.Infrastructure;
using DtPipe.Cli.Pipeline;
using DtPipe.Core.Abstractions;
using DtPipe.Core.Options;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DtPipe.Tests.Unit.Cli;

/// <summary>
/// A stage flag the branch's component does not carry binds to nothing.
///
/// The flag registry is global — every database reader contributes <c>--query</c> through
/// <c>QueryableReaderOptions</c> — so <c>--query</c> is a legal token whatever the branch reads,
/// and the capability interface that decides whether it applies was acting as a silent filter:
/// <c>--query</c> and <c>--table</c> on a <c>csv:</c> source copied the whole file and exited 0,
/// while the same line misspelt already failed closed. <c>666987eb</c> refused the neighbouring
/// case, a reader flag in a branch with no reader at all.
/// </summary>
public class UnbindableFlagTests
{
    private static (IEnumerable<IStreamReaderFactory> Readers, IEnumerable<IDataWriterFactory> Writers) Catalogue()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<OptionsRegistry>();
        var sp = services.BuildServiceProvider();
        var registry = sp.GetRequiredService<OptionsRegistry>();

        var readers = new IStreamReaderFactory[]
        {
            new CliStreamReaderFactory(new DtPipe.Adapters.Csv.CsvReaderDescriptor(), registry, sp),
            new CliStreamReaderFactory(new DtPipe.Adapters.Sqlite.SqliteReaderDescriptor(), registry, sp)
        };
        var writers = new IDataWriterFactory[]
        {
            new CliDataWriterFactory(new DtPipe.Adapters.Csv.CsvWriterDescriptor(), registry, sp),
            new CliDataWriterFactory(new DtPipe.Adapters.Sqlite.SqliteWriterDescriptor(), registry, sp),
            new CliDataWriterFactory(new DtPipe.Adapters.Parquet.ParquetWriterDescriptor(), registry, sp),
            // Carries --duck-init, the flag a processor reads off its own branch's raw tokens.
            new CliDataWriterFactory(new DtPipe.Adapters.DuckDB.DuckDbWriterDescriptor(), registry, sp)
        };
        return (readers, writers);
    }

    private static ParsedPipeline Parse(params string[] args)
    {
        var registry = new FlagRegistry();
        CoreFlagRegistry.RegisterCoreFlags(registry);
        foreach (var def in new PipelineOptionsCliContributor().GetFlagDefs())
            registry.Register(def);
        registry.Register(new FlagDef("--sql", Array.Empty<string>(), FlagArity.Scalar, FlagScope.PerBranch,
            "sql", FlagStage.Pipeline, ProcessorTrigger: true));
        var (readers, writers) = Catalogue();
        foreach (var f in readers.Cast<IDataFactory>().Concat(writers))
            foreach (var def in CliOptionBuilder.GenerateFlagDefsForType(f.OptionsType))
                registry.Register(def);
        return new PipelineLexer(registry).Parse(args);
    }

    private static (Dictionary<string, DtPipe.Core.Models.JobDefinition>, DtPipe.Core.Pipelines.Dag.JobDagDefinition, Dictionary<string, CliJobContext>) Convert(params string[] args)
    {
        var (readers, writers) = Catalogue();
        return PipelineToJobConverter.Convert(
            Parse(args), new[] { new SqlLikeProcessorFactory() }, null, readers, writers);
    }

    [Theory]
    [InlineData("--query", "SELECT 1")]
    [InlineData("-q", "SELECT 1")]
    [InlineData("--table", "orders")]
    public void A_Reader_Flag_The_Reader_Does_Not_Carry_Is_Refused(string flag, string value)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            Convert("-i", "csv:in.csv", flag, value, "-o", "csv:out.csv"));

        Assert.Contains(flag, ex.Message);
        Assert.Contains("'csv' reader", ex.Message);
        Assert.Contains("nothing binds it", ex.Message);
    }

    /// <summary>
    /// The refusal names what the component does accept rather than a fixed list: the options come
    /// off the type, so an option added, renamed or retired cannot leave the message behind.
    /// </summary>
    [Fact]
    public void The_Refusal_Names_The_Options_The_Component_Accepts()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            Convert("-i", "csv:in.csv", "--query", "SELECT 1", "-o", "csv:out.csv"));

        Assert.Contains("--csv-separator", ex.Message);
        Assert.DoesNotContain("--query,", ex.Message);
    }

    /// <summary>A flag the counterpart does carry is misplaced, not unbindable — say which way to move it.</summary>
    [Fact]
    public void A_Flag_The_Writer_Carries_Says_To_Move_It_After_Output()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            Convert("-i", "csv:in.csv", "--table", "orders", "-o", "sqlite:Data Source=out.db"));

        Assert.Contains("'sqlite' writer does carry it", ex.Message);
        Assert.Contains("move it after -o", ex.Message);
    }

    [Fact]
    public void A_Reader_Flag_The_Reader_Does_Carry_Is_Accepted()
    {
        var (jobs, _, _) = Convert("-i", "sqlite:Data Source=in.db", "--query", "SELECT 1", "-o", "csv:out.csv");

        Assert.Single(jobs);
    }

    [Fact]
    public void A_Component_Option_In_Its_Own_Stage_Is_Accepted()
    {
        var (jobs, _, _) = Convert("-i", "csv:in.csv", "--csv-separator", ";", "-o", "csv:out.csv");

        Assert.Single(jobs);
    }

    /// <summary>
    /// Engine flags belong to no component, so they are never candidates — the check reads the
    /// component catalogue, and a core flag is absent from it by construction.
    /// </summary>
    [Fact]
    public void Engine_Flags_Are_Not_Candidates()
    {
        var (jobs, _, _) = Convert("-i", "csv:in.csv", "--limit", "10", "--batch-size", "512", "-o", "csv:out.csv");

        Assert.Single(jobs);
    }

    /// <summary>
    /// The writer stage is deliberately not judged. <c>--strategy</c> on a file target binds to
    /// nothing, but the writer replaces the file anyway, so the flag is redundant rather than
    /// wrong — and four of this repository's own data-init scripts write it that way.
    /// </summary>
    [Fact]
    public void A_Writer_Flag_The_Writer_Does_Not_Carry_Is_Left_Alone()
    {
        var (jobs, _, _) = Convert("-i", "csv:in.csv", "-o", "parquet:out.parquet", "--strategy", "Recreate");

        Assert.Single(jobs);
    }

    /// <summary>
    /// A processor reads its own branch's raw tokens — DuckDBSqlTransformerFactory pulls
    /// <c>--duck-init</c> straight out of them and executes it — so on such a branch the reader's
    /// and writer's option sets do not decide what binds, and the check stands down.
    /// </summary>
    [Fact]
    public void A_Processor_Branch_Is_Not_Judged()
    {
        var (jobs, _, _) = Convert(
            "-i", "csv:in.csv", "--alias", "s",
            "--from", "s", "--sql", "SELECT 1 FROM s", "-o", "csv:out.csv", "--duck-init", "SELECT 1;");

        Assert.Equal(2, jobs.Count);
    }

    private sealed class SqlLikeProcessorFactory : IStreamTransformerFactory
    {
        public string ComponentName => "sql";
        public string Category => "Stream Processors";
        public bool RequiresArrowChannels => true;
        public int MinStreams => 1;
        public int MaxStreams => 1;
        public int MinLookups => 0;
        public int MaxLookups => -1;
        public IReadOnlyList<(string Flag, bool IsBoolean)> CliTriggerFlags => new[] { ("--sql", false) };

        public bool IsApplicable(string[] branchArgs) => branchArgs.Contains("--sql");
        public IStreamTransformer Create(string[] branchArgs, DtPipe.Core.Pipelines.Dag.BranchChannelContext ctx, IServiceProvider sp)
            => throw new NotSupportedException();
        public IStreamTransformer CreateFromJob(DtPipe.Core.Models.JobDefinition job, DtPipe.Core.Pipelines.Dag.BranchChannelContext ctx, IServiceProvider sp)
            => throw new NotSupportedException();
    }
}
