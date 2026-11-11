using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;
using DtPipe.Core.Expressions;
using DtPipe.Core.Security;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Processors.DuckDB;

/// <summary>
/// Factory for <see cref="Sql.SqlStreamTransformer"/> backed by DuckDB.
/// Activated when branch arguments contain <c>--sql &lt;query&gt;</c>.
/// The <c>--from</c> source is streamed lazily via a DuckDB table function;
/// <c>--ref</c> sources are fully materialised into in-memory DuckDB tables before query execution.
/// </summary>
public class DuckDBSqlTransformerFactory : IStreamTransformerFactory
{
    public string ComponentName => "sql";
    public string Category => "Stream Processors";
    public bool RequiresArrowChannels => true;

    public int MinStreams => 1;
    public int MaxStreams => 1;
    public int MinLookups => 0;
    public int MaxLookups => -1;

    public IReadOnlyList<(string Flag, bool IsBoolean)> CliTriggerFlags => [("--sql", false)];

    public bool IsApplicable(string[] branchArgs)
        => BranchArgParser.ExtractValue(branchArgs, "--sql") != null ||
           (BranchArgParser.ExtractValue(branchArgs, "--from") != null && BranchArgParser.GetPositionalQuery(branchArgs) != null);

    public Dictionary<string, object?>? ExportToProviderOptions(string[] branchArgs)
    {
        var query = BranchArgParser.ExtractValue(branchArgs, "--sql") ?? BranchArgParser.GetPositionalQuery(branchArgs);
        if (query == null) return null;

        var options = new Dictionary<string, object?> { ["query"] = query };
        var initSql = BranchArgParser.ExtractValue(branchArgs, "--duck-init");
        if (!string.IsNullOrEmpty(initSql)) options["duck-init"] = initSql;
        return options;
    }

    public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider serviceProvider)
    {
        var query = BranchArgParser.ExtractValue(branchArgs, "--sql")
            ?? BranchArgParser.GetPositionalQuery(branchArgs)
            ?? throw new ArgumentException("--sql <query> or a positional SQL query is required for DuckDBSqlTransformer");

        var mainAlias = BranchArgParser.ExtractValue(branchArgs, "--from") ?? "";

        // Only one source streams into a SQL branch; the others are materialized through --ref.
        // Without this the comma reached the channel registry as part of the alias, and the run
        // failed on "no channel named 'a,b'" — a name the user never wrote.
        if (mainAlias.Contains(','))
            throw new ArgumentException(
                $"--sql takes a single streaming source, but --from names several ('{mainAlias}'). " +
                $"Keep one on --from and pass the others to --ref: --from {mainAlias.Split(',')[0].Trim()} " +
                $"--ref {string.Join(",", mainAlias.Split(',').Skip(1).Select(a => a.Trim()))}.");

        var refAliases = BranchArgParser.ExtractAllValues(branchArgs, "--ref")
            .SelectMany(r => r.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .ToArray();

        var initSql = BranchArgParser.ExtractValue(branchArgs, "--duck-init");

        return CreateFromOptions(query, mainAlias, refAliases, initSql, ctx, serviceProvider);
    }

    public IStreamTransformer CreateFromJob(JobDefinition job, BranchChannelContext ctx, IServiceProvider serviceProvider)
    {
        var sqlOpts = job.ProviderOptions?.GetValueOrDefault(ComponentName);
        var query = sqlOpts?.GetValueOrDefault("query") as string;
        if (string.IsNullOrEmpty(query))
            throw new ArgumentException("provider-options.sql.query is required for the SQL stream processor.");

        var mainAlias = job.From ?? "";
        var refAliases = job.Ref;
        var initSql = sqlOpts?.GetValueOrDefault("duck-init") as string;

        return CreateFromOptions(query, mainAlias, refAliases, initSql, ctx, serviceProvider);
    }

    // Shared convergence point for the CLI (Create) and YAML (CreateFromJob) surfaces.
    private static IStreamTransformer CreateFromOptions(
        string query, string mainAlias, string[] refAliases, string? initSql,
        BranchChannelContext ctx, IServiceProvider serviceProvider)
    {
        var mainChannelAlias = ctx.AliasMap.GetValueOrDefault(mainAlias, mainAlias);
        var refChannelAliases = refAliases
            .Select(a => ctx.AliasMap.GetValueOrDefault(a, a))
            .ToArray();

        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
        var resolver = serviceProvider.GetService<IStringContentResolver>();

        var processor = new DuckDBSqlProcessor(
            registry: registry,
            query: query,
            mainAlias: mainAlias,
            mainChannelAlias: mainChannelAlias,
            refAliases: refAliases,
            refChannelAliases: refChannelAliases,
            logger: loggerFactory.CreateLogger<DuckDBSqlProcessor>(),
            initSql: initSql,
            resolver: resolver,
            edgesOnly: ctx.EdgesOnly);

        return new Sql.SqlStreamTransformer(processor);
    }
}
