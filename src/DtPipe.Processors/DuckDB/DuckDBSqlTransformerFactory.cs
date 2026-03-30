using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Pipelines.Dag;
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

    public bool IsApplicable(string[] branchArgs)
        => BranchArgParser.ExtractValue(branchArgs, "--sql") != null;

    public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider serviceProvider)
    {
        var query = BranchArgParser.ExtractValue(branchArgs, "--sql")
            ?? throw new ArgumentException("--sql <query> is required for DuckDBSqlTransformer");

        var mainAlias = BranchArgParser.ExtractValue(branchArgs, "--from") ?? "";
        var mainChannelAlias = ctx.AliasMap.GetValueOrDefault(mainAlias, mainAlias);

        var refAliases = BranchArgParser.ExtractAllValues(branchArgs, "--ref")
            .SelectMany(r => r.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .ToArray();
        var refChannelAliases = refAliases
            .Select(a => ctx.AliasMap.GetValueOrDefault(a, a))
            .ToArray();

        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();

        var processor = new DuckDBSqlProcessor(
            registry: registry,
            query: query,
            mainAlias: mainAlias,
            mainChannelAlias: mainChannelAlias,
            refAliases: refAliases,
            refChannelAliases: refChannelAliases,
            logger: loggerFactory.CreateLogger<DuckDBSqlProcessor>());

        return new Sql.SqlStreamTransformer(processor);
    }
}
