using DtPipe.Core.Abstractions;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Pipelines.Dag;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DtPipe.Processors.DataFusion;

/// <summary>
/// Factory for <see cref="SqlTransformer"/>.
/// Activated when branch arguments contain <c>--sql &lt;query&gt;</c>.
/// Reads <c>--from</c> (main streaming alias), <c>--ref</c> (materialized aliases),
/// and <c>--sql</c> (inline SQL query).
/// </summary>
public class SqlTransformerFactory : IStreamTransformerFactory
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
            ?? throw new ArgumentException("--sql <query> is required for SqlTransformer");

        // The args carry logical aliases (SQL table names). The orchestrator preserved them
        // so the SQL query can reference them directly. Physical channel aliases are resolved
        // via ctx.AliasMap (built by DagOrchestrator for fan-out scenarios).
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

        var processor = new DataFusionProcessor(
            registry: registry,
            query: query,
            mainAlias: mainAlias,
            mainChannelAlias: mainChannelAlias,
            refAliases: refAliases,
            refChannelAliases: refChannelAliases,
            logger: loggerFactory.CreateLogger<DataFusionProcessor>());

        return new SqlTransformer(processor);
    }
}
