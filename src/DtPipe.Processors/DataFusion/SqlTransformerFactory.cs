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
/// <c>--sql</c> (inline SQL query), and optionally <c>--src-main</c> / <c>--src-ref</c>
/// (direct file sources bypassing memory channels).
/// </summary>
public class SqlTransformerFactory : IStreamTransformerFactory
{
    public string ComponentName => "sql";
    public string Category => "Stream Processors";
    public bool RequiresArrowChannels => true;

    public bool IsApplicable(string[] branchArgs)
        => ExtractSqlQuery(branchArgs) != null;

    public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider serviceProvider)
    {
        var query = ExtractSqlQuery(branchArgs)
            ?? throw new ArgumentException("--sql <query> is required for SqlTransformer");

        // The args carry logical aliases (SQL table names). The orchestrator preserved them
        // so the SQL query can reference them directly. Physical channel aliases are resolved
        // via ctx.AliasMap (built by DagOrchestrator for fan-out scenarios).
        var mainAlias = ExtractArgValue(branchArgs, "--from") ?? "";
        var mainChannelAlias = ctx.AliasMap.GetValueOrDefault(mainAlias, mainAlias);

        var refAliases = ExtractAllArgValues(branchArgs, "--ref")
            .SelectMany(r => r.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            .ToArray();
        var refChannelAliases = refAliases
            .Select(a => ctx.AliasMap.GetValueOrDefault(a, a))
            .ToArray();

        var srcMain = ExtractArgValue(branchArgs, "--src-main") ?? "";
        var srcRefs = ExtractAllArgValues(branchArgs, "--src-ref").ToArray();

        var registry = serviceProvider.GetRequiredService<IMemoryChannelRegistry>();
        var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();

        var processor = new DataFusionProcessor(
            registry: registry,
            query: query,
            mainAlias: mainAlias,
            mainChannelAlias: mainChannelAlias,
            refAliases: refAliases,
            refChannelAliases: refChannelAliases,
            srcMain: srcMain,
            srcRefs: srcRefs,
            logger: loggerFactory.CreateLogger<DataFusionProcessor>());

        return new SqlTransformer(processor);
    }

    private static string? ExtractSqlQuery(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (!args[i].Equals("--sql", StringComparison.OrdinalIgnoreCase)) continue;
            if (i + 1 >= args.Length) return null;
            var val = args[i + 1];
            if (val.StartsWith('-') && val.Length > 1 && !char.IsDigit(val[1])) return null;
            return val;
        }
        return null;
    }

    private static string? ExtractArgValue(string[] args, string flag)
    {
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (!args[i].Equals(flag, StringComparison.OrdinalIgnoreCase)) continue;
            var val = args[i + 1];
            if (val.StartsWith('-') && val.Length > 1 && !char.IsDigit(val[1])) return null;
            return val;
        }
        return null;
    }

    private static IEnumerable<string> ExtractAllArgValues(string[] args, string flag)
    {
        for (int i = 0; i < args.Length - 1; i++)
            if (args[i].Equals(flag, StringComparison.OrdinalIgnoreCase)) yield return args[i + 1];
    }
}
