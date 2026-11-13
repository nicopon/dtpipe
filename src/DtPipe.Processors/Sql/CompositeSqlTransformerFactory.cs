namespace DtPipe.Processors.Sql;

using DtPipe.Core.Abstractions;
using DtPipe.Core.Models;
using DtPipe.Core.Pipelines.Dag;

/// <summary>
/// SQL engine factory for DuckDB.
/// Provides standard SQL, richer function library, no build step, and queries testable
/// outside DtPipe. Recommended for all typical ETL/transformation use cases.
/// </summary>
public class CompositeSqlTransformerFactory : IStreamTransformerFactory, IStreamProcessorMarker
{
    public string ComponentName => "sql";
    public string Category => "Stream Processors";
    public bool RequiresArrowChannels => true;
    public int MinStreams => 1;
    public int MaxStreams => 1;
    public int MinLookups => 0;
    public int MaxLookups => -1;

    public string Description => "Run a DuckDB SQL query over the branches this branch reads (join, aggregate, filter).";

    public string UsageNotes =>
        """
        The query runs in DuckDB, never in the SQL dialect of a source or target database.
        Each alias named by 'from' and 'ref' is a table of that name in the query.
        Example:
          main:
            from: orders
            ref: [ customers ]
            provider-options:
              sql:
                query: SELECT c.name, sum(o.amount) AS total FROM orders o JOIN customers c ON o.customer_id = c.id GROUP BY c.name
                duck-init: <optional DuckDB statement run before the query>
            output: "<adapter>:<target>"
        Column types are those DuckDB gives the result. An untyped NULL literal takes an integer type,
        which a text column of the target refuses: write CAST(NULL AS VARCHAR), or the type the target
        holds. 'dry-run' lists each column whose type differs from the target's.
        """;

    public IReadOnlyList<(string Flag, bool IsBoolean)> CliTriggerFlags => [("--sql", false)];

    public bool IsApplicable(string[] branchArgs)
        => BranchArgParser.ExtractValue(branchArgs, "--sql") != null;

    public Dictionary<string, object?>? ExportToProviderOptions(string[] branchArgs)
    {
        var query = BranchArgParser.ExtractValue(branchArgs, "--sql") ?? BranchArgParser.GetPositionalQuery(branchArgs);
        if (query == null) return null;

        var options = new Dictionary<string, object?> { ["query"] = query };
        var initSql = BranchArgParser.ExtractValue(branchArgs, "--duck-init");
        if (!string.IsNullOrEmpty(initSql)) options["duck-init"] = initSql;
        return options;
    }

    public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider sp)
    {
        return new DuckDB.DuckDBSqlTransformerFactory().Create(branchArgs, ctx, sp);
    }

    public IStreamTransformer CreateFromJob(JobDefinition job, BranchChannelContext ctx, IServiceProvider sp)
    {
        return new DuckDB.DuckDBSqlTransformerFactory().CreateFromJob(job, ctx, sp);
    }

    /// <summary>
    /// Returns the list of SQL engines with their availability status.
    /// </summary>
    public static IReadOnlyList<SqlEngineInfo> GetEngines() =>
    [
        new("duckdb", Available: true, IsDefault: true,
            "Standard SQL · no build step · queries testable with DuckDB CLI"),
    ];

    public sealed record SqlEngineInfo(string Name, bool Available, bool IsDefault, string Description);
}
