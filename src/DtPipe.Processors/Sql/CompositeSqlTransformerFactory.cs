namespace DtPipe.Processors.Sql;

using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines.Dag;

/// <summary>
/// SQL engine factory for DuckDB.
/// Provides standard SQL, richer function library, no build step, and queries testable
/// outside DtPipe. Recommended for all typical ETL/transformation use cases.
/// </summary>
public class CompositeSqlTransformerFactory : IStreamTransformerFactory
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

    public IStreamTransformer Create(string[] branchArgs, BranchChannelContext ctx, IServiceProvider sp)
    {
        return new DuckDB.DuckDBSqlTransformerFactory().Create(branchArgs, ctx, sp);
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
