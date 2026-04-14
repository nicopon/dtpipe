namespace DtPipe.Processors.Sql;

using DtPipe.Core.Abstractions;
using DtPipe.Core.Pipelines.Dag;

/// <summary>
/// Selects DuckDB or DataFusion as the SQL engine.
/// Priority: --sql-engine (branch arg) > DTPIPE_SQL_ENGINE (env var) > duckdb (default).
///
/// DuckDB (default): standard SQL, richer function library, no build step, queries testable
/// outside DtPipe. Recommended for all typical ETL/transformation use cases.
///
/// DataFusion (--sql-engine datafusion): Arrow-native zero-copy output, no DataChunk→Arrow
/// conversion on the output path. Consider for high-throughput pipelines (>10M rows) or
/// when the Rust native library is already present in the distribution.
/// Requires ./build_datafusion_bridge.sh — not available in standard distributions.
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
        var engine = BranchArgParser.ExtractValue(branchArgs, "--sql-engine")
                     ?? Environment.GetEnvironmentVariable("DTPIPE_SQL_ENGINE")
                     ?? "duckdb";

        if (engine.Equals("datafusion", StringComparison.OrdinalIgnoreCase))
        {
            if (!_dataFusionAvailable)
                throw new InvalidOperationException(
                    "DataFusion native library not found. Run ./build_datafusion_bridge.sh first.");
            return new DataFusion.DataFusionSqlTransformerFactory().Create(branchArgs, ctx, sp);
        }

        return new DuckDB.DuckDBSqlTransformerFactory().Create(branchArgs, ctx, sp);
    }

    // Probe once at class load time — native library presence does not change during a run.
    private static readonly bool _dataFusionAvailable = ProbeDataFusion();

    /// <summary>
    /// Returns the list of SQL engines with their availability status.
    /// Used by <c>dtpipe sql-engines</c> to report what is compiled into this distribution.
    /// </summary>
    public static IReadOnlyList<SqlEngineInfo> GetEngines() =>
    [
        new("duckdb",      Available: true,                 IsDefault: true,
            "Standard SQL · no build step · queries testable with DuckDB CLI"),
        new("datafusion",  Available: _dataFusionAvailable, IsDefault: false,
            "Experimental · Arrow-native zero-copy output · requires Rust bridge (DTPIPE_EXPERIMENTAL=1)"),
    ];

    public sealed record SqlEngineInfo(string Name, bool Available, bool IsDefault, string Description);

    private static bool ProbeDataFusion()
    {
        // Use NativeLibrary.TryLoad with the assembly overload so the runtime's full
        // resolution chain is used — this covers both side-by-side files and native libs
        // extracted from a single-file bundle into a temp directory.
        return System.Runtime.InteropServices.NativeLibrary.TryLoad(
            "dtpipe_datafusion",
            typeof(DataFusion.DataFusionBridge).Assembly,
            null,
            out _);
    }
}
