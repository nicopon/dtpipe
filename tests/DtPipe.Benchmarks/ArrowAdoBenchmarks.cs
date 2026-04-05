using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Ado.Binder;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;
using System.Data.Common;

namespace DtPipe.Benchmarks;

/// <summary>
/// Benchmarks for the Apache.Arrow.Ado adapter — read side (AdoToArrow) and write side
/// (IArrowBinder, RecordBatchDataReader) — plus config builder allocation cost.
///
/// Mirrors the structure of JdbcAdapterBenchmarks.java in the Apache Arrow Java project
/// (performance/src/main/java/org/apache/arrow/adapter/jdbc/JdbcAdapterBenchmarks.java).
/// Uses SQLite in-memory as H2 is used in Java — no external infrastructure required.
///
/// Run (release mode required for meaningful numbers):
///   dotnet run -c Release --project tests/DtPipe.Benchmarks -- --filter "*ArrowAdo*"
/// </summary>
[MemoryDiagnoser]
public class ArrowAdoBenchmarks
{
    private const int RowCount = 3000;

    private SqliteConnection _conn = null!;

    // Pre-built configs — initialized once in GlobalSetup, not per iteration.
    private AdoToArrowConfig _config = null!;
    private AdoToArrowConfig _configNoOverrides = null!;

    // Pre-built RecordBatch for write-side benchmarks.
    private RecordBatch _recordBatch = null!;
    private Int32Array _int32Array = null!;

    private SqliteParameter _param = null!;
    private Int32Binder _int32Binder = null!;

    // ── Setup / Teardown ─────────────────────────────────────────────────────

    [GlobalSetup]
    public async Task Setup()
    {
        // Build configs once — the anti-pattern (building per call) is benchmarked separately.
        _config = new AdoToArrowConfigBuilder().SetTargetBatchSize(1024).Build();
        _configNoOverrides = new AdoToArrowConfigBuilder()
            .ClearDataTypeNameOverrides()
            .SetTargetBatchSize(1024)
            .Build();

        // SQLite in-memory database with RowCount rows.
        // Schema mirrors Java: (int, long, string, bool) → f0, f1, f2, f3.
        _conn = new SqliteConnection("Data Source=:memory:");
        await _conn.OpenAsync();

        using (var createCmd = _conn.CreateCommand())
        {
            createCmd.CommandText =
                "CREATE TABLE bench (f0 INTEGER NOT NULL, f1 INTEGER NOT NULL, " +
                "f2 TEXT NOT NULL, f3 INTEGER NOT NULL)";
            await createCmd.ExecuteNonQueryAsync();
        }

        await using var tx = await _conn.BeginTransactionAsync();
        using (var insertCmd = _conn.CreateCommand())
        {
            insertCmd.CommandText = "INSERT INTO bench VALUES (@f0, @f1, @f2, @f3)";
            var p0 = insertCmd.Parameters.Add("@f0", SqliteType.Integer);
            var p1 = insertCmd.Parameters.Add("@f1", SqliteType.Integer);
            var p2 = insertCmd.Parameters.Add("@f2", SqliteType.Text);
            var p3 = insertCmd.Parameters.Add("@f3", SqliteType.Integer);

            for (int i = 0; i < RowCount; i++)
            {
                p0.Value = i;
                p1.Value = (long)i;
                p2.Value = "test" + i;
                p3.Value = i % 2 == 0 ? 1 : 0;
                await insertCmd.ExecuteNonQueryAsync();
            }
        }
        await tx.CommitAsync();

        // Pre-built RecordBatch for write-side benchmarks.
        var int32Builder  = new Int32Array.Builder();
        var int64Builder  = new Int64Array.Builder();
        var stringBuilder = new StringArray.Builder();
        var boolBuilder   = new BooleanArray.Builder();

        for (int i = 0; i < RowCount; i++)
        {
            int32Builder.Append(i);
            int64Builder.Append((long)i);
            stringBuilder.Append("test" + i);
            boolBuilder.Append(i % 2 == 0);
        }

        var schema = new Schema(
        [
            new Field("f0", Int32Type.Default,   nullable: false),
            new Field("f1", Int64Type.Default,   nullable: false),
            new Field("f2", StringType.Default,  nullable: false),
            new Field("f3", BooleanType.Default, nullable: false),
        ], null);

        _recordBatch = new RecordBatch(schema,
        [
            int32Builder.Build(),
            int64Builder.Build(),
            stringBuilder.Build(),
            boolBuilder.Build(),
        ], RowCount);

        _int32Array = (Int32Array)_recordBatch.Column(0);
        _param      = new SqliteParameter();
        _int32Binder = new Int32Binder();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _conn.CloseAsync();
        await _conn.DisposeAsync();
    }

    // ── Read side — mirrors Java JdbcAdapterBenchmarks ───────────────────────

    /// <summary>
    /// E2E: open a SQLite reader and convert all rows to Arrow RecordBatches.
    /// Config is pre-built (see GlobalSetup) — measures pure pipeline overhead.
    /// Mirrors Java testJdbcToArrow().
    /// </summary>
    [Benchmark]
    public async Task<int> ReadToArrowBatches()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT f0, f1, f2, f3 FROM bench";
        using var reader = (DbDataReader)await cmd.ExecuteReaderAsync();

        int rowCount = 0;
        await foreach (var batch in AdoToArrow.ReadToArrowBatchesAsync(reader, _config))
        {
            rowCount += batch.Length;
            batch.Dispose();
        }
        return rowCount;
    }

    /// <summary>
    /// Same as ReadToArrowBatches but with DataTypeNameOverrides cleared.
    /// Compares the overhead of the override dictionary TryGetValue calls (one per column
    /// per schema build) against the direct base-resolver path.
    /// </summary>
    [Benchmark]
    public async Task<int> ReadToArrowBatchesNoOverrides()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT f0, f1, f2, f3 FROM bench";
        using var reader = (DbDataReader)await cmd.ExecuteReaderAsync();

        int rowCount = 0;
        await foreach (var batch in AdoToArrow.ReadToArrowBatchesAsync(reader, _configNoOverrides))
        {
            rowCount += batch.Length;
            batch.Dispose();
        }
        return rowCount;
    }

    // ── Write side ───────────────────────────────────────────────────────────

    /// <summary>
    /// E2E write side: iterate a pre-built RecordBatch through RecordBatchDataReader.
    /// Validates the IDataReader adapter layer used by SqlBulkCopy.
    ///
    /// Note: GetValue() boxes per cell — this matches SqlBulkCopy's actual call pattern
    /// and is intentional. The key metric is the absence of an intermediate DataTable
    /// (which would allocate one DataRow per row, one object[] per row, and box every cell).
    /// </summary>
    [Benchmark]
    public int RecordBatchReaderIterate()
    {
        // RecordBatchDataReader does NOT own or dispose the batch.
        using var reader = new RecordBatchDataReader(_recordBatch);
        int sum = 0;
        while (reader.Read())
        {
            var val = reader.GetValue(0);
            if (val is int v) sum += v;
        }
        return sum;
    }

    /// <summary>
    /// Isolated: Int32Binder.Bind() per row — measures single-binder overhead.
    /// Mirrors Java consumeBenchmark() (write direction, single-column variant).
    /// </summary>
    [Benchmark]
    public void BindInt32Column()
    {
        for (int i = 0; i < _int32Array.Length; i++)
            _int32Binder.Bind(_int32Array, i, _param);
    }

    // ── Config builder allocation cost ───────────────────────────────────────

    /// <summary>
    /// Cost of AdoToArrowConfigBuilder.Build() with default DataTypeNameOverrides:
    /// copies the 3-entry override dictionary + allocates a resolver closure.
    /// This is the allocation paid when config is constructed per pipeline call
    /// instead of once at initialization — the anti-pattern fixed in ReadToArrowBatches.
    /// </summary>
    [Benchmark]
    public AdoToArrowConfig BuildConfigDefault()
        => new AdoToArrowConfigBuilder().SetTargetBatchSize(1024).Build();

    /// <summary>
    /// Cost of AdoToArrowConfigBuilder.Build() with no DataTypeNameOverrides:
    /// no dictionary copy, no closure — uses the base resolver function directly.
    /// Provides the lower bound for config construction cost.
    /// </summary>
    [Benchmark]
    public AdoToArrowConfig BuildConfigNoOverrides()
        => new AdoToArrowConfigBuilder().ClearDataTypeNameOverrides().SetTargetBatchSize(1024).Build();
}
