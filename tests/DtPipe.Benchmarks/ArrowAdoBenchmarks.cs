using Apache.Arrow;
using Apache.Arrow.Ado;
using Apache.Arrow.Ado.Binder;
using Apache.Arrow.Ado.Consumer;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using DtPipe.Core.Helpers;
using Microsoft.Data.Sqlite;
using System.Data.Common;

namespace DtPipe.Benchmarks;

/// <summary>
/// Benchmarks for the Apache.Arrow.Ado adapter — read side (AdoToArrow) and write side
/// (IArrowBinder, RecordBatchDataReader) — plus a ColumnConverterFactory regression check.
///
/// Mirrors the structure of JdbcAdapterBenchmarks.java in the Apache Arrow Java project
/// (performance/src/main/java/org/apache/arrow/adapter/jdbc/JdbcAdapterBenchmarks.java).
/// Uses SQLite in-memory as H2 is used in Java — no external infrastructure required.
///
/// Run (release mode required for meaningful numbers):
///   dotnet run -c Release --project tests/DtPipe.Benchmarks -- --filter "*ArrowAdo*"
///   dotnet run -c Release --project tests/DtPipe.Benchmarks -- --filter "*"
///   NOTE: do not use --job Dry — it adds a standard toolchain job that scans the full
///   solution directory and hits the permission-restricted tests/artifacts/restricted fixture.
///
/// Key metric: Allocated bytes/op (reported by [MemoryDiagnoser]).
/// A zero or near-zero allocation confirms the zero-boxing paths are working.
///
/// InProcessEmitToolchain is used to avoid BenchmarkDotNet's directory scan hitting
/// the tests/artifacts/restricted fixture (d---------). Results are equivalent to
/// the default out-of-process toolchain for these micro-benchmarks.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(InProcessConfig))]
public class ArrowAdoBenchmarks
{
    private const int RowCount = 3000;

    // Shared SQLite connection — kept open for the trial
    private SqliteConnection _conn = null!;

    // Pre-built RecordBatch for the write-side benchmarks
    private RecordBatch _recordBatch = null!;

    // Int32Array extracted from the batch — used in isolated binder benchmark
    private Int32Array _int32Array = null!;

    // Reusable output parameter for the binder benchmark (avoids parameter-creation noise)
    private SqliteParameter _param = null!;

    // Reusable consumer for the isolated column-consume benchmark
    private Int32Consumer _int32Consumer = null!;

    // Reusable binder for the isolated column-bind benchmark
    private Int32Binder _int32Binder = null!;

    // Pre-compiled converter delegate for the ColumnConverter comparison
    private Func<object?, object?> _int32ConverterDelegate = null!;

    // ── Setup / Teardown ─────────────────────────────────────────────────────

    [GlobalSetup]
    public async Task Setup()
    {
        // ── SQLite in-memory database with RowCount rows ──
        // Schema mirrors Java: (int, long, string, bool) → f0, f1, f2, f3
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

        // ── Pre-built RecordBatch for write-side benchmarks ──
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

        // ── Reusable per-benchmark objects ──
        _param               = new SqliteParameter();
        _int32Consumer       = new Int32Consumer(0);
        _int32Binder         = new Int32Binder();
        _int32ConverterDelegate = ColumnConverterFactory.Build(typeof(string), typeof(int));
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
    /// Mirrors Java testJdbcToArrow() — measures full IAdoConsumer pipeline overhead.
    /// </summary>
    [Benchmark]
    public async Task<int> ReadToArrowBatches()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT f0, f1, f2, f3 FROM bench";
        using var reader = (DbDataReader)await cmd.ExecuteReaderAsync();

        var config = new AdoToArrowConfigBuilder().SetTargetBatchSize(1024).Build();
        int rowCount = 0;
        await foreach (var batch in AdoToArrow.ReadToArrowBatchesAsync(reader, config))
        {
            rowCount += batch.Length;
            batch.Dispose();
        }
        return rowCount;
    }

    /// <summary>
    /// Isolated: Int32Consumer.Consume() per row — measures single-consumer overhead.
    /// Mirrors Java consumeBenchmark() (single-column variant).
    /// Expected allocation: only the final IArrowArray build (not per-row).
    /// </summary>
    [Benchmark]
    public async Task<IArrowArray> ConsumeInt32Column()
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = "SELECT f0 FROM bench";
        using var reader = (DbDataReader)await cmd.ExecuteReaderAsync();

        _int32Consumer.Reset();
        while (await reader.ReadAsync())
            _int32Consumer.Consume(reader);

        return _int32Consumer.BuildArray();
    }

    // ── Write side — no Java equivalent ──────────────────────────────────────

    /// <summary>
    /// E2E write side: iterate a pre-built RecordBatch through RecordBatchDataReader.
    /// Validates the IDataReader adapter layer used by SqlBulkCopy.
    /// Key metric: allocations/op — should be near zero (no intermediate DataTable).
    /// </summary>
    [Benchmark]
    public int RecordBatchReaderIterate()
    {
        // RecordBatchDataReader does NOT own or dispose the batch
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
    /// Key metric: 0 bytes allocated (reads int from Arrow, assigns int to DbParameter.Value,
    /// which boxes — one allocation per call — but no intermediate objects).
    /// </summary>
    [Benchmark]
    public void BindInt32Column()
    {
        for (int i = 0; i < _int32Array.Length; i++)
            _int32Binder.Bind(_int32Array, i, _param);
    }

    // ── ColumnConverter regression ────────────────────────────────────────────

    /// <summary>
    /// Baseline: ValueConverter.ConvertValue() — the original per-cell slow path.
    /// Involves: Nullable.GetUnderlyingType() + IsInstanceOfType() + if/else cascade + boxing.
    /// </summary>
    [Benchmark(Baseline = true)]
    public object? ConvertValueDirectCall()
    {
        object? last = null;
        for (int i = 0; i < RowCount; i++)
            last = ValueConverter.ConvertValue("42", typeof(int));
        return last;
    }

    /// <summary>
    /// Optimized: pre-built ColumnConverterFactory delegate — the Phase 2 replacement.
    /// The type-specific parse lambda was compiled once at writer initialization.
    /// Expected: measurably lower overhead than the baseline (no reflection per call).
    /// </summary>
    [Benchmark]
    public object? ColumnConverterDelegate()
    {
        object? last = null;
        for (int i = 0; i < RowCount; i++)
            last = _int32ConverterDelegate("42");
        return last;
    }
}

/// <summary>
/// BenchmarkDotNet config that uses InProcessEmitToolchain.
/// Avoids the directory scanner hitting permission-restricted test fixtures
/// (tests/artifacts/restricted). Results are equivalent to the default out-of-process
/// toolchain for micro-benchmarks that do not measure startup time.
/// </summary>
internal class InProcessConfig : ManualConfig
{
    public InProcessConfig()
    {
        AddJob(Job.MediumRun.WithToolchain(InProcessEmitToolchain.Instance));
    }
}
