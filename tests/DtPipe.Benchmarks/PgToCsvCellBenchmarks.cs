using Apache.Arrow;
using Apache.Arrow.Serialization.Reflection;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using DtPipe.Adapters.Csv;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;

namespace DtPipe.Benchmarks;

/// <summary>
/// Where the ~470 ns per cell of a PostgreSQL → CSV transfer actually go.
///
/// The competitive bench (experiments/dtpipe-sandbox) measures the pipeline end to
/// end: 1M rows x 5 columns in ~2333 ms is 429 k rows/s, about 470 ns per cell. That
/// figure cannot say whether it is the floor of the .NET client path or whether a
/// factor of 2-3 is sitting there unclaimed. This class splits the same schema across
/// the three stages dtpipe owns, so the total can be attributed:
///
///   A. Arrow build   — PostgreSqlReader appending each cell into an Arrow builder
///                      (the Npgsql socket and binary-COPY parse are NOT dtpipe code
///                      and are deliberately out of scope here)
///   B. Bridge        — RecordBatch → object?[] rows, the columnar→row materialization
///                      the executor performs because CsvDataWriter is an IRowDataWriter
///   C. CSV write     — CsvDataWriter.FormatValue + CsvHelper + the periodic flush
///
/// The schema is the bench dataset verbatim: id (Guid), name, email (string),
/// amount (decimal / NUMERIC), country (string). Two of the five columns are the
/// expensive types the plan names as suspects.
///
/// Per-type variants isolate the third suspect: ArrowTypeMap.GetValue dispatches
/// through a linear type-pattern chain, so a column's position in that chain is part
/// of its cost. Int32 is checked 4th, StringArray 12th, Decimal128Array 14th.
///
/// Divide any Mean by CellCount to get nanoseconds per cell.
///
/// Run:
///   dotnet run -c Release --project tests/DtPipe.Benchmarks -- --filter "*PgToCsvCell*"
/// </summary>
[MemoryDiagnoser]
public class PgToCsvCellBenchmarks
{
    /// <summary>Rows per batch — the pipeline default batch size order of magnitude.</summary>
    private const int RowCount = 2048;

    private const int ColumnCount = 5;

    /// <summary>Cells touched by every five-column benchmark, for the ns/cell division.</summary>
    public const int CellCount = RowCount * ColumnCount;

    // ── Source CLR data, generated once ──────────────────────────────────────
    private Guid[] _ids = null!;
    private string[] _names = null!;
    private string[] _emails = null!;
    private decimal[] _amounts = null!;
    private string[] _countries = null!;
    private int[] _ints = null!;

    // ── Pre-built Arrow batches ──────────────────────────────────────────────
    private Schema _schema = null!;
    private RecordBatch _batch = null!;
    private RecordBatch _guidBatch = null!;
    private RecordBatch _decimalBatch = null!;
    private RecordBatch _stringBatch = null!;
    private RecordBatch _int32Batch = null!;

    // ── Pre-materialized rows, so the CSV stage is measured on its own ───────
    private object?[][] _rows = null!;
    private IReadOnlyList<PipeColumnInfo> _columns = null!;

    private CsvDataWriter _csvWriter = null!;
    private string _csvPath = null!;

    // ── Setup / Teardown ─────────────────────────────────────────────────────

    [GlobalSetup]
    public async Task Setup()
    {
        var rng = new Random(20260828);

        _ids = new Guid[RowCount];
        _names = new string[RowCount];
        _emails = new string[RowCount];
        _amounts = new decimal[RowCount];
        _countries = new string[RowCount];
        _ints = new int[RowCount];

        for (int i = 0; i < RowCount; i++)
        {
            _ids[i] = Guid.NewGuid();
            _names[i] = $"Firstname{i} Lastname{i}";
            _emails[i] = $"user{i}.sample@example.com";
            _amounts[i] = Math.Round((decimal)(rng.NextDouble() * 10000), 2);
            _countries[i] = CountryCodes[i % CountryCodes.Length];
            _ints[i] = rng.Next();
        }

        _schema = BuildSchema();
        _batch = BuildBatch();

        _guidBatch = SingleColumnBatch(_schema.GetFieldByIndex(0), BuildUuidArray());
        _stringBatch = SingleColumnBatch(_schema.GetFieldByIndex(1), BuildStringArray(_names));
        _decimalBatch = SingleColumnBatch(_schema.GetFieldByIndex(3), BuildDecimalArray());

        var int32Field = new Field("value", Int32Type.Default, nullable: true);
        _int32Batch = SingleColumnBatch(int32Field, BuildInt32Array());

        // Rows exactly as the executor hands them to the writer.
        _rows = new object?[RowCount][];
        for (int i = 0; i < RowCount; i++)
        {
            var view = new ArrowRowView(_batch, i, NameIndex);
            _rows[i] = view.ToArray();
        }

        _columns = new[]
        {
            new PipeColumnInfo("id", typeof(Guid), true),
            new PipeColumnInfo("name", typeof(string), true),
            new PipeColumnInfo("email", typeof(string), true),
            new PipeColumnInfo("amount", typeof(decimal), true),
            new PipeColumnInfo("country", typeof(string), true),
        };

        _csvPath = Path.Combine(Path.GetTempPath(), $"dtpipe_bench_{Guid.NewGuid():N}.csv");
        _csvWriter = new CsvDataWriter(_csvPath, new CsvWriterOptions());
        await _csvWriter.InitializeAsync(_columns);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _csvWriter.CompleteAsync();
        await _csvWriter.DisposeAsync();
        if (File.Exists(_csvPath)) File.Delete(_csvPath);
    }

    // ── A. Arrow build — the reader side ─────────────────────────────────────

    /// <summary>
    /// What PostgreSqlReader does per batch once Npgsql has handed it the values:
    /// one builder append per cell, then Build() per column. The UUID column pays
    /// ToArrowUuidBytes (a byte[16] allocation) plus FixedSizeBinaryArrayBuilder.Append
    /// (a second byte[16] allocation and a List&lt;byte[]&gt; slot).
    /// </summary>
    [Benchmark(Description = "A1 Arrow build — 5 columns (reader side)")]
    public RecordBatch A1_ArrowBuild_FiveColumns() => BuildBatch();

    [Benchmark(Description = "A2 Arrow build — Guid column only")]
    public IArrowArray A2_ArrowBuild_Guid() => BuildUuidArray();

    [Benchmark(Description = "A3 Arrow build — decimal column only")]
    public IArrowArray A3_ArrowBuild_Decimal() => BuildDecimalArray();

    [Benchmark(Description = "A4 Arrow build — string column only")]
    public IArrowArray A4_ArrowBuild_String() => BuildStringArray(_names);

    // ── B. The columnar → row bridge ─────────────────────────────────────────

    /// <summary>
    /// The materialization the executor performs, through the same async bridge.
    /// ArrowColumnarToRowBridge yields an ArrowRowView struct as IReadOnlyList&lt;object?&gt;,
    /// and PipelineExecutor dispatches on the concrete type so the view sizes its array
    /// once. The gap against B2 is what the IAsyncEnumerable machinery costs; the gap
    /// against B3 is the whole price of handing the writer a row array.
    /// </summary>
    [Benchmark(Description = "B1 Bridge — 5 columns, as the executor materializes")]
    public async Task<int> B1_Bridge_AsExecutorDoes()
    {
        var bridge = new ArrowColumnarToRowBridge();
        int n = 0;
        await foreach (var row in bridge.ConvertBatchToRowsAsync(_batch))
        {
            var materialized = row switch
            {
                object?[] arr => arr,
                ArrowRowView view => view.ToArray(),
                _ => System.Linq.Enumerable.ToArray(row),
            };
            n += materialized.Length;
        }
        return n;
    }

    /// <summary>
    /// The same materialization without the async bridge: ArrowRowView.ToArray over a
    /// plain loop, extracting exactly the same values.
    /// </summary>
    [Benchmark(Description = "B2 Bridge — 5 columns, via ArrowRowView.ToArray")]
    public int B2_Bridge_ViaViewToArray()
    {
        int n = 0;
        for (int i = 0; i < RowCount; i++)
        {
            var view = new ArrowRowView(_batch, i, NameIndex);
            n += view.ToArray().Length;
        }
        return n;
    }

    /// <summary>
    /// Cell extraction alone, no row array at all: the floor the bridge cannot beat.
    /// </summary>
    [Benchmark(Description = "B3 Bridge — 5 columns, value extraction only")]
    public object? B3_Bridge_ExtractionOnly()
    {
        object? last = null;
        for (int i = 0; i < RowCount; i++)
            for (int c = 0; c < ColumnCount; c++)
                last = ArrowTypeMapper.GetValueForField(_batch.Column(c), _schema.GetFieldByIndex(c), i);
        return last;
    }

    // Per-type extraction: same call, one column, RowCount cells.

    [Benchmark(Description = "B4 Extract — Guid cells (FixedSizeBinary + arrow.uuid)")]
    public object? B4_Extract_Guid() => ExtractColumn(_guidBatch);

    [Benchmark(Description = "B5 Extract — decimal cells (Decimal128)")]
    public object? B5_Extract_Decimal() => ExtractColumn(_decimalBatch);

    [Benchmark(Description = "B6 Extract — string cells")]
    public object? B6_Extract_String() => ExtractColumn(_stringBatch);

    /// <summary>
    /// Reference point for the type-dispatch chain: Int32Array is matched 4th in
    /// ArrowTypeMap.GetValue's pattern switch, Decimal128Array 14th. The difference
    /// against B5, minus the cost of the decimal read itself, is the chain.
    /// </summary>
    [Benchmark(Description = "B7 Extract — int32 cells (cheap type, early in dispatch)")]
    public object? B7_Extract_Int32() => ExtractColumn(_int32Batch);

    /// <summary>
    /// The direction a row reader feeding a columnar writer pays on every cell — CSV to
    /// Parquet is the canonical shape — and a mixed pipeline pays on top of B1. Rows go
    /// back into Arrow via ArrowRowConverter.ToRecordBatch, which resolves one appender
    /// per column and reuses it for the whole batch. Compare against B1: the two
    /// directions of the same bridge are not symmetric, and the macro bench (B19) only
    /// sees their sum.
    /// </summary>
    [Benchmark(Description = "B8 Bridge — rows back to columnar (ArrowRowConverter)")]
    public RecordBatch B8_Bridge_RowsToColumnar() =>
        ArrowRowConverter.ToRecordBatch(_schema, _rows, RowCount);

    // ── C. CSV formatting and write ──────────────────────────────────────────

    /// <summary>
    /// CsvDataWriter over rows that are already materialized: FormatValue per cell
    /// (which calls Nullable.GetUnderlyingType on every cell), CsvHelper field
    /// escaping, and the flush to file every 1000 rows. Real I/O is included —
    /// it is part of the 470 ns and excluding it would flatter the result.
    /// </summary>
    [Benchmark(Description = "C1 CSV — format + write 5 columns")]
    public async Task C1_CsvWrite() => await _csvWriter.WriteBatchAsync(_rows);

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static readonly string[] CountryCodes = ["FR", "DE", "US", "JP", "BR", "IN", "ZA", "CA"];

    private static readonly Dictionary<string, int> NameIndex =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = 0, ["name"] = 1, ["email"] = 2, ["amount"] = 3, ["country"] = 4,
        };

    private static Schema BuildSchema()
    {
        var uuidMetadata = new Dictionary<string, string> { ["ARROW:extension:name"] = "arrow.uuid" };
        return new Schema(
            [
                new Field("id", new FixedSizeBinaryType(16), nullable: true, uuidMetadata),
                new Field("name", StringType.Default, nullable: true),
                new Field("email", StringType.Default, nullable: true),
                new Field("amount", new Decimal128Type(38, 2), nullable: true),
                new Field("country", StringType.Default, nullable: true),
            ],
            metadata: null);
    }

    private RecordBatch BuildBatch() => new(
        _schema,
        [
            BuildUuidArray(),
            BuildStringArray(_names),
            BuildStringArray(_emails),
            BuildDecimalArray(),
            BuildStringArray(_countries),
        ],
        RowCount);

    private static RecordBatch SingleColumnBatch(Field field, IArrowArray array) =>
        new(new Schema([field], metadata: null), [array], RowCount);

    private IArrowArray BuildUuidArray()
    {
        var builder = new FixedSizeBinaryArrayBuilder(16);
        for (int i = 0; i < RowCount; i++)
            builder.Append(ArrowTypeMapper.ToArrowUuidBytes(_ids[i]));
        return builder.Build();
    }

    private IArrowArray BuildDecimalArray()
    {
        var builder = new Decimal128Array.Builder(new Decimal128Type(38, 2));
        for (int i = 0; i < RowCount; i++)
            builder.Append(_amounts[i]);
        return builder.Build();
    }

    private static IArrowArray BuildStringArray(string[] values)
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < values.Length; i++)
            builder.Append(values[i]);
        return builder.Build();
    }

    private IArrowArray BuildInt32Array()
    {
        var builder = new Int32Array.Builder();
        for (int i = 0; i < RowCount; i++)
            builder.Append(_ints[i]);
        return builder.Build();
    }

    private static object? ExtractColumn(RecordBatch batch)
    {
        var array = batch.Column(0);
        var field = batch.Schema.GetFieldByIndex(0);
        object? last = null;
        for (int i = 0; i < RowCount; i++)
            last = ArrowTypeMapper.GetValueForField(array, field, i);
        return last;
    }
}
