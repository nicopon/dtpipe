using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Serialization.Reflection;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using DtPipe.Adapters.Parquet;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Parquet;
using Parquet.Schema;

// Apache.Arrow ships an ArrowArrayFactory of its own; the one under test is dtpipe's.
using DtPipeArrowArrayFactory = DtPipe.Core.Infrastructure.Arrow.ArrowArrayFactory;

namespace DtPipe.Benchmarks;

/// <summary>
/// The reader stage, which every other micro-benchmark in this project takes as given.
///
/// PgToCsvCellBenchmarks splits the cost of a transfer across the Arrow build, the
/// columnar→row bridge and the writer — three stages that together account for a small
/// fraction of a real run. The stage that carries the rest, the reader, has no control
/// benchmark: the macro family has B16 ("the pipeline that does nothing"), this layer had
/// no equivalent. These benchmarks are that control, on Parquet — the bench's canonical
/// source, and unlike Npgsql a decode path the project drives itself.
///
/// The decomposition, all on one file with the bench dataset's schema:
///
///   P0  open        — ParquetReader.CreateAsync: footer, schema, no rows. Per file, not per cell.
///   P1  whole stage — OpenAsync + ReadRecordBatchesAsync to exhaustion. P1 − P0 is what a cell
///                     costs to reach the first transformer.
///   P2  decode      — the same read stopping at Parquet.Net's CLR arrays, no Arrow. The half
///                     that is not dtpipe code.
///   P3  convert     — ArrowArrayFactory.Create over already-decoded columns. The half that is.
///                     P2 + P3 ≈ P1 is the cross-check that the split is real and not arithmetic.
///   P4  convert, typed — the same conversion written against the concrete builders. P3 − P4 is
///                     what the object?-shaped appender costs, and therefore what is available.
///   P5..P7          — P3 per column type, so the total can be attributed to a type.
///   P8              — the Guid column with nothing allocated per cell. P5 − P8 is the price of
///                     FixedSizeBinaryArrayBuilder's List&lt;byte[]&gt;, on the reader's costliest column.
///
/// Divide any Mean by CellCount (P0 excepted) to get nanoseconds per cell.
///
/// Run:
///   dotnet run -c Release --project tests/DtPipe.Benchmarks -- --filter "*ParquetReaderCell*"
/// </summary>
[MemoryDiagnoser]
public class ParquetReaderCellBenchmarks
{
    /// <summary>
    /// Rows per row group. ParquetDataWriter opens one row group per RecordBatch it is handed,
    /// so a file written by dtpipe has row groups of PipelineOptions.DefaultBatchSize — which is
    /// also what ParquetStreamReader will yield back as one RecordBatch.
    /// </summary>
    private const int RowsPerGroup = 32_768;

    /// <summary>More than one, so per-row-group work is inside the measurement rather than amortised away.</summary>
    private const int RowGroupCount = 2;

    private const int RowCount = RowsPerGroup * RowGroupCount;
    private const int ColumnCount = 5;

    /// <summary>Cells touched by every whole-file benchmark, for the ns/cell division.</summary>
    public const int CellCount = RowCount * ColumnCount;

    /// <summary>The bench dataset verbatim: two of the five columns are the expensive types.</summary>
    private static readonly Type[] ColumnClrTypes =
        [typeof(Guid), typeof(string), typeof(string), typeof(decimal), typeof(string)];

    private static readonly string[] CountryCodes = ["FR", "DE", "US", "JP", "BR", "IN", "ZA", "CA"];

    private string _path = null!;

    // Columns decoded once, so the Arrow conversion can be measured without the file.
    // Indexed [rowGroup][column].
    private System.Array[][] _decoded = null!;

    // ── Setup / Teardown ─────────────────────────────────────────────────────

    [GlobalSetup]
    public async Task Setup()
    {
        _path = Path.Combine(Path.GetTempPath(), $"dtpipe_bench_{Guid.NewGuid():N}.parquet");
        await WriteFixtureAsync(_path);

        _decoded = new System.Array[RowGroupCount][];
        await using var stream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read);
        await using var parquet = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true);
        for (int g = 0; g < parquet.RowGroupCount; g++)
        {
            using var group = parquet.OpenRowGroupReader(g);
            _decoded[g] = new System.Array[ColumnCount];
            for (int c = 0; c < ColumnCount; c++)
                _decoded[g][c] = await DecodeColumnAsync(group, parquet.Schema.DataFields[c], (int)group.RowCount);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (File.Exists(_path)) File.Delete(_path);
    }

    // ── P0 / P1 — the stage as the executor drives it ────────────────────────

    /// <summary>
    /// Everything ParquetStreamReader.OpenAsync does before a row moves: the access probe, the
    /// FileStream, the footer parse, and the PipeColumnInfo list. A fixed cost per file, which
    /// is why it is reported per operation and not per cell.
    /// </summary>
    [Benchmark(Description = "P0 Reader — open only (footer + schema, no rows)")]
    public async Task<int> P0_OpenOnly()
    {
        await using var reader = new ParquetStreamReader(_path);
        await reader.OpenAsync();
        return reader.Columns!.Count;
    }

    /// <summary>
    /// The whole reader stage, exactly as PipelineExecutor consumes it — including the batches'
    /// disposal, which is the executor's job and not free.
    /// </summary>
    [Benchmark(Description = "P1 Reader — whole stage (open + all record batches)")]
    public async Task<int> P1_WholeStage()
    {
        await using var reader = new ParquetStreamReader(_path);
        await reader.OpenAsync();
        int rows = 0;
        await foreach (var batch in reader.ReadRecordBatchesAsync())
        {
            using (batch) rows += batch.Length;
        }
        return rows;
    }

    // ── P2 — Parquet.Net's half ──────────────────────────────────────────────

    /// <summary>
    /// The same file, stopping at the CLR arrays Parquet.Net produces: page decompression,
    /// dictionary and definition levels, the T?[] each column lands in. No Arrow at all.
    /// </summary>
    [Benchmark(Description = "P2 Reader — decode only (Parquet.Net → CLR arrays)")]
    public async Task<int> P2_DecodeOnly()
    {
        await using var stream = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.Read);
        await using var parquet = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true);
        int cells = 0;
        for (int g = 0; g < parquet.RowGroupCount; g++)
        {
            using var group = parquet.OpenRowGroupReader(g);
            for (int c = 0; c < ColumnCount; c++)
                cells += (await DecodeColumnAsync(group, parquet.Schema.DataFields[c], (int)group.RowCount)).Length;
        }
        return cells;
    }

    // ── P3 / P4 — dtpipe's half, and its counterfactual ──────────────────────

    /// <summary>
    /// ArrowArrayFactory.Create over the decoded columns: the loop the reader runs per row group.
    /// It walks a System.Array through the non-generic enumerator — one box per value — and hands
    /// each boxed value to an Action&lt;object?&gt;, so every cell pays a box, a delegate invoke and a
    /// virtual call before the typed builder sees it.
    /// </summary>
    [Benchmark(Description = "P3 Reader — Arrow conversion (ArrowArrayFactory)")]
    public int P3_ArrowConversion()
    {
        int n = 0;
        for (int g = 0; g < RowGroupCount; g++)
        {
            for (int c = 0; c < ColumnCount; c++)
            {
                var array = DtPipeArrowArrayFactory.Create(_decoded[g][c], ColumnClrTypes[c], isNullable: true);
                n += array.Length;
                array.Dispose();
            }
        }
        return n;
    }

    /// <summary>
    /// The same conversion, same output type, written against the concrete builder for each
    /// column type — no box, no delegate, no handler. The gap against P3 is the price of the
    /// object?-shaped appender, and it is the only part of this stage dtpipe can remove.
    /// </summary>
    [Benchmark(Description = "P4 Reader — Arrow conversion, typed builders (counterfactual)")]
    public int P4_ArrowConversion_Typed()
    {
        int n = 0;
        for (int g = 0; g < RowGroupCount; g++)
        {
            for (int c = 0; c < ColumnCount; c++)
            {
                var array = BuildTyped(_decoded[g][c]);
                n += array.Length;
                array.Dispose();
            }
        }
        return n;
    }

    // ── P5..P7 — the same conversion, one column type at a time ──────────────

    [Benchmark(Description = "P5 Convert — Guid column (FixedSizeBinary + arrow.uuid)")]
    public int P5_Convert_Guid() => ConvertColumn(0);

    [Benchmark(Description = "P6 Convert — decimal column (Decimal128)")]
    public int P6_Convert_Decimal() => ConvertColumn(3);

    [Benchmark(Description = "P7 Convert — string column")]
    public int P7_Convert_String() => ConvertColumn(1);

    /// <summary>
    /// The decimal column with no box and no delegate, at the Arrow type the mapper actually
    /// returns for a CLR decimal — Decimal128(38, 18). Against P6 this isolates the appender;
    /// against P10 it isolates what the scale costs.
    /// </summary>
    [Benchmark(Description = "P9 Convert — decimal column, typed builder at Decimal128(38,18)")]
    public int P9_Convert_Decimal_Typed() => BuildDecimalColumn(38, 18);

    /// <summary>
    /// The same values into a Decimal128 whose scale is the one the data has. Arrow rescales every
    /// cell whose scale is below the vector's — DecimalUtility.GetBytes multiplies by
    /// BigInteger.Pow(10, scale − decScale) per value, and validates against BigInteger.Pow(10,
    /// precision) per value. Neither Pow is hoisted, so both are paid 65 536 times.
    /// </summary>
    [Benchmark(Description = "P10 Convert — decimal column, scale matching the data (38,2)")]
    public int P10_Convert_Decimal_TypedMatchingScale() => BuildDecimalColumn(38, 2);

    /// <summary>
    /// The Guid column with nothing allocated per cell: the 16 bytes are written straight into the
    /// data buffer through Guid.TryWriteBytes(bigEndian: true), and validity into the bitmap.
    /// FixedSizeBinaryArrayBuilder instead keeps a List&lt;byte[]&gt; — one heap array per cell, filled
    /// from a second one that ToArrowUuidBytes returns — and copies the whole list into the buffer
    /// at Build(). The gap against P5 is what that shape costs on the reader's hottest column.
    /// The buffers here are managed rather than native, which changes who frees them, not what the
    /// per-cell loop does.
    /// </summary>
    [Benchmark(Description = "P8 Convert — Guid column, no per-cell allocation (counterfactual)")]
    public int P8_Convert_Guid_NoAlloc()
    {
        int n = 0;
        for (int g = 0; g < RowGroupCount; g++)
        {
            var guids = (Guid?[])_decoded[g][0];
            var data = new byte[guids.Length * 16];
            var valid = new byte[(guids.Length + 7) / 8];
            int nulls = 0;

            for (int i = 0; i < guids.Length; i++)
            {
                if (guids[i] is { } value)
                {
                    value.TryWriteBytes(data.AsSpan(i * 16, 16), bigEndian: true, out _);
                    valid[i / 8] |= (byte)(1 << (i % 8));
                }
                else nulls++;
            }

            var array = new FixedSizeBinaryArray(new ArrayData(
                new FixedSizeBinaryType(16), guids.Length, nulls, offset: 0,
                buffers: [new ArrowBuffer(valid), new ArrowBuffer(data)]));
            n += array.Length;
            array.Dispose();
        }
        return n;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private int BuildDecimalColumn(int precision, int scale)
    {
        int n = 0;
        for (int g = 0; g < RowGroupCount; g++)
        {
            var amounts = (decimal?[])_decoded[g][3];
            var builder = new Decimal128Array.Builder(new Decimal128Type(precision, scale));
            foreach (var value in amounts)
            {
                if (value.HasValue) builder.Append(value.Value);
                else builder.AppendNull();
            }
            var array = builder.Build();
            n += array.Length;
            array.Dispose();
        }
        return n;
    }

    private int ConvertColumn(int column)
    {
        int n = 0;
        for (int g = 0; g < RowGroupCount; g++)
        {
            var array = DtPipeArrowArrayFactory.Create(_decoded[g][column], ColumnClrTypes[column], isNullable: true);
            n += array.Length;
            array.Dispose();
        }
        return n;
    }

    /// <summary>
    /// Mirrors ParquetStreamReader.ReadColumnDataAsArrayAsync for the three types this schema
    /// uses. That method is private, and reading through it would drag the Arrow conversion in —
    /// which is the thing P2 exists to leave out. Keep the call shapes identical to the reader's:
    /// the destination being T?[] rather than T[] is part of what is being measured.
    /// </summary>
    private static async Task<System.Array> DecodeColumnAsync(ParquetRowGroupReader group, DataField field, int rowCount)
    {
        var clrType = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;

        if (clrType == typeof(ReadOnlyMemory<char>) || clrType == typeof(string))
        {
            var data = new string?[rowCount];
            await group.ReadAsync(field, data.AsMemory(), null, default);
            return data;
        }
        if (clrType == typeof(decimal))
        {
            var data = new decimal?[rowCount];
            await group.ReadAsync<decimal>(field, data);
            return data;
        }
        if (clrType == typeof(Guid))
        {
            var data = new Guid?[rowCount];
            await group.ReadAsync<Guid>(field, data);
            return data;
        }

        throw new NotSupportedException($"Benchmark fixture has no column of type {clrType.Name}.");
    }

    /// <summary>
    /// Builds the same Arrow array as ArrowArrayFactory.Create, taking the Arrow type from the
    /// same mapper so the two produce identical output — a counterfactual that changed the type
    /// would measure a different job.
    /// </summary>
    private static IArrowArray BuildTyped(System.Array data)
    {
        switch (data)
        {
            case Guid?[] guids:
            {
                var width = ((FixedSizeBinaryType)ArrowTypeMapper.GetLogicalType(typeof(Guid)).ArrowType).ByteWidth;
                var builder = new FixedSizeBinaryArrayBuilder(width);
                foreach (var value in guids)
                {
                    if (value.HasValue) builder.Append(ArrowTypeMapper.ToArrowUuidBytes(value.Value));
                    else builder.AppendNull();
                }
                return builder.Build();
            }
            case decimal?[] amounts:
            {
                var type = (Decimal128Type)ArrowTypeMapper.GetLogicalType(typeof(decimal)).ArrowType;
                var builder = new Decimal128Array.Builder(type);
                foreach (var value in amounts)
                {
                    if (value.HasValue) builder.Append(value.Value);
                    else builder.AppendNull();
                }
                return builder.Build();
            }
            case string?[] texts:
            {
                var builder = new StringArray.Builder();
                foreach (var value in texts)
                {
                    if (value is null) builder.AppendNull();
                    else builder.Append(value);
                }
                return builder.Build();
            }
            default:
                throw new NotSupportedException($"No typed builder for {data.GetType().Name}.");
        }
    }

    /// <summary>
    /// Writes the fixture through dtpipe's own ParquetDataWriter, so the row groups, encodings
    /// and physical types are the ones the bench's source file actually has.
    /// </summary>
    private static async Task WriteFixtureAsync(string path)
    {
        var columns = new[]
        {
            new PipeColumnInfo("id", typeof(Guid), true),
            new PipeColumnInfo("name", typeof(string), true),
            new PipeColumnInfo("email", typeof(string), true),
            new PipeColumnInfo("amount", typeof(decimal), true),
            new PipeColumnInfo("country", typeof(string), true),
        };

        var schema = ArrowSchemaFactory.Create(columns);
        var writer = new ParquetDataWriter(path);
        await using (writer)
        {
            await writer.InitializeAsync(columns);
            var rng = new Random(20260910);
            for (int g = 0; g < RowGroupCount; g++)
                await writer.WriteRecordBatchAsync(BuildBatch(schema, rng));
            await writer.CompleteAsync();
        }
    }

    private static RecordBatch BuildBatch(Schema schema, Random rng)
    {
        var ids = new FixedSizeBinaryArrayBuilder(16);
        var names = new StringArray.Builder();
        var emails = new StringArray.Builder();
        var amounts = new Decimal128Array.Builder((Decimal128Type)ArrowTypeMapper.GetLogicalType(typeof(decimal)).ArrowType);
        var countries = new StringArray.Builder();

        for (int i = 0; i < RowsPerGroup; i++)
        {
            ids.Append(ArrowTypeMapper.ToArrowUuidBytes(Guid.NewGuid()));
            names.Append($"Firstname{i} Lastname{i}");
            emails.Append($"user{i}.sample@example.com");
            amounts.Append(Math.Round((decimal)(rng.NextDouble() * 10000), 2));
            countries.Append(CountryCodes[i % CountryCodes.Length]);
        }

        return new RecordBatch(
            schema,
            [ids.Build(), names.Build(), emails.Build(), amounts.Build(), countries.Build()],
            RowsPerGroup);
    }
}
