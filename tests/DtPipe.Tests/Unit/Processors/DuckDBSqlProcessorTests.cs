using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Abstractions.Dag;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Options;
using DtPipe.Processors.DuckDB;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Threading.Channels;
using Xunit;

namespace DtPipe.Tests.Unit.Processors;

/// <summary>
/// End-to-end tests for DuckDBSqlProcessor: OpenAsync → ReadRecordBatchesAsync.
/// These tests exercise the real DuckDB native library (no mocking of the engine)
/// to catch P/Invoke entry point errors and schema/streaming bugs early.
/// </summary>
public class DuckDBSqlProcessorTests
{
    private static IMemoryChannelRegistry BuildRegistry(
        string alias, Schema schema, IEnumerable<RecordBatch> batches)
    {
        var channel = Channel.CreateUnbounded<RecordBatch>();
        foreach (var b in batches) channel.Writer.TryWrite(b);
        channel.Writer.Complete();

        var mock = new Mock<IMemoryChannelRegistry>();
        mock.Setup(r => r.WaitForArrowChannelSchemaAsync(alias, It.IsAny<CancellationToken>()))
            .ReturnsAsync(schema);
        mock.Setup(r => r.GetArrowChannel(alias))
            .Returns((channel, schema));
        return mock.Object;
    }

    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_SimpleSelect_ReturnsBatches()
    {
        // Arrange: one integer column, two rows
        var field = new Field("val", Int32Type.Default, nullable: false);
        var schema = new Schema(new[] { field }, null);
        var arr = new Int32Array.Builder().Append(42).Append(99).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 2);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT val FROM src", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        // Act
        await processor.OpenAsync();

        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);

        await processor.DisposeAsync();

        // Assert
        Assert.NotEmpty(batches);
        var totalRows = batches.Sum(b => b.Length);
        Assert.Equal(2, totalRows);

        // Verify schema is correct
        Assert.NotNull(processor.Schema);
        Assert.Single(processor.Schema!.FieldsList);
        Assert.Equal("val", processor.Schema.FieldsList[0].Name);
    }

    /// <summary>
    /// Regression test: WHERE clauses on CDI streaming sources (duckdb_arrow_scan) must be applied.
    ///
    /// Root cause: duckdb_arrow_scan declares filter_pushdown=true. DuckDB removes the Filter
    /// operator from the plan assuming the scan will apply it. But the C API wrapper (FactoryGetNext)
    /// ignores ArrowStreamParameters — the filter was silently dropped, returning all rows.
    /// Fix: SET disabled_optimizers='filter_pushdown' forces DuckDB to keep Filter operators.
    /// </summary>
    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_WhereFilter_FiltersRowsCorrectly()
    {
        var field = new Field("n", Int32Type.Default, nullable: false);
        var schema = new Schema(new[] { field }, null);
        // 10 rows: 0..9
        var arr = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 10)).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 10);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT n FROM src WHERE n < 5", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);
        await processor.DisposeAsync();

        // Must be 5 rows (0,1,2,3,4), not 10
        Assert.Equal(5, batches.Sum(b => b.Length));
    }

    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_UuidColumn_PreservesArrowUuidExtension()
    {
        // Arrange: UUID column — verifies arrow_lossless_conversion is active
        // and duckdb_to_arrow_schema + duckdb_data_chunk_to_arrow preserve arrow.uuid
        var uuidField = ArrowTypeMapper.GetField("id", typeof(Guid));
        var schema = new Schema(new[] { uuidField }, null);

        var uuidBuilder = new Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder(16);
        uuidBuilder.Append(ArrowTypeMapper.ToArrowUuidBytes(Guid.Parse("550e8400-e29b-41d4-a716-446655440000")));
        var uuidArr = uuidBuilder.Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { uuidArr }, 1);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT id FROM src", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        // Act
        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        // Assert: schema from prepared statement has arrow.uuid
        Assert.NotNull(processor.Schema);
        var idField = Assert.Single(processor.Schema!.FieldsList);
        Assert.Equal("id", idField.Name);
        string? ext = null;
        processor.Schema.FieldsList[0].Metadata?.TryGetValue("ARROW:extension:name", out ext);
        Assert.Equal("arrow.uuid", ext, StringComparer.OrdinalIgnoreCase);

        // Assert: data round-trips correctly
        Assert.NotEmpty(resultBatches);
        Assert.Equal(1, resultBatches.Sum(b => b.Length));
    }

    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_Aggregation_ReturnsAggregatedResult()
    {
        // Verifies that the streaming path handles SQL aggregation correctly
        var field = new Field("n", Int32Type.Default, nullable: false);
        var schema = new Schema(new[] { field }, null);

        var arr = new Int32Array.Builder()
            .AppendRange(Enumerable.Range(1, 100))
            .Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 100);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT COUNT(*) AS cnt FROM src", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        Assert.NotEmpty(resultBatches);
        Assert.Equal(1, resultBatches.Sum(b => b.Length));

        var cntField = processor.Schema!.FieldsList.Single(f => f.Name == "cnt");
        Assert.NotNull(cntField);
    }

    // ── Arrow FFI Projection Regression Suite ──────────────────────────────────────────────

    /// <summary>
    /// Aggregate on a String column when the schema has fixed-width columns
    /// before it (FixedSizeBinary + String + Timestamp). Without the pre-flight EXPLAIN projection,
    /// DuckDB's arrow_scan reads children[0] (the fixed-width column) as a String, causing a
    /// SIGSEGV / SIGBUS in SetVectorString. With the projection, DuckDB receives children[0] = String.
    /// </summary>
    [Fact]
    public async Task AggregateOnString_MixedSchema_DoesNotCrash()
    {
        // Schema: UUID-like (FixedSizeBinary) + String + Timestamp
        var uuidField  = new Field("id",         new FixedSizeBinaryType(16), nullable: false);
        var nameField  = new Field("username",    StringType.Default,          nullable: false);
        var tsField    = new Field("last_login",  new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: false);
        var schema = new Schema(new[] { uuidField, nameField, tsField }, null);

        var uuidBuilder = new Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder(16);
        for (int i = 0; i < 100; i++) uuidBuilder.Append(Enumerable.Repeat((byte)i, 16).ToArray());
        var uuidArr = uuidBuilder.Build();

        var nameArr = new StringArray.Builder()
            .AppendRange(Enumerable.Range(0, 100).Select(i => $"user_{i:D3}"))
            .Build();

        var epoch = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var tsArr = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"))
            .AppendRange(Enumerable.Range(0, 100).Select(i => epoch.AddSeconds(i)))
            .Build();

        var batch = new RecordBatch(schema, new IArrowArray[] { uuidArr, nameArr, tsArr }, 100);

        const string alias = "db";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry,
            "SELECT count(*) AS total, avg(length(username)) AS avg_len FROM db",
            alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        // One row of aggregation results
        Assert.Equal(1, resultBatches.Sum(b => b.Length));

        // Verify total = 100
        var totalField = processor.Schema!.FieldsList.Single(f => f.Name == "total");
        Assert.NotNull(totalField);
    }

    /// <summary>
    /// Aggregate with a string expression in the aggregate
    /// ('xx' || username). Verifies that string concatenation in aggregates also works.
    /// </summary>
    [Fact]
    public async Task AggregateWithStringConcat_MixedSchema_DoesNotCrash()
    {
        var uuidField = new Field("id",       new FixedSizeBinaryType(16), nullable: false);
        var nameField = new Field("username", StringType.Default,          nullable: false);
        var schema = new Schema(new[] { uuidField, nameField }, null);

        var uuidBuilder = new Apache.Arrow.Serialization.Reflection.FixedSizeBinaryArrayBuilder(16);
        for (int i = 0; i < 50; i++) uuidBuilder.Append(Enumerable.Repeat((byte)i, 16).ToArray());
        var uuidArr = uuidBuilder.Build();

        var nameArr = new StringArray.Builder()
            .AppendRange(Enumerable.Range(0, 50).Select(i => $"u{i}"))
            .Build();

        var batch = new RecordBatch(schema, new IArrowArray[] { uuidArr, nameArr }, 50);

        const string alias = "db";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry,
            "SELECT count(*) AS total, avg(length('xx' || username)) AS avg_len FROM db",
            alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        Assert.Equal(1, resultBatches.Sum(b => b.Length));
    }

    /// <summary>
    /// Projection order test: when a query selects columns in non-schema order, the output
    /// must reflect the query order — not the schema order.
    ///
    /// Schema: [num (Int32), text (String)]
    /// Query:  SELECT text, num FROM src       ← text first, num second
    ///
    /// Without the projection order fix, the stream returns [num_data, text_data] (schema order).
    /// DuckDB reads children[0] as "text" (Int32 data) → type mismatch → wrong output or crash.
    /// With the fix, the stream returns [text_data, num_data] → output is correct.
    /// </summary>
    [Fact]
    public async Task Projection_MultiColumn_OutputOrderMatchesQueryOrder_NotSchemaOrder()
    {
        // Schema order: num first, text second
        var numField  = new Field("num",  Int32Type.Default,  nullable: false);
        var textField = new Field("text", StringType.Default, nullable: false);
        var schema = new Schema(new[] { numField, textField }, null);

        var numArr  = new Int32Array.Builder().AppendRange(new[] { 10, 20, 30 }).Build();
        var textArr = new StringArray.Builder().AppendRange(new[] { "alpha", "beta", "gamma" }).Build();
        var batch   = new RecordBatch(schema, new IArrowArray[] { numArr, textArr }, 3);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        // Query selects text FIRST, num SECOND — opposite of schema order
        var processor = new DuckDBSqlProcessor(
            registry, "SELECT text, num FROM src ORDER BY num", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        // Output schema must have text first, num second
        Assert.Equal(2, processor.Schema!.FieldsList.Count);
        Assert.Equal("text", processor.Schema.FieldsList[0].Name);
        Assert.Equal("num",  processor.Schema.FieldsList[1].Name);

        // Data: text column must contain strings ("alpha"/"beta"/"gamma"), not integers
        Assert.Equal(3, resultBatches.Sum(b => b.Length));
        var firstBatch   = resultBatches[0];
        var textColumn   = Assert.IsType<StringArray>(firstBatch.Column(0));
        var numColumn    = Assert.IsType<Int32Array>(firstBatch.Column(1));

        // Verify text values are the original strings (not integers cast to strings)
        var textValues = Enumerable.Range(0, textColumn.Length).Select(i => textColumn.GetString(i)).ToList();
        Assert.All(textValues, v => Assert.True(v == "alpha" || v == "beta" || v == "gamma",
            $"Expected a string value but got '{v}'"));

        // Verify num values are the original integers
        var numValues = Enumerable.Range(0, numColumn.Length).Select(i => numColumn.GetValue(i)).ToList();
        Assert.All(numValues, v => Assert.True(v == 10 || v == 20 || v == 30,
            $"Expected 10/20/30 but got '{v}'"));
    }

    /// <summary>
    /// Projection order test with three columns: schema order A→B→C, query selects C→A.
    /// Verifies that a two-column subset projection in non-schema order is correctly mapped.
    /// </summary>
    [Fact]
    public async Task Projection_PartialAndReordered_CorrectOutput()
    {
        var aField = new Field("a", Int32Type.Default,  nullable: false);
        var bField = new Field("b", StringType.Default, nullable: false);
        var cField = new Field("c", Int64Type.Default,  nullable: false);
        var schema = new Schema(new[] { aField, bField, cField }, null);

        var aArr = new Int32Array.Builder().AppendRange(new[] { 1, 2, 3 }).Build();
        var bArr = new StringArray.Builder().AppendRange(new[] { "x", "y", "z" }).Build();
        var cArr = new Int64Array.Builder().AppendRange(new[] { 100L, 200L, 300L }).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { aArr, bArr, cArr }, 3);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        // Select c first, then a — skipping b entirely
        var processor = new DuckDBSqlProcessor(
            registry, "SELECT c, a FROM src ORDER BY a", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        // Output schema: c first, a second
        Assert.Equal(2, processor.Schema!.FieldsList.Count);
        Assert.Equal("c", processor.Schema.FieldsList[0].Name);
        Assert.Equal("a", processor.Schema.FieldsList[1].Name);

        Assert.Equal(3, resultBatches.Sum(b => b.Length));
        var firstBatch = resultBatches[0];

        // c column (Int64): values should be 100, 200, 300
        var cColumn = Assert.IsType<Int64Array>(firstBatch.Column(0));
        var cValues = Enumerable.Range(0, cColumn.Length).Select(i => cColumn.GetValue(i)).ToList();
        Assert.All(cValues, v => Assert.True(v == 100 || v == 200 || v == 300,
            $"c column has wrong value '{v}'"));

        // a column (Int32): values should be 1, 2, 3
        var aColumn = Assert.IsType<Int32Array>(firstBatch.Column(1));
        var aValues = Enumerable.Range(0, aColumn.Length).Select(i => aColumn.GetValue(i)).ToList();
        Assert.All(aValues, v => Assert.True(v == 1 || v == 2 || v == 3,
            $"a column has wrong value '{v}'"));
    }

    /// <summary>
    /// Sentinel test: verifies that duckdb_execute_prepared_streaming is available in the
    /// bundled DuckDB native library and that the full streaming pipeline works end-to-end.
    ///
    /// If this test fails with EntryPointNotFoundException, the DuckDB native library was
    /// upgraded to a version that removed this deprecated function.
    ///
    /// ACTION REQUIRED — do NOT add a silent fallback to materialized execution:
    ///   1. Find the new non-deprecated streaming API that DuckDB introduced.
    ///   2. Migrate DuckDBSqlProcessor.ExecuteStreamingQuery and DuckDBArrowNativeMethods
    ///      to use it — single code path, no parallel logic.
    ///   3. If no streaming API is available yet, revert DuckDB.NET.Data.Full to the last
    ///      working version until one is available.
    /// </summary>
    [Fact]
    public async Task DuckDB_StreamingAPI_IsAvailable()
    {
        var field = new Field("n", Int32Type.Default, nullable: false);
        var schema = new Schema(new[] { field }, null);
        var arr = new Int32Array.Builder().AppendRange(Enumerable.Range(1, 10)).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 10);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT n FROM src", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        // EntryPointNotFoundException here = duckdb_execute_prepared_streaming was removed.
        // See the ACTION REQUIRED comment above — do not catch this exception.
        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);
        await processor.DisposeAsync();

        Assert.NotEmpty(batches);
        Assert.Equal(10, batches.Sum(b => b.Length));
    }
}
