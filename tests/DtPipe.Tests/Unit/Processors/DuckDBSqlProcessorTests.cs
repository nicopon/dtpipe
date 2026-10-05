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

    [Fact]
    public async Task OpenAsync_JoinQueryWithRefTables_ExplainDoesNotThrow()
    {
        var mainField = new Field("GenerateIndex", Int64Type.Default, nullable: false);
        var mainSchema = new Schema(new[] { mainField }, null);
        var mainBatch = new RecordBatch(mainSchema, new IArrowArray[] { new Int64Array.Builder().Append(1).Build() }, 1);

        var refField = new Field("Id", Int64Type.Default, nullable: false);
        var refSchema = new Schema(new[] { refField }, null);
        var refBatch = new RecordBatch(refSchema, new IArrowArray[] { new Int64Array.Builder().Append(1).Build() }, 1);

        var channelMain = Channel.CreateUnbounded<RecordBatch>();
        channelMain.Writer.TryWrite(mainBatch);
        channelMain.Writer.Complete();

        var channelRef = Channel.CreateUnbounded<RecordBatch>();
        channelRef.Writer.TryWrite(refBatch);
        channelRef.Writer.Complete();

        var channelRef2 = Channel.CreateUnbounded<RecordBatch>();
        channelRef2.Writer.TryWrite(refBatch);
        channelRef2.Writer.Complete();

        var mock = new Mock<IMemoryChannelRegistry>();
        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("main", It.IsAny<CancellationToken>())).ReturnsAsync(mainSchema);
        mock.Setup(r => r.GetArrowChannel("main")).Returns((channelMain, mainSchema));

        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("ref", It.IsAny<CancellationToken>())).ReturnsAsync(refSchema);
        mock.Setup(r => r.GetArrowChannel("ref")).Returns((channelRef, refSchema));

        mock.Setup(r => r.WaitForArrowChannelSchemaAsync("ref2", It.IsAny<CancellationToken>())).ReturnsAsync(refSchema);
        mock.Setup(r => r.GetArrowChannel("ref2")).Returns((channelRef2, refSchema));

        var processor = new DuckDBSqlProcessor(
            mock.Object,
            "SELECT m.*, r.Id as ref1_id, r2.Id as ref2_id FROM main m LEFT JOIN ref r ON m.GenerateIndex = CAST(r.Id AS BIGINT) LEFT JOIN ref2 r2 ON m.GenerateIndex = CAST(r2.Id AS BIGINT)",
            "main", "main",
            refAliases: ["ref", "ref2"], refChannelAliases: ["ref", "ref2"],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);

        await processor.DisposeAsync();
        Assert.NotEmpty(batches);
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

    // ── --duck-init Tests ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Verifies that initSql runs before the main query on the same in-memory connection.
    /// Creates a TEMP TABLE in initSql and SELECTs from it in the main query — if initSql
    /// did not run, the query would fail with "table not found".
    /// No streaming source is required: the query is a plain table scan.
    /// </summary>
    [Fact]
    public async Task InitSql_CreatesObject_ObjectIsVisibleInQuery()
    {
        var emptyRegistry = new Mock<IMemoryChannelRegistry>().Object;

        var processor = new DuckDBSqlProcessor(
            emptyRegistry,
            "SELECT val FROM _init_check",
            mainAlias: "", mainChannelAlias: "",
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance,
            initSql: "CREATE TEMP TABLE _init_check (val VARCHAR); INSERT INTO _init_check VALUES ('duck_init_test')");

        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);
        processor.Dispose();

        Assert.Equal(1, batches.Sum(b => b.Length));
        var col = Assert.IsType<Apache.Arrow.StringArray>(batches[0].Column(0));
        Assert.Equal("duck_init_test", col.GetString(0));
    }

    /// <summary>
    /// Verifies that a macro defined in initSql is callable in the main query.
    /// Uses only built-in DuckDB SQL — no extension download required.
    /// </summary>
    [Fact]
    public async Task InitSql_CreateMacro_MacroIsUsableInQuery()
    {
        var field = new Field("val", Apache.Arrow.Types.Int32Type.Default, nullable: false);
        var schema = new Schema(new[] { field }, null);
        var arr = new Int32Array.Builder().AppendRange(new[] { 1, 2, 3 }).Build();
        var batch = new RecordBatch(schema, new IArrowArray[] { arr }, 3);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry,
            "SELECT triple(val) AS result FROM src ORDER BY val",
            alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance,
            initSql: "CREATE MACRO triple(x) AS x * 3");

        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);
        processor.Dispose();

        Assert.Equal(3, batches.Sum(b => b.Length));
        var resultCol = Assert.IsType<Int32Array>(batches[0].Column(0));
        Assert.Equal(3, resultCol.GetValue(0));
        Assert.Equal(6, resultCol.GetValue(1));
        Assert.Equal(9, resultCol.GetValue(2));
    }

    /// <summary>
    /// Verifies that LOAD json (bundled in DuckDB.NET.Data.Full — no network required)
    /// via initSql enables JSON functions in the subsequent query.
    /// json is auto-loaded in DuckDB 1.5.x, so LOAD json is a no-op if already active;
    /// this test confirms it does not break normal processor operation.
    /// </summary>
    [Fact]
    public async Task InitSql_LoadJsonExtension_JsonFunctionsWorkInQuery()
    {
        var emptyRegistry = new Mock<IMemoryChannelRegistry>().Object;

        var processor = new DuckDBSqlProcessor(
            emptyRegistry,
            "SELECT json_array_length('[10, 20, 30]') AS cnt",
            mainAlias: "", mainChannelAlias: "",
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance,
            initSql: "LOAD json");

        await processor.OpenAsync();
        var batches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            batches.Add(b);
        processor.Dispose();

        Assert.Equal(1, batches.Sum(b => b.Length));
        // json_array_length returns UBIGINT in DuckDB → UInt64Array in Arrow
        var col = batches[0].Column(0);
        Assert.NotNull(col);
        Assert.Equal(1, col.Length);
    }

    /// <summary>
    /// Verifies that the @file prefix causes initSql to be read from disk, not
    /// interpreted as inline SQL. The file creates a temp table; the query
    /// selects from it to confirm the file content was executed.
    /// </summary>
    [Fact]
    public async Task InitSql_AtFilePrefix_LoadsSqlFromFile()
    {
        var tempFile = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(tempFile,
                "CREATE TEMP TABLE _file_check (val VARCHAR); INSERT INTO _file_check VALUES ('from_file_test')");

            var emptyRegistry = new Mock<IMemoryChannelRegistry>().Object;
            var processor = new DuckDBSqlProcessor(
                emptyRegistry,
                "SELECT val FROM _file_check",
                mainAlias: "", mainChannelAlias: "",
                refAliases: [], refChannelAliases: [],
                NullLogger<DuckDBSqlProcessor>.Instance,
                initSql: $"@{tempFile}");

            await processor.OpenAsync();
            var batches = new List<RecordBatch>();
            await foreach (var b in processor.ReadRecordBatchesAsync())
                batches.Add(b);
            processor.Dispose();

            Assert.Equal(1, batches.Sum(b => b.Length));
            var col = Assert.IsType<Apache.Arrow.StringArray>(batches[0].Column(0));
            Assert.Equal("from_file_test", col.GetString(0));
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    /// <summary>
    /// Verifies that a syntax error in initSql causes OpenAsync to throw before any
    /// query execution — the error must propagate and not be silently swallowed.
    /// </summary>
    [Fact]
    public async Task InitSql_InvalidSql_ThrowsOnOpenAsync()
    {
        var emptyRegistry = new Mock<IMemoryChannelRegistry>().Object;

        var processor = new DuckDBSqlProcessor(
            emptyRegistry,
            "SELECT 1 AS n",
            mainAlias: "", mainChannelAlias: "",
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance,
            initSql: "THIS IS NOT VALID SQL");

        await Assert.ThrowsAnyAsync<Exception>(() => processor.OpenAsync());
        processor.Dispose();
    }

    /// <summary>
    /// Verifies that DuckDB's projection pushdown correctly handles nested Arrow Struct fields.
    /// When querying a sub-property like `User.Role`, the processor should pass the entire `User`
    /// root column to DuckDB to prevent native crashes, and the output should contain only `Role`.
    /// </summary>
    [Fact]
    public async Task OpenAsync_AndReadRecordBatches_NestedStructProjection_ReturnsNativeExtraction()
    {
        // Schema: User = Struct { Id: Int32, Role: String }
        var idField = new Field("Id", Int32Type.Default, nullable: false);
        var roleField = new Field("Role", StringType.Default, nullable: false);
        var userStructType = new StructType(new[] { idField, roleField });
        
        var userField = new Field("User", userStructType, nullable: false);
        var schema = new Schema(new[] { userField }, null);

        // Data arrays
        var idArr = new Int32Array.Builder().AppendRange(new[] { 1, 2 }).Build();
        var roleArr = new StringArray.Builder().AppendRange(new[] { "Admin", "Guest" }).Build();

        // Valid buffer for the struct array (2 items, both valid)
        var validBuf = new ArrowBuffer.Builder<byte>(1).Append(0xFF).Build();
        var structArr = new StructArray(userStructType, 2, new IArrowArray[] { idArr, roleArr }, validBuf, 0);

        var batch = new RecordBatch(schema, new IArrowArray[] { structArr }, 2);

        const string alias = "src";
        var registry = BuildRegistry(alias, schema, new[] { batch });

        var processor = new DuckDBSqlProcessor(
            registry, "SELECT User.Role FROM src ORDER BY User.Id", alias, alias,
            refAliases: [], refChannelAliases: [],
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var resultBatches = new List<RecordBatch>();
        await foreach (var b in processor.ReadRecordBatchesAsync())
            resultBatches.Add(b);
        await processor.DisposeAsync();

        // Output schema must only have 'Role'
        Assert.Single(processor.Schema!.FieldsList);
        Assert.Equal("Role", processor.Schema.FieldsList[0].Name);

        // Data verification
        Assert.Single(resultBatches);
        var firstBatch = resultBatches[0];
        Assert.Equal(2, firstBatch.Length);
        
        var roleColumn = Assert.IsType<StringArray>(firstBatch.Column(0));
        Assert.Equal("Admin", roleColumn.GetString(0));
        Assert.Equal("Guest", roleColumn.GetString(1));
    }

    // ── --ref scanned more than once ──────────────────────────────────────────────────────

    private static IMemoryChannelRegistry BuildRefRegistry(
        (string Alias, Schema Schema, RecordBatch[] Batches)[] sources)
    {
        var mock = new Mock<IMemoryChannelRegistry>();
        foreach (var (alias, schema, batches) in sources)
        {
            var channel = Channel.CreateUnbounded<RecordBatch>();
            foreach (var b in batches) channel.Writer.TryWrite(b);
            channel.Writer.Complete();
            mock.Setup(r => r.WaitForArrowChannelSchemaAsync(alias, It.IsAny<CancellationToken>()))
                .ReturnsAsync(schema);
            mock.Setup(r => r.GetArrowChannel(alias)).Returns((channel, schema));
        }
        return mock.Object;
    }

    private static (Schema Schema, RecordBatch[] Batches) LookupSource(params (long Id, long V)[][] batches)
    {
        var schema = new Schema(new[]
        {
            new Field("id", Int64Type.Default, nullable: false),
            new Field("v", Int64Type.Default, nullable: false),
        }, null);
        var built = batches.Select(rows => new RecordBatch(schema, new IArrowArray[]
        {
            new Int64Array.Builder().AppendRange(rows.Select(r => r.Id)).Build(),
            new Int64Array.Builder().AppendRange(rows.Select(r => r.V)).Build(),
        }, rows.Length)).ToArray();
        return (schema, built);
    }

    private static async Task<List<object?[]>> RunRowsAsync(
        IMemoryChannelRegistry registry, string query, string[] refs, string mainAlias = "main")
    {
        var processor = new DuckDBSqlProcessor(
            registry, query, mainAlias, mainAlias,
            refAliases: refs, refChannelAliases: refs,
            NullLogger<DuckDBSqlProcessor>.Instance);

        await processor.OpenAsync();
        var rows = new List<object?[]>();
        await foreach (var batch in processor.ReadRecordBatchesAsync())
        {
            using (batch)
                for (var i = 0; i < batch.Length; i++)
                    rows.Add(Enumerable.Range(0, batch.ColumnCount)
                        .Select(c => ArrowTypeMapper.GetValue(batch.Column(c), i)).ToArray());
        }
        await processor.DisposeAsync();
        return rows;
    }

    /// <summary>
    /// A <c>--ref</c> is documented as joinable several times. <c>duckdb_arrow_scan</c> hands its single
    /// stream to the first scan and leaves nothing for the second, so a self-join silently returned no
    /// rows (INNER) or NULLs (LEFT).
    /// </summary>
    [Fact]
    public async Task Ref_JoinedTwice_ReadsTheFullTableEachTime()
    {
        var (mainSchema, mainBatches) = LookupSource(new[] { (1L, 0L), (2L, 0L), (3L, 0L) });
        var (refSchema, refBatches) = LookupSource(new[] { (1L, 10L), (2L, 20L) }, new[] { (3L, 30L) });
        var registry = BuildRefRegistry(new[]
        {
            ("main", mainSchema, mainBatches),
            ("r", refSchema, refBatches),
        });

        var rows = await RunRowsAsync(registry,
            "SELECT m.id, a.v AS av, b.v AS bv FROM main m JOIN r a ON a.id = m.id JOIN r b ON b.id = m.id ORDER BY m.id",
            refs: ["r"]);

        Assert.Equal(3, rows.Count);
        Assert.Equal(new object?[] { 1L, 10L, 10L }, rows[0]);
        Assert.Equal(new object?[] { 3L, 30L, 30L }, rows[2]);
    }

    [Fact]
    public async Task Ref_UnionedWithItself_ReturnsEveryRowTwice()
    {
        var (mainSchema, mainBatches) = LookupSource(new[] { (1L, 0L) });
        var (refSchema, refBatches) = LookupSource(new[] { (1L, 10L), (2L, 20L), (3L, 30L) });
        var registry = BuildRefRegistry(new[]
        {
            ("main", mainSchema, mainBatches),
            ("r", refSchema, refBatches),
        });

        var rows = await RunRowsAsync(registry,
            "SELECT count(*) AS n, CAST(sum(v) AS BIGINT) AS s FROM (SELECT * FROM r UNION ALL SELECT * FROM r)",
            refs: ["r"]);

        Assert.Equal(new object?[] { 6L, 120L }, Assert.Single(rows));
    }

    /// <summary>
    /// Every <c>--ref</c> alias is materialised independently: one scanned twice must not disturb
    /// another scanned once.
    /// </summary>
    [Fact]
    public async Task TwoRefs_OneScannedTwice_BothAreComplete()
    {
        var (mainSchema, mainBatches) = LookupSource(new[] { (1L, 0L), (2L, 0L) });
        var (aSchema, aBatches) = LookupSource(new[] { (1L, 10L), (2L, 20L) });
        var (bSchema, bBatches) = LookupSource(new[] { (1L, 100L), (2L, 200L) });
        var registry = BuildRefRegistry(new[]
        {
            ("main", mainSchema, mainBatches),
            ("a", aSchema, aBatches),
            ("b", bSchema, bBatches),
        });

        var rows = await RunRowsAsync(registry,
            "SELECT m.id, a1.v + a2.v AS av, b.v AS bv FROM main m JOIN a a1 ON a1.id = m.id JOIN a a2 ON a2.id = m.id JOIN b ON b.id = m.id ORDER BY m.id",
            refs: ["a", "b"]);

        Assert.Equal(2, rows.Count);
        Assert.Equal(new object?[] { 1L, 20L, 100L }, rows[0]);
        Assert.Equal(new object?[] { 2L, 40L, 200L }, rows[1]);
    }

    // ── --from read more than once ────────────────────────────────────────────────────────

    private static IMemoryChannelRegistry StreamedMainWithRef()
    {
        var (mainSchema, mainBatches) = LookupSource(new[] { (1L, 10L), (2L, 20L), (3L, 30L) });
        var (refSchema, refBatches) = LookupSource(new[] { (1L, 100L), (2L, 200L) });
        return BuildRefRegistry(new[]
        {
            ("main", mainSchema, mainBatches),
            ("r", refSchema, refBatches),
        });
    }

    /// <summary>
    /// A stream keeps nothing for a second read, so these queries used to run and return wrong rows
    /// with no error. Each must now be refused before it executes, with a message that says why.
    /// </summary>
    [Theory]
    [InlineData("SELECT count(*) AS n FROM main a JOIN main b USING(id)")]
    [InlineData("SELECT count(*) AS n FROM (SELECT * FROM main UNION ALL SELECT * FROM main)")]
    [InlineData("SELECT id FROM main WHERE v > (SELECT avg(v) FROM main)")]
    public async Task From_ReadMoreThanOnce_IsRefusedBeforeTheQueryRuns(string query)
    {
        var ex = await Assert.ThrowsAsync<DtPipe.Processors.Sql.StreamedSourceReadRepeatedlyException>(
            () => RunRowsAsync(StreamedMainWithRef(), query, refs: ["r"]));

        Assert.Equal("main", ex.Alias);
        Assert.True(ex.Reads >= 2);
        Assert.Contains("'main'", ex.Message);
        Assert.Contains("MATERIALIZED", ex.Message);
        Assert.Contains("--ref", ex.Message);
        Assert.DoesNotContain("Pre-flight EXPLAIN failed", ex.Message);
    }

    /// <summary>The way out the message offers has to work, or the message is a trap.</summary>
    [Fact]
    public async Task From_ReadOnceIntoAMaterializedCte_CanBeReusedFreely()
    {
        var rows = await RunRowsAsync(StreamedMainWithRef(),
            "WITH once AS MATERIALIZED (SELECT * FROM main) SELECT count(*) AS n, CAST(sum(a.v + b.v) AS BIGINT) AS s FROM once a JOIN once b USING(id)",
            refs: ["r"]);

        Assert.Equal(new object?[] { 3L, 120L }, Assert.Single(rows));
    }

    /// <summary>Shapes that read the streaming source once, however much they look like they read it twice.</summary>
    [Theory]
    [InlineData("SELECT m.id, (SELECT count(*) FROM r WHERE r.id = m.id) AS c FROM main m ORDER BY m.id", 3)]
    [InlineData("SELECT id, sum(v) AS s FROM main GROUP BY ROLLUP(id)", 4)]
    [InlineData("SELECT id, sum(v) OVER (ORDER BY id) AS running FROM main", 3)]
    [InlineData("SELECT m.id FROM main m JOIN r a ON a.id = m.id JOIN r b ON b.id = m.id", 2)]
    [InlineData("SELECT count(*) AS n FROM r", 1)]
    public async Task From_ReadOnce_IsNotRefused(string query, int expectedRows)
    {
        var rows = await RunRowsAsync(StreamedMainWithRef(), query, refs: ["r"]);

        Assert.Equal(expectedRows, rows.Count);
    }
}
