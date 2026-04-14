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
