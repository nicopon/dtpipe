using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Core;

public class ArrowRowToColumnarBridgeTests
{
	/// <summary>
	/// Regression test for the root cause of the interleaved overwrite+format FormatException.
	///
	/// Scenario: a transformer (e.g. Overwrite) mutated column "A" from Int64 to string, but
	/// ExportService propagated the reader's original Arrow schema (Int64) as overrideSchema for
	/// every pipeline segment. When the rows→Arrow bridge used that stale schema to create builders,
	/// it produced an Int32Array.Builder for "A". Appending "Val1" → FormatException.
	///
	/// After the fix, PipeColumnInfo is the type authority. overrideSchema is only accepted for
	/// metadata enrichment when the base Arrow type is identical.
	/// </summary>
	[Fact]
	public async Task InitializeAsync_ShouldIgnoreStaleOverrideSchema_WhenTypesConflict()
	{
		var bridge = new ArrowRowToColumnarBridge();
		var columns = new List<PipeColumnInfo> { new("A", typeof(string), true) };
		var staleSchema = new Schema(new[] { new Field("A", Int64Type.Default, true) }, null);

		await bridge.InitializeAsync(columns, batchSize: 10, overrideSchema: staleSchema);

		var ingestTask = Task.Run(async () =>
		{
			await bridge.IngestRowsAsync(new ReadOnlyMemory<object?[]>(new[] { new object?[] { "Val1" } }));
			await bridge.CompleteAsync();
		});

		var batches = await bridge.ReadRecordBatchesAsync().ToListAsync();
		await ingestTask;

		batches.Should().HaveCount(1);
		batches[0].Schema.GetFieldByIndex(0).DataType.Should().BeOfType<StringType>(
			"PipeColumnInfo says string — stale Int64Type from overrideSchema must not win");
		ArrowTypeMapper.GetValueForField(batches[0].Column(0), batches[0].Schema.GetFieldByIndex(0), 0)
			.Should().Be("Val1");
	}

	[Fact]
	public async Task InitializeAsync_ShouldUseRichMetadata_WhenOverrideSchemaTypeIsCompatible()
	{
		// When PipeColumnInfo and overrideSchema agree on the base type, the richer overrideSchema
		// field (e.g. with timezone) is used so that complex type metadata is preserved.
		var bridge = new ArrowRowToColumnarBridge();
		var columns = new List<PipeColumnInfo> { new("CreatedAt", typeof(DateTimeOffset), true) };
		var richField = new Field("CreatedAt", new TimestampType(TimeUnit.Microsecond, "Europe/Paris"), true);
		var richSchema = new Schema(new[] { richField }, null);

		await bridge.InitializeAsync(columns, batchSize: 10, overrideSchema: richSchema);

		// The bridge's stored schema should carry the timezone from richSchema.
		bridge.Schema.Should().NotBeNull();
		var outputField = bridge.Schema!.GetFieldByIndex(0);
		outputField.DataType.Should().BeOfType<TimestampType>();
		((TimestampType)outputField.DataType).Timezone.Should().Be("Europe/Paris");
	}

	/// <summary>
	/// Every flush discards its builders and resolves a fresh appender per column against the
	/// replacements. An appender left bound to a flushed builder appends into an array nobody
	/// reads: the batch count still comes out right and the rows are simply gone. Nothing else
	/// in this suite crosses a batch boundary, so nothing else would catch it.
	/// </summary>
	[Fact]
	public async Task IngestRowsAsync_ShouldKeepEveryValue_WhenIngestSpansSeveralBatches()
	{
		var bridge = new ArrowRowToColumnarBridge();
		var columns = new List<PipeColumnInfo> { new("Id", typeof(int), false) };

		await bridge.InitializeAsync(columns, batchSize: 10, overrideSchema: null);

		var ingestTask = Task.Run(async () =>
		{
			for (int i = 0; i < 25; i++)
				await bridge.IngestRowsAsync(new ReadOnlyMemory<object?[]>(new[] { new object?[] { i } }));
			await bridge.CompleteAsync();
		});

		var batches = await bridge.ReadRecordBatchesAsync().ToListAsync();
		await ingestTask;

		batches.Should().HaveCount(3, "25 rows at a batch size of 10 flush twice, then once on completion");

		var values = batches.SelectMany(b => Enumerable
			.Range(0, b.Length)
			.Select(i => ArrowTypeMapper.GetValueForField(b.Column(0), b.Schema.GetFieldByIndex(0), i)));

		values.Should().Equal(Enumerable.Range(0, 25).Select(i => (object?)i));
	}

	[Fact]
	public async Task InitializeAsync_ShouldFallbackToPipeColumnInfo_WhenNoOverrideSchema()
	{
		var bridge = new ArrowRowToColumnarBridge();
		var columns = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Name", typeof(string), true)
		};

		await bridge.InitializeAsync(columns, batchSize: 10, overrideSchema: null);

		var ingestTask = Task.Run(async () =>
		{
			await bridge.IngestRowsAsync(new ReadOnlyMemory<object?[]>(new[] { new object?[] { 42, "Alice" } }));
			await bridge.CompleteAsync();
		});

		var batches = await bridge.ReadRecordBatchesAsync().ToListAsync();
		await ingestTask;

		batches.Should().HaveCount(1);
		batches[0].Schema.GetFieldByIndex(0).DataType.Should().BeOfType<Int32Type>();
		batches[0].Schema.GetFieldByIndex(1).DataType.Should().BeOfType<StringType>();
		ArrowTypeMapper.GetValueForField(batches[0].Column(0), batches[0].Schema.GetFieldByIndex(0), 0).Should().Be(42);
		ArrowTypeMapper.GetValueForField(batches[0].Column(1), batches[0].Schema.GetFieldByIndex(1), 0).Should().Be("Alice");
	}
}
