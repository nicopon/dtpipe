using DtPipe.Adapters.Arrow;
using DtPipe.Core.Models;
using DtPipe.Core.Infrastructure.Arrow;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Arrow;

public class ArrowAdapterTests : IAsyncLifetime
{
	private string _testArrowPath = null!;

	public ValueTask InitializeAsync()
	{
		_testArrowPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.arrow");
		return ValueTask.CompletedTask;
	}

	public ValueTask DisposeAsync()
	{
		if (File.Exists(_testArrowPath)) File.Delete(_testArrowPath);
		return ValueTask.CompletedTask;
	}

	[Fact]
	public void ArrowTypeMapper_UuidRoundTrip_ShouldPreserveGuidValue()
	{
		// RFC 4122 bytes → .NET Guid → RFC 4122 bytes must round-trip exactly
		var original = Guid.Parse("550e8400-e29b-41d4-a716-446655440000");

		var arrowBytes = ArrowTypeMapper.ToArrowUuidBytes(original);
		var recovered = ArrowTypeMapper.FromArrowUuidBytes(arrowBytes);

		recovered.Should().Be(original);

		// Bytes should be RFC 4122 big-endian: first 4 bytes big-endian A component
		// 0x550e8400 → bytes: 55 0e 84 00
		arrowBytes[0].Should().Be(0x55);
		arrowBytes[1].Should().Be(0x0e);
		arrowBytes[2].Should().Be(0x84);
		arrowBytes[3].Should().Be(0x00);
	}

	[Fact]
	public void ArrowTypeMapper_GetArrowType_GuidReturnsBinaryType()
	{
		var arrowType = ArrowTypeMapper.GetArrowType(typeof(Guid));
		arrowType.Should().BeOfType<Apache.Arrow.Types.BinaryType>();
	}

	[Fact]
	public void ArrowTypeMapper_AppendValue_GuidProducesRfc4122Bytes()
	{
		var guid = Guid.Parse("550e8400-e29b-41d4-a716-446655440000");
		var builder = new Apache.Arrow.BinaryArray.Builder();
		ArrowTypeMapper.AppendValue(builder, guid);
		var array = (Apache.Arrow.BinaryArray)ArrowTypeMapper.BuildArray(builder);

		var bytes = array.GetBytes(0).ToArray();
		// First byte of RFC 4122 for "550e8400-..." must be 0x55
		bytes.Should().HaveCount(16);
		bytes[0].Should().Be(0x55);
		ArrowTypeMapper.FromArrowUuidBytes(bytes).Should().Be(guid);
	}

	[Fact]
	public async Task ArrowAdapter_ShouldWriteAndReadData()
	{
		// Arrange
		var columns = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Name", typeof(string), true),
			new("Value", typeof(double), false)
		};

		var rows = new List<object?[]>
		{
			new object?[] { 1, "Alice", 1.5 },
			new object?[] { 2, "Bob", 2.5 },
			new object?[] { 3, null, 3.5 }
		};

		// Act 1: Write
		var writer = new ArrowAdapterDataWriter(_testArrowPath, new ArrowWriterOptions { BatchSize = 10 });
		await writer.InitializeAsync(columns);

		// ArrowAdapterDataWriter is now purely columnar, we must use the bridge or write RecordBatches directly.
		var bridge = new ArrowRowToColumnarBridge();
		await bridge.InitializeAsync(columns, 10);

		var writeTask = Task.Run(async () =>
		{
			await foreach (var batch in bridge.ReadRecordBatchesAsync())
			{
				await writer.WriteRecordBatchAsync(batch);
			}
		});

		await bridge.IngestRowsAsync(rows.ToArray());
		await bridge.CompleteAsync();
		await writeTask;

		await writer.CompleteAsync();
		await writer.DisposeAsync();

		// Act 2: Read
		var reader = new ArrowAdapterStreamReader(_testArrowPath, new ArrowReaderOptions());
		await reader.OpenAsync();
		var readColumns = reader.Columns;
		var readRows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			readRows.AddRange(batch.ToArray());
		}
		await reader.DisposeAsync();

		// Assert
		readColumns.Should().HaveCount(3);
		readRows.Should().HaveCount(3);
		readRows[0][0].Should().Be(1);
		readRows[0][1].Should().Be("Alice");
		readRows[0][2].Should().Be(1.5);
		readRows[2][1].Should().BeNull();
	}
}
