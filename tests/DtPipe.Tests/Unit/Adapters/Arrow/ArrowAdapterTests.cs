using DtPipe.Adapters.Arrow;
using DtPipe.Core.Models;
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
		await writer.WriteBatchAsync(rows);
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
