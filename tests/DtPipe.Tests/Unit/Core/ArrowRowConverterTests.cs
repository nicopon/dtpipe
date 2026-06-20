using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests;

public class ArrowRowConverterTests
{
	[Fact]
	public void FlattenBatch_ShouldChunkRowsPerfect_WhenBatchSizeDividesLength()
	{
		// Arrange
		var schema = new Schema.Builder()
			.Field(f => f.Name("Id").DataType(Int64Type.Default).Nullable(false))
			.Build();

		var idBuilder = new Int64Array.Builder().Append(1).Append(2).Append(3).Append(4);
		var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build() }, 4);

		// Act
		var chunks = ArrowRowConverter.FlattenBatch(batch, 2).ToList();

		// Assert
		chunks.Should().HaveCount(2);
		chunks[0].Length.Should().Be(2);
		chunks[0].Span[0][0].Should().Be(1L);
		chunks[0].Span[1][0].Should().Be(2L);
		chunks[1].Length.Should().Be(2);
		chunks[1].Span[0][0].Should().Be(3L);
		chunks[1].Span[1][0].Should().Be(4L);
	}

	[Fact]
	public void FlattenBatch_ShouldChunkRowsImperfect_WhenBatchSizeDoesNotDivideLength()
	{
		// Arrange
		var schema = new Schema.Builder()
			.Field(f => f.Name("Id").DataType(Int64Type.Default).Nullable(false))
			.Build();

		var idBuilder = new Int64Array.Builder().Append(1).Append(2).Append(3).Append(4).Append(5);
		var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build() }, 5);

		// Act
		var chunks = ArrowRowConverter.FlattenBatch(batch, 2).ToList();

		// Assert
		chunks.Should().HaveCount(3);
		chunks[0].Length.Should().Be(2);
		chunks[1].Length.Should().Be(2);
		chunks[2].Length.Should().Be(1);
		chunks[2].Span[0][0].Should().Be(5L);
	}

	[Fact]
	public void FlattenBatch_ShouldReturnEmpty_WhenBatchIsEmpty()
	{
		// Arrange
		var schema = new Schema.Builder()
			.Field(f => f.Name("Id").DataType(Int64Type.Default).Nullable(false))
			.Build();

		var batch = new RecordBatch(schema, new IArrowArray[] { new Int64Array.Builder().Build() }, 0);

		// Act
		var chunks = ArrowRowConverter.FlattenBatch(batch, 2).ToList();

		// Assert
		chunks.Should().BeEmpty();
	}

	[Fact]
	public void FlattenBatch_ShouldReturnOneChunk_WhenBatchSizeIsLargerThanLength()
	{
		// Arrange
		var schema = new Schema.Builder()
			.Field(f => f.Name("Id").DataType(Int64Type.Default).Nullable(false))
			.Build();

		var idBuilder = new Int64Array.Builder().Append(1).Append(2).Append(3);
		var batch = new RecordBatch(schema, new IArrowArray[] { idBuilder.Build() }, 3);

		// Act
		var chunks = ArrowRowConverter.FlattenBatch(batch, 10).ToList();

		// Assert
		chunks.Should().HaveCount(1);
		chunks[0].Length.Should().Be(3);
	}
}
