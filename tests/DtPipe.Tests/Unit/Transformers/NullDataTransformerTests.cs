using Apache.Arrow.Types;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Null;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests;

public class NullDataTransformerTests
{
	[Fact]
	public async Task Transform_ShouldSetColumnToNull_WhenNullColumnsSpecified()
	{
		var options = new NullOptions { Columns = new[] { "SENSITIVE" } };
		var transformer = new NullDataTransformer(options);
		var columns = new List<PipeColumnInfo>
		{
			new("ID", typeof(int), false),
			new("SENSITIVE", typeof(string), true),
			new("NAME", typeof(string), true)
		};

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { 1, "SecretData", "John" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().Be(1);          // ID unchanged
		TestBatchBuilder.GetVal(result!, 1, 0).Should().BeNull();       // SENSITIVE nulled
		TestBatchBuilder.GetVal(result!, 2, 0).Should().Be("John");     // NAME unchanged
	}

	[Fact]
	public async Task Transform_ShouldDoNothing_WhenNoNullColumnsMatch()
	{
		var options = new NullOptions { Columns = new[] { "NONEXISTENT" } };
		var transformer = new NullDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("ID", typeof(int), false) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { 1 });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().Be(1);
	}

	[Fact]
	public async Task Transform_ShouldPreserveArrowType_ForDecimalColumn()
	{
		// Before the fix, CreateNullArray fell back to StringArray.Builder for Decimal128 and other
		// unrecognised types, creating a mismatch between the declared schema type and the actual
		// array data. After the fix (using ArrowTypeMapper.CreateBuilder), the null array matches
		// the field's Arrow type.
		var options = new NullOptions { Columns = new[] { "Price" } };
		var transformer = new NullDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("Price", typeof(decimal), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { 9.99m });
		var result = await transformer.TransformBatchAsync(batch);

		result!.Schema.GetFieldByIndex(0).DataType.Should().BeOfType<Decimal128Type>(
			"null array for a decimal column must still be Decimal128, not a StringArray fallback");
		result.Column(0).IsNull(0).Should().BeTrue();
	}

	[Fact]
	public async Task Transform_ShouldPreserveArrowType_ForTimestampColumn()
	{
		var options = new NullOptions { Columns = new[] { "CreatedAt" } };
		var transformer = new NullDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("CreatedAt", typeof(DateTimeOffset), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { DateTimeOffset.UtcNow });
		var result = await transformer.TransformBatchAsync(batch);

		result!.Schema.GetFieldByIndex(0).DataType.Should().BeOfType<TimestampType>(
			"null array for a DateTimeOffset column must be TimestampType, not a StringArray fallback");
		result.Column(0).IsNull(0).Should().BeTrue();
	}

	[Fact]
	public async Task Transform_ShouldPreserveArrowType_ForBooleanColumn()
	{
		var options = new NullOptions { Columns = new[] { "Active" } };
		var transformer = new NullDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("Active", typeof(bool), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { true });
		var result = await transformer.TransformBatchAsync(batch);

		result!.Schema.GetFieldByIndex(0).DataType.Should().BeOfType<BooleanType>();
		result.Column(0).IsNull(0).Should().BeTrue();
	}
}
