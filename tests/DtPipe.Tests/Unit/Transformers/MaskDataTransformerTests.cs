using Apache.Arrow.Types;
using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Mask;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests;

public class MaskDataTransformerTests
{
	[Fact]
	public async Task Transform_ShouldMaskData_UsingPattern()
	{
		// Pattern "###****": keep 3 chars, replace 4 with '*', rest kept
		// Input "test@example.com" (16 chars): "tes" + "****" + "ample.com"
		var options = new MaskOptions { Mask = new[] { "EMAIL:###****" } };
		var transformer = new MaskDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("EMAIL", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "test@example.com" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().Be("tes****ample.com");
	}

	[Fact]
	public async Task Transform_ShouldMaskData_FullReplacement()
	{
		var options = new MaskOptions { Mask = new[] { "PIN:****" } };
		var transformer = new MaskDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("PIN", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "1234" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().Be("****");
	}

	[Fact]
	public async Task Transform_ShouldMaskData_WithComplexPattern()
	{
		// Input: 0612345678 (10 chars), Pattern: ##******## -> Keep first 2, mask 6, keep last 2
		var options = new MaskOptions { Mask = new[] { "PHONE:##******##" } };
		var transformer = new MaskDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("PHONE", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { "0612345678" });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().Be("06******78");
	}

	[Fact]
	public async Task Transform_ShouldHandleNullValues_ByDefault()
	{
		var options = new MaskOptions { Mask = new[] { "COL:***" } };
		var transformer = new MaskDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("COL", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { null });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().BeNull("Nulls pass through the mask unchanged");
	}

	[Fact]
	public async Task Transform_ShouldSkipFormatting_WhenSkipNullEnabled_AndValueNull()
	{
		var options = new MaskOptions { Mask = new[] { "COL:***" }, SkipNull = true };
		var transformer = new MaskDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("COL", typeof(string), true) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { null });
		var result = await transformer.TransformBatchAsync(batch);

		TestBatchBuilder.GetVal(result!, 0, 0).Should().BeNull();
	}

	[Fact]
	public async Task InitializeAsync_ShouldDeclareStringType_ForNonStringColumn()
	{
		// MaskArray always produces a StringArray regardless of source type.
		// InitializeAsync must update ClrType to typeof(string) so downstream PipeColumnInfo
		// consumers (bridges, writers) don't expect the original numeric type.
		var options = new MaskOptions { Mask = new[] { "Amount:###*" } };
		var transformer = new MaskDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("Amount", typeof(long), false) };

		var outputSchema = await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);

		outputSchema[0].ClrType.Should().Be(typeof(string),
			"MaskArray always produces StringArray — ClrType must match to avoid bridge type mismatches");
	}

	[Fact]
	public async Task TransformBatch_ShouldProduceStringTypeInSchema_ForNonStringColumn()
	{
		// The output RecordBatch schema must carry StringType for the masked column so that
		// ArrowColumnarToRowBridge reads values correctly and downstream bridges don't create
		// wrong-type builders.
		var options = new MaskOptions { Mask = new[] { "Amount:###*" } };
		var transformer = new MaskDataTransformer(options);
		var columns = new List<PipeColumnInfo> { new("Amount", typeof(long), false) };

		await transformer.InitializeAsync(columns, TestContext.Current.CancellationToken);
		var batch = TestBatchBuilder.FromRows(columns, new object?[] { 1234L });
		var result = await transformer.TransformBatchAsync(batch);

		result!.Schema.GetFieldByIndex(0).DataType.Should().BeOfType<StringType>(
			"output batch schema must reflect StringType after masking a non-string column");
		TestBatchBuilder.GetVal(result, 0, 0).Should().Be("123*");
	}
}
