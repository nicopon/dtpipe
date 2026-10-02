using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Filter;
using DtPipe.Transformers.Row.Compute;
using DtPipe.Transformers.Row.Expand;
using DtPipe.Transformers.Row.Window;
using DtPipe.Transformers.Services;
using AwesomeAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

/// <summary>
/// Every transformer that evaluates JavaScript per row goes through one Jint engine for the whole
/// run, so a byte retained per row grows with the row count. These tests hold the retention per row
/// under a ceiling far below a leak (kilobytes per row) and above measurement noise.
/// </summary>
[Collection(MemoryMeasurementCollection.Name)]
public class JsTransformerRetentionTests : IDisposable
{
	private const int Rows = 200_000;
	private const int WarmUpRows = 1_000;
	private const double MaxRetainedBytesPerRow = 150;

	private static readonly IReadOnlyList<PipeColumnInfo> Columns = [new("Id", typeof(long), false)];

	private readonly JsEngineProvider _jsEngineProvider = new();

	public void Dispose() => _jsEngineProvider.Dispose();

	[Fact]
	public async Task Compute_DoesNotRetainMemoryPerRow()
	{
		var transformer = new ComputeDataTransformer(
			new ComputeOptions { Compute = ["c:'x'"] }, _jsEngineProvider);
		await transformer.InitializeAsync(Columns);

		RetainedBytesPerRow(row => transformer.Transform(row)).Should().BeLessThan(MaxRetainedBytesPerRow);
	}

	[Fact]
	public async Task Expand_DoesNotRetainMemoryPerRow()
	{
		var transformer = new ExpandDataTransformer(
			new ExpandOptions { Expand = ["return [row];"] }, _jsEngineProvider);
		await transformer.InitializeAsync(Columns);

		RetainedBytesPerRow(row => transformer.TransformMany(row).Count()).Should().BeLessThan(MaxRetainedBytesPerRow);
	}

	[Fact]
	public async Task Window_DoesNotRetainMemoryPerRow()
	{
		var transformer = new WindowDataTransformer(
			new WindowOptions { Count = 5, Script = "rows" }, _jsEngineProvider);
		await transformer.InitializeAsync(Columns);

		RetainedBytesPerRow(row => transformer.TransformMany(row).Count()).Should().BeLessThan(MaxRetainedBytesPerRow);
	}

	[Fact]
	public async Task Filter_OutsideTheSimplePath_DoesNotRetainMemoryPerRow()
	{
		// A plain comparison is evaluated without JavaScript; the disjunction forces Jint.
		var transformer = new FilterDataTransformer(
			new FilterOptions { Filters = ["row.Id % 2 === 0 || row.Id >= 0"] }, _jsEngineProvider);
		await transformer.InitializeAsync(Columns);

		RetainedBytesPerRow(row => transformer.Transform(row)).Should().BeLessThan(MaxRetainedBytesPerRow);
	}

	private static double RetainedBytesPerRow(Action<object?[]> processRow)
	{
		for (var i = 0; i < WarmUpRows; i++)
			processRow([(long)i]);

		var before = GC.GetTotalMemory(forceFullCollection: true);
		for (var i = 0; i < Rows; i++)
			processRow([(long)i]);
		var after = GC.GetTotalMemory(forceFullCollection: true);

		return (after - before) / (double)Rows;
	}
}
