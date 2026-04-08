using Apache.Arrow;
using Apache.Arrow.Types;
using DtPipe.Core.Models;
using DtPipe.Transformers.Arrow.Project;
using Xunit;

namespace DtPipe.Tests.Unit.Transformers;

public class ProjectDataTransformerTests
{
	private readonly IReadOnlyList<PipeColumnInfo> _sourceSchema = new List<PipeColumnInfo>
	{
		new("A", typeof(int), false),
		new("B", typeof(string), true),
		new("C", typeof(double), false),
		new("D", typeof(DateTime), false)
	};

	private static RecordBatch BuildBatch()
	{
		var schema = new Schema(new[]
		{
			new Field("A", Int32Type.Default, false),
			new Field("B", StringType.Default, true),
			new Field("C", DoubleType.Default, false),
			new Field("D", Int64Type.Default, false),  // DateTime stored as ticks
		}, null);

		var a = new Int32Array.Builder().Append(1).Append(2).Build();
		var b = new StringArray.Builder().Append("text").AppendNull().Build();
		var c = new DoubleArray.Builder().Append(3.14).Append(2.71).Build();
		var d = new Int64Array.Builder().Append(DateTime.Now.Ticks).Append(DateTime.Now.Ticks).Build();

		return new RecordBatch(schema, new IArrowArray[] { a, b, c, d }, 2);
	}

	[Fact]
	public async Task InitializeAsync_NoOptions_PassesThrough()
	{
		var transformer = new ProjectDataTransformer(new ProjectOptions());
		var resultSchema = await transformer.InitializeAsync(_sourceSchema);

		Assert.Equal(_sourceSchema.Count, resultSchema.Count);
		for (int i = 0; i < _sourceSchema.Count; i++)
			Assert.Equal(_sourceSchema[i].Name, resultSchema[i].Name);

		// Columnar path: pass-through should return the same batch unchanged
		var batch = BuildBatch();
		var result = await transformer.TransformBatchAsync(batch);
		Assert.NotNull(result);
		Assert.Equal(4, result!.Schema.FieldsList.Count);
		Assert.Equal(2, result.Length);
	}

	[Fact]
	public async Task InitializeAsync_Project_SelectsColumnsAndReorders()
	{
		var options = new ProjectOptions { Project = new[] { "C", "A" } };
		var transformer = new ProjectDataTransformer(options);
		var resultSchema = await transformer.InitializeAsync(_sourceSchema);

		Assert.Equal(2, resultSchema.Count);
		Assert.Equal("C", resultSchema[0].Name);
		Assert.Equal("A", resultSchema[1].Name);

		var batch = BuildBatch();
		var result = await transformer.TransformBatchAsync(batch);
		Assert.NotNull(result);
		Assert.Equal(2, result!.Schema.FieldsList.Count);
		Assert.Equal("C", result.Schema.GetFieldByIndex(0).Name);
		Assert.Equal("A", result.Schema.GetFieldByIndex(1).Name);
		Assert.Equal(2, result.Length);
		// First row: C=3.14, A=1
		Assert.Equal(3.14, ((DoubleArray)result.Column(0)).GetValue(0));
		Assert.Equal(1, ((Int32Array)result.Column(1)).GetValue(0));
	}

	[Fact]
	public async Task InitializeAsync_Drop_RemovesColumns()
	{
		var options = new ProjectOptions { Drop = new[] { "B", "D" } };
		var transformer = new ProjectDataTransformer(options);
		var resultSchema = await transformer.InitializeAsync(_sourceSchema);

		Assert.Equal(2, resultSchema.Count);
		Assert.Equal("A", resultSchema[0].Name);
		Assert.Equal("C", resultSchema[1].Name);

		var batch = BuildBatch();
		var result = await transformer.TransformBatchAsync(batch);
		Assert.NotNull(result);
		Assert.Equal(2, result!.Schema.FieldsList.Count);
		Assert.Equal("A", result.Schema.GetFieldByIndex(0).Name);
		Assert.Equal("C", result.Schema.GetFieldByIndex(1).Name);
		Assert.Equal(1, ((Int32Array)result.Column(0)).GetValue(0));
		Assert.Equal(3.14, ((DoubleArray)result.Column(1)).GetValue(0));
	}

	[Fact]
	public async Task InitializeAsync_DropAndProject_DropTakesPrecedence()
	{
		var options = new ProjectOptions { Drop = new[] { "A" }, Project = new[] { "A", "C" } };
		var transformer = new ProjectDataTransformer(options);
		var resultSchema = await transformer.InitializeAsync(_sourceSchema);

		Assert.NotNull(resultSchema);
		Assert.Single(resultSchema!);
		Assert.Equal("C", resultSchema[0].Name);

		var batch = BuildBatch();
		var result = await transformer.TransformBatchAsync(batch);
		Assert.NotNull(result);
		Assert.Single(result!.Schema.FieldsList);
		Assert.Equal(3.14, ((DoubleArray)result.Column(0)).GetValue(0));
	}
}
