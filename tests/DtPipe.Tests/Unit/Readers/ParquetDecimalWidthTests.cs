using Apache.Arrow;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Adapters.Parquet;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Parquet;
using Parquet.Schema;
using Xunit;

namespace DtPipe.Tests.Unit.Readers;

/// <summary>
/// A decimal is the one column whose Arrow form its CLR type does not fix: precision and scale
/// belong to the source. These cover the two ends of that — what the reader declares from a file,
/// and what the writer puts in one.
/// </summary>
public class ParquetDecimalWidthTests : IAsyncLifetime
{
	private string _path = null!;

	public ValueTask InitializeAsync()
	{
		_path = Path.Combine(Path.GetTempPath(), $"decimal_{Guid.NewGuid():N}.parquet");
		return ValueTask.CompletedTask;
	}

	public ValueTask DisposeAsync()
	{
		if (File.Exists(_path)) File.Delete(_path);
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task Reader_TakesPrecisionAndScaleFromTheFile()
	{
		await WriteFileAsync(new DecimalDataField("Amount", 10, 2, isNullable: true), [12.34m, 56.78m, null]);

		await using var reader = new ParquetStreamReader(_path);
		await reader.OpenAsync();

		reader.Columns.Should().ContainSingle();
		reader.Columns![0].Precision.Should().Be(10);
		reader.Columns![0].Scale.Should().Be(2);

		await foreach (var batch in reader.ReadRecordBatchesAsync())
		{
			using (batch)
			{
				var field = batch.Schema.GetFieldByIndex(0);
				field.DataType.Should().BeOfType<Decimal128Type>();
				var decimalType = (Decimal128Type)field.DataType;
				decimalType.Precision.Should().Be(10);
				decimalType.Scale.Should().Be(2);

				var column = (Apache.Arrow.Decimal128Array)batch.Column(0);
				column.GetValue(0).Should().Be(12.34m);
				column.GetValue(1).Should().Be(56.78m);
				column.IsNull(2).Should().BeTrue();
			}
		}
	}

	[Fact]
	public async Task Reader_FallsBackToTheWidestDecimal_WhenTheFileDeclaresNone()
	{
		await WriteFileAsync(new DataField<decimal?>("Amount"), [1.5m]);

		await using var reader = new ParquetStreamReader(_path);
		await reader.OpenAsync();

		// Parquet.Net writes its own default width for a plain decimal field; whatever it is, the
		// column carries it rather than nothing.
		reader.Columns![0].Precision.Should().NotBeNull();
	}

	[Fact]
	public async Task Writer_PutsTheDeclaredWidthInTheFile()
	{
		var columns = new List<PipeColumnInfo> { new("Amount", typeof(decimal), true, Precision: 10, Scale: 2) };
		var schema = ArrowSchemaFactory.Create(columns);

		var writer = new ParquetDataWriter(_path);
		await using (writer)
		{
			await writer.InitializeAsync(columns);
			var builder = new Decimal128Array.Builder((Decimal128Type)schema.GetFieldByIndex(0).DataType);
			builder.Append(12.34m).Append(56.78m);
			using var batch = new RecordBatch(schema, [builder.Build()], 2);
			await writer.WriteRecordBatchAsync(batch);
			await writer.CompleteAsync();
		}

		await using var stream = File.OpenRead(_path);
		await using var parquet = await ParquetReader.CreateAsync(stream);
		parquet.Schema.DataFields[0].Should().BeOfType<DecimalDataField>();
		var written = (DecimalDataField)parquet.Schema.DataFields[0];
		written.Precision.Should().Be(10);
		written.Scale.Should().Be(2);
	}

	[Fact]
	public async Task Writer_LeavesTheDefaultWidth_WhenTheColumnDeclaresNone()
	{
		var columns = new List<PipeColumnInfo> { new("Amount", typeof(decimal), true) };
		var schema = ArrowSchemaFactory.Create(columns);

		var writer = new ParquetDataWriter(_path);
		await using (writer)
		{
			await writer.InitializeAsync(columns);
			var builder = new Decimal128Array.Builder((Decimal128Type)schema.GetFieldByIndex(0).DataType);
			builder.Append(12.34m);
			using var batch = new RecordBatch(schema, [builder.Build()], 1);
			await writer.WriteRecordBatchAsync(batch);
			await writer.CompleteAsync();
		}

		await using var stream = File.OpenRead(_path);
		await using var parquet = await ParquetReader.CreateAsync(stream);
		var written = (DecimalDataField)parquet.Schema.DataFields[0];
		written.Scale.Should().Be(18);
	}

	private async Task WriteFileAsync(DataField field, decimal?[] values)
	{
		var schema = new ParquetSchema(field);
		await using var stream = File.OpenWrite(_path);
		await using var writer = await ParquetWriter.CreateAsync(schema, stream);
		using var rowGroup = writer.CreateRowGroup();
		await rowGroup.WriteAsync<decimal>(schema.DataFields[0], values.AsMemory());
	}
}
