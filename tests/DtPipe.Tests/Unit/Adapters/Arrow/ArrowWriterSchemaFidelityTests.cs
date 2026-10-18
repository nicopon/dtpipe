using Apache.Arrow;
using Apache.Arrow.Serialization.Reflection;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DtPipe.Adapters.Arrow;
using DtPipe.Core.Infrastructure.Arrow;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.Arrow;

/// <summary>
/// The writer declares the stream's schema from the first batch it receives: the columns
/// (<see cref="PipeColumnInfo"/>) cannot say a timestamp's zone or an extension's metadata.
/// </summary>
public class ArrowWriterSchemaFidelityTests : IDisposable
{
	private readonly string _dir = Path.Combine(Path.GetTempPath(), $"dtpipe_arrowfidelity_{Guid.NewGuid():N}");

	public ArrowWriterSchemaFidelityTests() => Directory.CreateDirectory(_dir);

	public void Dispose()
	{
		try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
	}

	private static readonly IReadOnlyList<PipeColumnInfo> Columns =
	[
		new("ts", typeof(DateTimeOffset), true),
		new("u", typeof(Guid), true),
	];

	private static RecordBatch RichBatch()
	{
		var uuidMetadata = new Dictionary<string, string>
		{
			["ARROW:extension:name"] = "arrow.uuid",
			["ARROW:extension:metadata"] = "",
		};
		var schema = new Schema.Builder()
			.Field(new Field("ts", new TimestampType(TimeUnit.Microsecond, "Europe/Paris"), true))
			.Field(new Field("u", new FixedSizeBinaryType(16), true, uuidMetadata))
			.Build();
		var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "Europe/Paris"))
			.Append(new DateTimeOffset(2026, 10, 9, 12, 0, 0, TimeSpan.Zero)).Build();
		var uuids = new FixedSizeBinaryArrayBuilder(16);
		ArrowTypeMapper.AppendValue(uuids, Guid.NewGuid());
		var u = uuids.Build();
		return new RecordBatch(schema, [ts, u], 1);
	}

	private async Task WriteAsync(string path, params RecordBatch[] batches)
	{
		await using var writer = new ArrowAdapterDataWriter(path);
		await writer.InitializeAsync(Columns);
		foreach (var batch in batches) await writer.WriteRecordBatchAsync(batch);
		await writer.CompleteAsync();
	}

	private static async Task<Schema> ReadSchemaAsync(string path, bool file)
	{
		await using var fs = File.OpenRead(path);
		if (file)
		{
			using var reader = new ArrowFileReader(fs);
			await reader.ReadNextRecordBatchAsync();
			return reader.Schema;
		}
		using var streamReader = new ArrowStreamReader(fs);
		await streamReader.ReadNextRecordBatchAsync();
		return streamReader.Schema;
	}

	[Theory]
	[InlineData("out.arrow", true)]
	[InlineData("out.ipc", false)]
	public async Task The_Zone_And_The_Extension_Metadata_Of_The_Batch_Reach_The_Stream(string name, bool file)
	{
		var path = Path.Combine(_dir, name);

		await WriteAsync(path, RichBatch());

		var schema = await ReadSchemaAsync(path, file);
		var ts = Assert.IsType<TimestampType>(schema.GetFieldByName("ts").DataType);
		Assert.Equal("Europe/Paris", ts.Timezone);
		var u = schema.GetFieldByName("u");
		Assert.Equal("arrow.uuid", u.Metadata["ARROW:extension:name"]);
		Assert.True(u.Metadata.ContainsKey("ARROW:extension:metadata"));
	}

	[Theory]
	[InlineData("empty.arrow", true)]
	[InlineData("empty.ipc", false)]
	public async Task A_Stream_Without_A_Batch_Still_Declares_The_Schema_Of_The_Columns(string name, bool file)
	{
		var path = Path.Combine(_dir, name);

		await WriteAsync(path);

		var schema = await ReadSchemaAsync(path, file);
		Assert.Equal(["ts", "u"], schema.FieldsList.Select(f => f.Name));
		Assert.IsType<TimestampType>(schema.GetFieldByName("ts").DataType);
	}

	[Theory]
	[InlineData("drift.arrow")]
	[InlineData("drift.ipc")]
	public async Task A_Later_Batch_That_Differs_From_The_First_Is_Refused(string name)
	{
		var path = Path.Combine(_dir, name);
		var otherZone = new TimestampType(TimeUnit.Microsecond, "UTC");
		var schema = new Schema.Builder()
			.Field(new Field("ts", otherZone, true))
			.Field(new Field("u", new FixedSizeBinaryType(16), true))
			.Build();
		var ts = new TimestampArray.Builder(otherZone).Append(DateTimeOffset.UnixEpoch).Build();
		var uuids = new FixedSizeBinaryArrayBuilder(16);
		ArrowTypeMapper.AppendValue(uuids, Guid.NewGuid());
		var u = uuids.Build();
		var drifted = new RecordBatch(schema, [ts, u], 1);

		var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => WriteAsync(path, RichBatch(), drifted));

		Assert.Contains("'ts'", ex.Message);
	}
}
