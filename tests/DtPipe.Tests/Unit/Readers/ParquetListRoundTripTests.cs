using Apache.Arrow;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Adapters.Parquet;
using DtPipe.Core.Models;
using Xunit;

namespace DtPipe.Tests.Unit.Readers;

/// <summary>
/// dtpipe could not read back a single Parquet list file it had written itself: the reader dropped
/// a list column from its schema, then read the column's flattened leaf — one entry per element —
/// into a buffer sized by row count.
///
/// Every assertion here goes through <see cref="ParquetStreamReader"/>. Reading the file back with
/// DuckDB's read_parquet is what <c>validate_nested_types.sh</c> did, and it is exactly why the
/// defect survived: it proves the writer and says nothing about the reader.
/// </summary>
public class ParquetListRoundTripTests : IDisposable
{
	private readonly string _path = Path.Combine(Path.GetTempPath(), $"list_{Guid.NewGuid():N}.parquet");

	public void Dispose()
	{
		if (File.Exists(_path)) File.Delete(_path);
		GC.SuppressFinalize(this);
	}

	/// <summary>The four shapes the three-level encoding has to keep apart, plus an ordinary one.</summary>
	private static RecordBatch FiveShapes()
	{
		var item = new Field("element", Int32Type.Default, nullable: true);
		var schema = new Schema(
			new[] { new Field("id", Int32Type.Default, true), new Field("vals", new ListType(item), true) },
			null);

		var id = new Int32Array.Builder().Append(1).Append(2).Append(3).Append(4).Append(5).Build();

		var values = new Int32Array.Builder();
		var offsets = new ArrowBuffer.Builder<int>();
		var validity = new ArrowBuffer.BitmapBuilder();

		offsets.Append(0);
		values.Append(10).Append(20).Append(30); validity.Append(true); offsets.Append(3); // [10,20,30]
		validity.Append(false); offsets.Append(3);                                          // NULL
		validity.Append(true); offsets.Append(3);                                           // []
		values.Append(7); validity.Append(true); offsets.Append(4);                         // [7]
		values.Append(1); values.AppendNull(); values.Append(3);
		validity.Append(true); offsets.Append(7);                                           // [1,null,3]

		var vals = new ListArray(new ListType(item), 5, offsets.Build(), values.Build(),
			validity.Build(), nullCount: 1);

		return new RecordBatch(schema, new IArrowArray[] { id, vals }, 5);
	}

	private static List<PipeColumnInfo> Columns() =>
		[new("id", typeof(int), true), new("vals", typeof(int?[]), true)];

	private async Task WriteAsync()
	{
		var writer = new ParquetDataWriter(_path);
		await writer.InitializeAsync(Columns());
		await writer.WriteRecordBatchAsync(FiveShapes());
		await writer.CompleteAsync();
		await writer.DisposeAsync();
	}

	[Fact]
	public async Task The_reader_declares_the_list_column()
	{
		await WriteAsync();

		await using var reader = new ParquetStreamReader(_path);
		await reader.OpenAsync();

		// A list column used to be dropped from the schema while the read loop still found it.
		reader.Columns!.Select(c => c.Name).Should().Equal("id", "vals");
		reader.Columns![1].ClrType.Should().Be(typeof(int?[]));
	}

	[Fact]
	public async Task The_row_path_reads_every_shape_back()
	{
		await WriteAsync();

		await using var reader = new ParquetStreamReader(_path);
		await reader.OpenAsync();

		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100)) rows.AddRange(batch.ToArray());

		rows.Should().HaveCount(5);
		((int?[])rows[0][1]!).Should().Equal(10, 20, 30);
		rows[1][1].Should().BeNull("a NULL list is not an empty one");
		((int?[])rows[2][1]!).Should().BeEmpty("an empty list is not a NULL one");
		((int?[])rows[3][1]!).Should().Equal(7);
		((int?[])rows[4][1]!).Should().Equal([1, null, 3], "a NULL element keeps its slot");
	}

	[Fact]
	public async Task The_columnar_path_reads_every_shape_back()
	{
		await WriteAsync();

		await using var reader = new ParquetStreamReader(_path);
		await reader.OpenAsync();

		var lists = new List<string?>();
		await foreach (var batch in reader.ReadRecordBatchesAsync())
		{
			using (batch)
			{
				var column = (ListArray)batch.Column(1);
				for (var i = 0; i < batch.Length; i++)
				{
					if (column.IsNull(i)) { lists.Add(null); continue; }
					var values = (Int32Array)column.GetSlicedValues(i);
					lists.Add(string.Join(",", Enumerable.Range(0, values.Length)
						.Select(j => values.IsNull(j) ? "null" : values.GetValue(j)!.Value.ToString())));
				}
			}
		}

		lists.Should().Equal(["10,20,30", null, "", "7", "1,null,3"]);
	}
}
