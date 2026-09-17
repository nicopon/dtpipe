using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using AwesomeAssertions;
using DtPipe.Adapters.Arrow;
using Xunit;

namespace DtPipe.Tests.Unit.Readers;

/// <summary>
/// An Arrow IPC stream ends with the marker <c>ffffffff 00000000</c>. A producer killed between
/// two messages leaves a stream that stops on a clean boundary, and the reader used to report that
/// the same way it reports a complete one: 100 000 rows of 400 000 written, exit 0, not a word.
///
/// The three cases below are the whole contract. The first is the one that keeps the fix honest —
/// the probe rests on the Arrow reader not reading past the marker, so if a future version reads
/// ahead, that case goes red and complete streams start being refused loudly, rather than
/// truncated ones being accepted in silence.
/// </summary>
public class ArrowStreamTruncationTests
{
	private const int Batches = 3;
	private const int RowsPerBatch = 4;

	private static readonly Schema Schema =
		new([new Field("n", Int32Type.Default, nullable: false)], null);

	private static byte[] CompleteStream()
	{
		var ms = new MemoryStream();
		using (var writer = new ArrowStreamWriter(ms, Schema, leaveOpen: true))
		{
			for (int b = 0; b < Batches; b++)
			{
				var builder = new Int32Array.Builder();
				for (int i = 0; i < RowsPerBatch; i++) builder.Append(b * RowsPerBatch + i);
				using var batch = new RecordBatch(Schema, [builder.Build()], RowsPerBatch);
				writer.WriteRecordBatch(batch);
			}
			writer.WriteEnd();
		}
		return ms.ToArray();
	}

	private static string WriteFile(byte[] bytes, int dropTrailingBytes = 0)
	{
		var path = Path.Combine(Path.GetTempPath(), $"dtpipe-arrow-{Guid.NewGuid():N}.arrows");
		File.WriteAllBytes(path, bytes.AsSpan(0, bytes.Length - dropTrailingBytes).ToArray());
		return path;
	}

	private static async Task<(int Rows, Exception? Error)> ReadAll(string path)
	{
		int rows = 0;
		try
		{
			await using var reader = new ArrowAdapterStreamReader(path, new ArrowReaderOptions());
			await reader.OpenAsync();
			await foreach (var batch in reader.ReadRecordBatchesAsync())
				using (batch) rows += batch.Length;
			return (rows, null);
		}
		catch (Exception ex)
		{
			return (rows, ex);
		}
	}

	[Fact]
	public async Task A_complete_stream_is_read_to_the_end_and_accepted()
	{
		var path = WriteFile(CompleteStream());
		try
		{
			var (rows, error) = await ReadAll(path);
			error.Should().BeNull();
			rows.Should().Be(Batches * RowsPerBatch);
		}
		finally { File.Delete(path); }
	}

	[Fact]
	public async Task A_stream_missing_only_its_end_marker_is_refused()
	{
		var complete = CompleteStream();
		Convert.ToHexString(complete.AsSpan(complete.Length - 8))
			.Should().Be("FFFFFFFF00000000", "the marker is what the reader must require");

		var path = WriteFile(complete, dropTrailingBytes: 8);
		try
		{
			var (_, error) = await ReadAll(path);
			error.Should().BeOfType<EndOfStreamException>()
				.Which.Message.Should().Contain("truncated");
		}
		finally { File.Delete(path); }
	}

	[Fact]
	public async Task A_stream_cut_inside_a_message_stays_refused()
	{
		var path = WriteFile(CompleteStream(), dropTrailingBytes: 200);
		try
		{
			var (_, error) = await ReadAll(path);
			error.Should().NotBeNull();
		}
		finally { File.Delete(path); }
	}

	[Fact]
	public async Task The_row_mode_reader_refuses_a_truncated_stream_too()
	{
		var path = WriteFile(CompleteStream(), dropTrailingBytes: 8);
		try
		{
			await using var reader = new ArrowAdapterStreamReader(path, new ArrowReaderOptions());
			await reader.OpenAsync();

			var read = async () =>
			{
				await foreach (var _ in reader.ReadBatchesAsync(1024)) { }
			};

			await read.Should().ThrowAsync<EndOfStreamException>();
		}
		finally { File.Delete(path); }
	}
}
