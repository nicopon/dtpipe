using Apache.Arrow;
using AwesomeAssertions;
using DtPipe.Adapters.Csv;
using DtPipe.Adapters.JsonL;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Abstractions;
using Xunit;

namespace DtPipe.Tests.Unit.Readers;

/// <summary>
/// The files a glob matches are read as one stream. The schema is the first file's, and a file
/// that does not match it stops the run: a directory glob makes one stale file among many easy to
/// pick up, and reading on would drop or misalign its columns without saying so.
/// </summary>
public class ConcatenatedStreamReaderTests : IDisposable
{
	private readonly string _dir = Path.Combine(Path.GetTempPath(), $"concat_{Guid.NewGuid():N}");

	public ConcatenatedStreamReaderTests() => Directory.CreateDirectory(_dir);

	public void Dispose()
	{
		if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
		GC.SuppressFinalize(this);
	}

	private string Write(string name, string content)
	{
		var path = Path.Combine(_dir, name);
		File.WriteAllText(path, content);
		return path;
	}

	private static IStreamReader OpenCsv(string path) => new CsvStreamReader(path, new CsvReaderOptions());

	// CsvStreamReader is row-only; the columnar wrapper needs an adapter that is actually columnar,
	// which is the distinction CliStreamReaderFactory makes when it picks between the two.
	private static IStreamReader OpenJsonL(string path) => new JsonLStreamReader(path, new JsonLReaderOptions());

	[Fact]
	public async Task Rows_of_every_file_arrive_in_order()
	{
		var paths = new[]
		{
			Write("a.csv", "id,v\n1,a\n"),
			Write("b.csv", "id,v\n2,b\n"),
			Write("c.csv", "id,v\n3,c\n"),
		};

		await using var reader = new ConcatenatedStreamReader(paths, OpenCsv);
		await reader.OpenAsync();

		reader.Columns!.Select(c => c.Name).Should().Equal("id", "v");

		var values = new List<string?>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
			foreach (var row in batch.ToArray())
				values.Add(row[1]?.ToString());

		values.Should().Equal("a", "b", "c");
	}

	[Fact]
	public async Task A_single_file_behaves_like_no_glob_at_all()
	{
		var paths = new[] { Write("only.csv", "id,v\n1,a\n") };

		await using var reader = new ConcatenatedStreamReader(paths, OpenCsv);
		await reader.OpenAsync();

		var count = 0;
		await foreach (var batch in reader.ReadBatchesAsync(100)) count += batch.Length;
		count.Should().Be(1);
	}

	[Fact]
	public async Task A_file_with_different_columns_stops_the_run_and_names_it()
	{
		var paths = new[]
		{
			Write("a.csv", "id,v\n1,a\n"),
			Write("z_stale.csv", "id,other\n2,b\n"),
		};

		await using var reader = new ConcatenatedStreamReader(paths, OpenCsv);
		await reader.OpenAsync();

		var act = async () =>
		{
			await foreach (var _ in reader.ReadBatchesAsync(100)) { }
		};

		(await act.Should().ThrowAsync<InvalidOperationException>())
			.WithMessage("*z_stale.csv*").And.Message.Should().Contain("other");
	}

	[Fact]
	public async Task The_columnar_form_carries_the_arrow_schema_and_every_batch()
	{
		var paths = new[]
		{
			Write("a.jsonl", "{\"id\":1,\"v\":\"a\"}\n"),
			Write("b.jsonl", "{\"id\":2,\"v\":\"b\"}\n"),
		};

		await using var reader = new ConcatenatedColumnarStreamReader(paths, OpenJsonL);
		await reader.OpenAsync();

		reader.Schema.Should().NotBeNull("the engine reads row-vs-columnar mode off this type");
		reader.Schema!.FieldsList.Select(f => f.Name).Should().Equal("id", "v");

		var rows = 0;
		await foreach (var batch in reader.ReadRecordBatchesAsync())
		{
			using (batch) rows += batch.Length;
		}

		rows.Should().Be(2);
	}
}
