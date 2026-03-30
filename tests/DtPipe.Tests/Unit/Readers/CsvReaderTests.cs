using DtPipe.Adapters.Csv;
using DtPipe.Cli.Infrastructure;
using DtPipe.Core.Options;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests;

public class CsvReaderTests : IAsyncLifetime
{
	private string _testCsvPath = null!;

	public async ValueTask InitializeAsync()
	{
		_testCsvPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.csv");

		// Create test CSV file
		var content = """
            Id,Name,Score,Active
            1,Alice,95.5,true
            2,Bob,87.3,false
            3,Charlie,92.0,true
            """;
		await File.WriteAllTextAsync(_testCsvPath, content);
	}

	public ValueTask DisposeAsync()
	{
		if (File.Exists(_testCsvPath)) File.Delete(_testCsvPath);
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task CsvReader_ShouldReadHeaderAndData()
	{
		// Arrange
		var options = new CsvReaderOptions { Separator = ",", HasHeader = true };
		var reader = new CsvStreamReader(_testCsvPath, options);

		// Act
		await reader.OpenAsync();
		var columns = reader.Columns;
		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			foreach (var row in batch.ToArray())
			{
				rows.Add(row);
			}
		}
		await reader.DisposeAsync();

		// Assert
		columns.Should().HaveCount(4);
		columns![0].Name.Should().Be("Id");
		columns[1].Name.Should().Be("Name");
		columns[2].Name.Should().Be("Score");
		columns[3].Name.Should().Be("Active");

		rows.Should().HaveCount(3);
		rows[0][0].Should().Be("1");
		rows[0][1].Should().Be("Alice");
		rows[1][1].Should().Be("Bob");
		rows[2][1].Should().Be("Charlie");
	}

	[Fact]
	public async Task CsvReader_ShouldHandleCustomDelimiter()
	{
		// Arrange: Create semicolon-delimited file
		var semicolonPath = Path.Combine(Path.GetTempPath(), $"test_semicolon_{Guid.NewGuid()}.csv");
		await File.WriteAllTextAsync(semicolonPath, "A;B;C\n1;2;3\n4;5;6");

		try
		{
			var options = new CsvReaderOptions { Separator = ";", HasHeader = true };
			var reader = new CsvStreamReader(semicolonPath, options);

			// Act
			await reader.OpenAsync();
			var columns = reader.Columns;
			var rows = new List<object?[]>();
			await foreach (var batch in reader.ReadBatchesAsync(100))
			{
				foreach (var row in batch.ToArray())
				{
					rows.Add(row);
				}
			}
			await reader.DisposeAsync();

			// Assert
			columns.Should().HaveCount(3);
			columns![0].Name.Should().Be("A");
			rows.Should().HaveCount(2);
			rows[0][0].Should().Be("1");
		}
		finally
		{
			if (File.Exists(semicolonPath)) File.Delete(semicolonPath);
		}
	}

	[Fact]
	public async Task CsvReader_ColumnTypes_ShouldParseUuidAndInt()
	{
		// Arrange: CSV with UUID-formatted Id and numeric Score
		var uuidPath = Path.Combine(Path.GetTempPath(), $"test_uuid_{Guid.NewGuid()}.csv");
		var guid1 = Guid.NewGuid();
		var guid2 = Guid.NewGuid();
		await File.WriteAllTextAsync(uuidPath, $"Id,Name,Score\n{guid1},Alice,42\n{guid2},Bob,99");

		try
		{
			var options = new CsvReaderOptions
			{
				Separator = ",",
				HasHeader = true,
				ColumnTypes = "Id:uuid,Score:int32"
			};
			var reader = new CsvStreamReader(uuidPath, options);

			// Act
			await reader.OpenAsync();
			var columns = reader.Columns!;
			var rows = new List<object?[]>();
			await foreach (var batch in reader.ReadBatchesAsync(100))
				foreach (var row in batch.ToArray())
					rows.Add(row);
			await reader.DisposeAsync();

			// Assert: schema reflects declared types
			columns.Should().HaveCount(3);
			columns[0].ClrType.Should().Be(typeof(Guid));
			columns[1].ClrType.Should().Be(typeof(string));
			columns[2].ClrType.Should().Be(typeof(int));

			// Assert: row values are parsed
			rows.Should().HaveCount(2);
			rows[0][0].Should().BeOfType<Guid>().Which.Should().Be(guid1);
			rows[0][2].Should().BeOfType<int>().Which.Should().Be(42);
			rows[1][0].Should().BeOfType<Guid>().Which.Should().Be(guid2);
			rows[1][2].Should().BeOfType<int>().Which.Should().Be(99);
		}
		finally
		{
			if (File.Exists(uuidPath)) File.Delete(uuidPath);
		}
	}

	[Fact]
	public async Task CsvReader_InferColumnTypes_ShouldSuggestUuid()
	{
		// Arrange: CSV with standard UUID strings
		var uuidPath = Path.Combine(Path.GetTempPath(), $"test_infer_{Guid.NewGuid()}.csv");
		var lines = Enumerable.Range(0, 10).Select(i => $"{Guid.NewGuid()},value{i}");
		await File.WriteAllTextAsync(uuidPath, "Id,Name\n" + string.Join("\n", lines));

		try
		{
			var options = new CsvReaderOptions { Separator = ",", HasHeader = true };
			var reader = new CsvStreamReader(uuidPath, options);
			await reader.OpenAsync();

			// Act
			var suggestions = await reader.InferColumnTypesAsync(10);
			await reader.DisposeAsync();

			// Assert
			suggestions.Should().ContainKey("Id");
			suggestions["Id"].Should().Be("uuid");
		}
		finally
		{
			if (File.Exists(uuidPath)) File.Delete(uuidPath);
		}
	}

	[Fact]
	public void CsvReaderFactory_ShouldDetectCsvFiles()
	{
		var registry = new OptionsRegistry();
		registry.Register(new CsvReaderOptions());
		var factory = new CliStreamReaderFactory(new CsvReaderDescriptor(), registry, null!);

		factory.CanHandle("csv:data.csv").Should().BeTrue();
		factory.CanHandle("data.csv").Should().BeTrue();
		factory.CanHandle("path/to/file.CSV").Should().BeTrue();
		factory.CanHandle("data.parquet").Should().BeFalse();
		factory.CanHandle("sqlite:test.db").Should().BeFalse();
	}
}
