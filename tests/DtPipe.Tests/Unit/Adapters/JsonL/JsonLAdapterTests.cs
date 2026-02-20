using DtPipe.Adapters.JsonL;
using DtPipe.Core.Models;
using FluentAssertions;
using Xunit;

namespace DtPipe.Tests.Unit.Adapters.JsonL;

public class JsonLAdapterTests : IAsyncLifetime
{
	private string _testJsonLPath = null!;

	public ValueTask InitializeAsync()
	{
		_testJsonLPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.jsonl");
		return ValueTask.CompletedTask;
	}

	public ValueTask DisposeAsync()
	{
		if (File.Exists(_testJsonLPath)) File.Delete(_testJsonLPath);
		return ValueTask.CompletedTask;
	}

	[Fact]
	public async Task JsonLStreamReader_ShouldReadDataAndInferSchema()
	{
		// Arrange
		var content = """
            {"Id": 1, "Name": "Alice", "Active": true}
            {"Id": 2, "Name": "Bob", "Active": false}
            """;
		await File.WriteAllTextAsync(_testJsonLPath, content);

		var options = new JsonLReaderOptions();
		var reader = new JsonLStreamReader(_testJsonLPath, options);

		// Act
		await reader.OpenAsync();
		var columns = reader.Columns;
		var rows = new List<object?[]>();
		await foreach (var batch in reader.ReadBatchesAsync(100))
		{
			rows.AddRange(batch.ToArray());
		}
		await reader.DisposeAsync();

		// Assert
		columns.Should().HaveCount(3);
		columns![0].Name.Should().Be("Id");
		columns[1].Name.Should().Be("Name");
		columns[2].Name.Should().Be("Active");

		rows.Should().HaveCount(2);
		rows[0][0].Should().Be(1.0); // Numbers are inferred as double by default in my implementation
		rows[0][1].Should().Be("Alice");
		rows[0][2].Should().Be(true);
	}

	[Fact]
	public async Task JsonLDataWriter_ShouldWriteData()
	{
		// Arrange
		var columns = new List<PipeColumnInfo>
		{
			new("Id", typeof(int), false),
			new("Name", typeof(string), true),
			new("Active", typeof(bool), false)
		};

		var rows = new List<object?[]>
		{
			new object?[] { 1, "Alice", true },
			new object?[] { 2, "Bob", false }
		};

		var options = new JsonLWriterOptions();
		var writer = new JsonLDataWriter(_testJsonLPath, options);

		// Act
		await writer.InitializeAsync(columns);
		await writer.WriteBatchAsync(rows);
		await writer.CompleteAsync();
		await writer.DisposeAsync();

		// Assert
		var lines = await File.ReadAllLinesAsync(_testJsonLPath);
		lines.Should().HaveCount(2);
		lines[0].Should().Contain("\"Id\":1");
		lines[0].Should().Contain("\"Name\":\"Alice\"");
		lines[0].Should().Contain("\"Active\":true");
	}

    [Fact]
    public async Task JsonLReaderAndWriter_ShouldWorkTogether()
    {
        // Arrange
        var initialPath = Path.Combine(Path.GetTempPath(), $"initial_{Guid.NewGuid()}.jsonl");
        var finalPath = Path.Combine(Path.GetTempPath(), $"final_{Guid.NewGuid()}.jsonl");
        try
        {
            await File.WriteAllTextAsync(initialPath, "{\"A\":1,\"B\":\"X\"}\n{\"A\":2,\"B\":\"Y\"}");

            // Act 1: Read
            var reader = new JsonLStreamReader(initialPath, new JsonLReaderOptions());
            await reader.OpenAsync();
            var columns = reader.Columns!;
            var rows = new List<object?[]>();
            await foreach(var batch in reader.ReadBatchesAsync(10))
            {
                rows.AddRange(batch.ToArray());
            }

            // Act 2: Write
            var writer = new JsonLDataWriter(finalPath, new JsonLWriterOptions());
            await writer.InitializeAsync(columns);
            await writer.WriteBatchAsync(rows);
            await writer.CompleteAsync();
            await writer.DisposeAsync();

            // Assert
            var resultLines = await File.ReadAllLinesAsync(finalPath);
            resultLines.Should().HaveCount(2);
            resultLines[0].Should().Be("{\"A\":1,\"B\":\"X\"}");
            resultLines[1].Should().Be("{\"A\":2,\"B\":\"Y\"}");
        }
        finally
        {
            if (File.Exists(initialPath)) File.Delete(initialPath);
            if (File.Exists(finalPath)) File.Delete(finalPath);
        }
    }
}
