using FluentAssertions;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Providers.Csv;
using Xunit;

namespace QueryDump.Tests;

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
        var options = new CsvReaderOptions { Delimiter = ",", HasHeader = true };
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
            var options = new CsvReaderOptions { Delimiter = ";", HasHeader = true };
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
    public void CsvReaderFactory_ShouldDetectCsvFiles()
    {
        var registry = new OptionsRegistry();
        registry.Register(new CsvReaderOptions());
        var factory = new CsvReaderFactory(registry);

        factory.CanHandle("csv:data.csv").Should().BeTrue();
        factory.CanHandle("data.csv").Should().BeTrue();
        factory.CanHandle("path/to/file.CSV").Should().BeTrue();
        factory.CanHandle("data.parquet").Should().BeFalse();
        factory.CanHandle("sqlite:test.db").Should().BeFalse();
    }
}
