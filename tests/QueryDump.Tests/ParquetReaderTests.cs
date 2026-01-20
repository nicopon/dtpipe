using FluentAssertions;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Providers.Parquet;
using Xunit;

namespace QueryDump.Tests;

public class ParquetReaderTests : IAsyncLifetime
{
    private string _testParquetPath = null!;

    public async ValueTask InitializeAsync()
    {
        _testParquetPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.parquet");
        
        // Create test Parquet file
        var schema = new ParquetSchema(
            new DataField<int>("Id"),
            new DataField<string>("Name"),
            new DataField<double>("Score")
        );

        var idColumn = new DataColumn(schema.DataFields[0], new int[] { 1, 2, 3 });
        var nameColumn = new DataColumn(schema.DataFields[1], new string[] { "Alice", "Bob", "Charlie" });
        var scoreColumn = new DataColumn(schema.DataFields[2], new double[] { 95.5, 87.3, 92.0 });

        await using var stream = File.OpenWrite(_testParquetPath);
        await using var writer = await ParquetWriter.CreateAsync(schema, stream);
        using var rowGroup = writer.CreateRowGroup();
        await rowGroup.WriteColumnAsync(idColumn);
        await rowGroup.WriteColumnAsync(nameColumn);
        await rowGroup.WriteColumnAsync(scoreColumn);
    }

    public ValueTask DisposeAsync()
    {
        if (File.Exists(_testParquetPath)) File.Delete(_testParquetPath);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task ParquetReader_ShouldReadSchema()
    {
        // Arrange
        var reader = new ParquetStreamReader(_testParquetPath);

        // Act
        await reader.OpenAsync();
        var columns = reader.Columns;
        await reader.DisposeAsync();

        // Assert
        columns.Should().HaveCount(3);
        columns![0].Name.Should().Be("Id");
        columns[0].ClrType.Should().Be(typeof(int));
        columns[1].Name.Should().Be("Name");
        columns[1].ClrType.Should().Be(typeof(string));
        columns[2].Name.Should().Be("Score");
        columns[2].ClrType.Should().Be(typeof(double));
    }

    [Fact]
    public async Task ParquetReader_ShouldReadData()
    {
        // Arrange
        var reader = new ParquetStreamReader(_testParquetPath);

        // Act
        await reader.OpenAsync();
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
        rows.Should().HaveCount(3);
        rows[0][0].Should().Be(1);
        rows[0][1].Should().Be("Alice");
        rows[0][2].Should().Be(95.5);
        rows[1][1].Should().Be("Bob");
        rows[2][1].Should().Be("Charlie");
    }

    [Fact]
    public void ParquetReaderFactory_ShouldDetectParquetFiles()
    {
        var registry = new OptionsRegistry();
        var factory = new ParquetReaderFactory(registry);

        factory.CanHandle("parquet:data.parquet").Should().BeTrue();
        factory.CanHandle("data.parquet").Should().BeTrue();
        factory.CanHandle("path/to/file.PARQUET").Should().BeTrue();
        factory.CanHandle("data.csv").Should().BeFalse();
        factory.CanHandle("sqlite:test.db").Should().BeFalse();
    }
}
