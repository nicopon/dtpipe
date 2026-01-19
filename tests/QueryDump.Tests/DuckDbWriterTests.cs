using System.Data;
using DuckDB.NET.Data;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Core;
using QueryDump.Configuration;
using QueryDump.Core.Options;
using QueryDump.Providers.DuckDB;
using Xunit;
using ColumnInfo = QueryDump.Core.ColumnInfo;

namespace QueryDump.Tests;

public class DuckDbWriterTests : IAsyncLifetime
{
    private readonly string _outputPath;
    private readonly DuckDbDataWriterFactory _factory;
    private readonly OptionsRegistry _registry;

    public DuckDbWriterTests()
    {
        _outputPath = Path.GetTempFileName() + ".duckdb";
        _registry = new OptionsRegistry();
        _factory = new DuckDbDataWriterFactory(_registry);
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        if (File.Exists(_outputPath))
        {
            try { File.Delete(_outputPath); } catch { }
        }
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void CanHandle_Detects_DuckDb_Patterns()
    {
        _factory.CanHandle("output.duckdb").Should().BeTrue();
        _factory.CanHandle("output.db").Should().BeTrue();
        _factory.CanHandle("duckdb:output.foo").Should().BeTrue();
        _factory.CanHandle("output.csv").Should().BeFalse();
    }

    [Fact]
    public async Task Write_CreatesTable_And_InsertsData()
    {
        // Arrange
        var options = new DumpOptions 
        { 
            OutputPath = _outputPath, 
            ConnectionString = "fake_connection", 
            Query = "SELECT 1" 
        };
        var writer = _factory.Create(options);

        var columns = new List<ColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Name", typeof(string), true),
            new("IsActive", typeof(bool), false),
            new("Score", typeof(double), false)
        };

        var rows = new List<object?[]>
        {
            new object?[] { 1, "Alice", true, 95.5 },
            new object?[] { 2, "Bob", false, 80.0 },
            new object?[] { 3, null, true, 0.0 }
        };

        // Act
        await writer.InitializeAsync(columns, CancellationToken.None);
        await writer.WriteBatchAsync(rows, CancellationToken.None);
        await writer.CompleteAsync(CancellationToken.None);
        await writer.DisposeAsync();

        // Assert
        using var connection = new DuckDBConnection($"Data Source={_outputPath}");
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM Export ORDER BY Id";
        using var reader = await cmd.ExecuteReaderAsync();

        // Row 1
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        reader.GetString(1).Should().Be("Alice");
        reader.GetBoolean(2).Should().BeTrue();
        reader.GetDouble(3).Should().Be(95.5);

        // Row 2
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);
        reader.GetString(1).Should().Be("Bob");
        reader.GetBoolean(2).Should().BeFalse();

        // Row 3 (Null Name)
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(3);
        reader.IsDBNull(1).Should().BeTrue();

        reader.Read().Should().BeFalse();
    }
}
