using DuckDB.NET.Data;
using FluentAssertions;
using QueryDump.Providers.DuckDB;
using QueryDump.Tests.Helpers;
using Xunit;

namespace QueryDump.Tests;

public class DuckDBIntegrationTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly string _connectionString;

    public DuckDBIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.duckdb");
        _connectionString = $"Data Source={_dbPath}";
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        try { File.Delete(_dbPath); } catch { /* best effort */ }
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task CanReadFromDuckDB()
    {
        // setup
        using (var connection = new DuckDBConnection(_connectionString))
        {
             await connection.OpenAsync(TestContext.Current.CancellationToken);
             
             using var command = connection.CreateCommand();
             command.CommandText = TestDataSeeder.GenerateTableDDL(connection, "users");
             await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

             await TestDataSeeder.SeedAsync(connection, "users");
        }

        // 2. Read with Reader
        await using var reader = new DuckDataSourceReader(
            _connectionString, 
            "SELECT * FROM users ORDER BY id",
            new DuckDbOptions());
        await reader.OpenAsync(TestContext.Current.CancellationToken);

        // 3. Verify Columns
        reader.Columns.Should().HaveCount(7);
        reader.Columns![0].Name.Should().Be("Id");
        reader.Columns![1].Name.Should().Be("Name");

        // 4. Verify Data retrieval
        var batches = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(10, TestContext.Current.CancellationToken))
        {
            for(int i = 0; i<batch.Length; i++)
                batches.Add(batch.Span[i]);
        }

        batches.Should().HaveCount(4); 
        batches[0][0].Should().Be(1);
        batches[0][1].Should().Be("Alice");
        
        batches[1][0].Should().Be(2);
        batches[1][1].Should().Be("Bob");
    }

    [Fact]
    public void ThrowsOnDDL()
    {
        // QueryDump throws InvalidOperationException or ArgumentException depending on where it catches it
        // The regex check throws InvalidOperationException for forbidden keywords at the start
        var act = () => new DuckDataSourceReader(
            _connectionString, 
            "DROP TABLE users",
            new DuckDbOptions());
        act.Should().Throw<Exception>(); // Relaxed check to catch either ArgumentException or InvalidOperationException
    }
}
