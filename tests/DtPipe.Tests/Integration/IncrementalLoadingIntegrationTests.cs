using DuckDB.NET.Data;
using FluentAssertions;
using DtPipe.Adapters.DuckDB;
using DtPipe.Tests.Helpers;
using Xunit;
using DtPipe.Core.Models;
using Microsoft.Extensions.Logging.Abstractions;
using DtPipe.Core.Abstractions;

namespace DtPipe.Tests.Integration;

public class IncrementalLoadingIntegrationTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly string _connectionString;

    public IncrementalLoadingIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"inc_test_{Guid.NewGuid()}.duckdb");
        _connectionString = $"Data Source={_dbPath}";
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;
    public ValueTask DisposeAsync()
    {
        try { File.Delete(_dbPath); } catch { }
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task DuckDB_Upsert_UpdatesExisting_InsertsNew()
    {
        // 1. Setup Table with Data
        await using var connection = new DuckDBConnection(_connectionString);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')";
        await cmd.ExecuteNonQueryAsync();

        // 2. Prepare Writer
        var options = new DuckDbWriterOptions { 
            Table = "users", 
            Strategy = DuckDbWriteStrategy.Upsert,
            Key = "id"
        };
        await using var writer = new DuckDbDataWriter(_connectionString, options);
        
        var columns = new List<DtPipe.Core.Models.ColumnInfo> { 
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };
        await writer.InitializeAsync(columns);

        // 3. Write Batch (1 Updated, 3 New)
        // Update Alice -> Alice V2
        // Insert Charlie
        var batch = new List<object?[]> {
            new object[] { 1, "Alice V2" },
            new object[] { 3, "Charlie" }
        };
        await writer.WriteBatchAsync(batch);
        await writer.CompleteAsync();

        // 4. Verify
        var result = await QueryAll("SELECT id, name FROM users ORDER BY id");
        result.Should().HaveCount(3);
        result[0][1].Should().Be("Alice V2");
        result[1][1].Should().Be("Bob");
        result[2][1].Should().Be("Charlie");
    }

    [Fact]
    public async Task DuckDB_Ignore_SkipsExisting_InsertsNew()
    {
         // 1. Setup Table with Data
        await using var connection = new DuckDBConnection(_connectionString);
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "CREATE TABLE users_ign (id INT PRIMARY KEY, name TEXT)";
        await cmd.ExecuteNonQueryAsync();
        cmd.CommandText = "INSERT INTO users_ign VALUES (1, 'Alice'), (2, 'Bob')";
        await cmd.ExecuteNonQueryAsync();

        // 2. Prepare Writer
        var options = new DuckDbWriterOptions { 
            Table = "users_ign", 
            Strategy = DuckDbWriteStrategy.Ignore,
            Key = "id"
        };
        await using var writer = new DuckDbDataWriter(_connectionString, options);
        
        var columns = new List<DtPipe.Core.Models.ColumnInfo> { 
            new("id", typeof(int), false),
            new("name", typeof(string), true)
        };
        await writer.InitializeAsync(columns);

        // 3. Write Batch
        // Update Alice -> Alice V2 (Should be ignored)
        // Insert Charlie
        var batch = new List<object?[]> {
            new object[] { 1, "Alice V2" }, // Should be ignored
            new object[] { 3, "Charlie" }
        };
        await writer.WriteBatchAsync(batch);
        await writer.CompleteAsync();

        // 4. Verify
        var result = await QueryAll("SELECT id, name FROM users_ign ORDER BY id");
        result.Should().HaveCount(3);
        result[0][1].Should().Be("Alice"); // Unchanged
        result[1][1].Should().Be("Bob");
        result[2][1].Should().Be("Charlie");
    }

    private async Task<List<object[]>> QueryAll(string sql)
    {
        var list = new List<object[]>();
        await using var conn = new DuckDBConnection(_connectionString);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();
        while(await reader.ReadAsync())
        {
            var row = new object[reader.FieldCount];
            reader.GetValues(row);
            list.Add(row);
        }
        return list;
    }
}
