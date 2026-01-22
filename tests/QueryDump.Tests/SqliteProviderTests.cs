using FluentAssertions;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using QueryDump.Configuration;
using QueryDump.Core;
using QueryDump.Core.Options;
using QueryDump.Adapters.Sqlite;
using Xunit;

namespace QueryDump.Tests;

public class SqliteProviderTests : IAsyncLifetime
{
    private string _testDbPath = null!;
    private string _outputDbPath = null!;
    private string _connectionString = null!;
    private string _outputConnectionString = null!;

    public ValueTask InitializeAsync()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid()}.sqlite");
        _outputDbPath = Path.Combine(Path.GetTempPath(), $"output_{Guid.NewGuid()}.sqlite");
        _connectionString = $"Data Source={_testDbPath}";
        _outputConnectionString = $"Data Source={_outputDbPath}";
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (File.Exists(_testDbPath)) File.Delete(_testDbPath);
        if (File.Exists(_outputDbPath)) File.Delete(_outputDbPath);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task SqliteReader_ShouldReadData()
    {
        // Arrange: Create test database
        await using (var conn = new SqliteConnection(_connectionString))
        {
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE Users (Id INTEGER, Name TEXT, Age INTEGER)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "INSERT INTO Users VALUES (1, 'Alice', 30), (2, 'Bob', 25)";
            await cmd.ExecuteNonQueryAsync();
        }

        // Act
        var reader = new SqliteStreamReader(_connectionString, "SELECT * FROM Users");
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
        columns![0].Name.Should().Be("Id");
        columns[1].Name.Should().Be("Name");
        columns[2].Name.Should().Be("Age");
        rows.Should().HaveCount(2);
        rows[0][1].Should().Be("Alice");
        rows[1][1].Should().Be("Bob");
    }

    [Fact]
    public async Task SqliteWriter_ShouldWriteData()
    {
        // Arrange
        var registry = new OptionsRegistry();
        registry.Register(new SqliteWriterOptions { Table = "Export", Strategy = "Recreate" });

        var columns = new List<ColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Name", typeof(string), true),
            new("Score", typeof(double), true)
        };

        var rows = new List<object?[]>
        {
            new object?[] { 1, "Alice", 95.5 },
            new object?[] { 2, "Bob", 87.3 },
            new object?[] { 3, "Charlie", null }
        };

        // Act
        var writer = new SqliteDataWriter(_outputConnectionString, registry);
        await writer.InitializeAsync(columns);
        await writer.WriteBatchAsync(rows);
        await writer.CompleteAsync();
        await writer.DisposeAsync();

        // Assert: Read back
        await using var conn = new SqliteConnection(_outputConnectionString);
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Export";
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        count.Should().Be(3);
    }

    [Fact]
    public async Task SqliteRoundTrip_ShouldPreserveData()
    {
        // Arrange: Create source database
        await using (var conn = new SqliteConnection(_connectionString))
        {
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE Source (Id INTEGER, Value TEXT)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "INSERT INTO Source VALUES (1, 'Test1'), (2, 'Test2'), (3, 'Test3')";
            await cmd.ExecuteNonQueryAsync();
        }

        var registry = new OptionsRegistry();
        registry.Register(new SqliteWriterOptions { Table = "Target", Strategy = "Recreate" });

        // Act: Read from source
        var reader = new SqliteStreamReader(_connectionString, "SELECT * FROM Source");
        await reader.OpenAsync();
        var columns = reader.Columns!;

        // Write to target
        var writer = new SqliteDataWriter(_outputConnectionString, registry);
        await writer.InitializeAsync(columns);

        await foreach (var batch in reader.ReadBatchesAsync(100))
        {
            await writer.WriteBatchAsync(batch.ToArray().ToList());
        }

        await writer.CompleteAsync();
        await writer.DisposeAsync();
        await reader.DisposeAsync();

        // Assert: Verify target
        await using var verifyConn = new SqliteConnection(_outputConnectionString);
        await verifyConn.OpenAsync();
        using var verifyCmd = verifyConn.CreateCommand();
        verifyCmd.CommandText = "SELECT * FROM Target ORDER BY Id";
        await using var result = await verifyCmd.ExecuteReaderAsync();
        
        var readRows = new List<(long Id, string Value)>();
        while (await result.ReadAsync())
        {
            readRows.Add((result.GetInt64(0), result.GetString(1)));
        }

        readRows.Should().HaveCount(3);
        readRows[0].Value.Should().Be("Test1");
        readRows[2].Value.Should().Be("Test3");
    }

    [Fact]
    public void SqliteConnectionHelper_ShouldDetectSqlitePaths()
    {
        SqliteConnectionHelper.CanHandle("sqlite:test.db").Should().BeTrue();
        SqliteConnectionHelper.CanHandle("test.sqlite").Should().BeTrue();
        SqliteConnectionHelper.CanHandle("test.sqlite3").Should().BeTrue();
        SqliteConnectionHelper.CanHandle("test.db").Should().BeFalse(); // .db is DuckDB by default
        SqliteConnectionHelper.CanHandle("oracle:connection").Should().BeFalse();
    }
}
