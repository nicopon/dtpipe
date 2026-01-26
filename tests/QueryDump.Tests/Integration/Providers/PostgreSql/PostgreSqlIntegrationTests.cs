using Testcontainers.PostgreSql;
using Xunit;
using Npgsql;
using QueryDump.Adapters.PostgreSQL;
using QueryDump.Tests.Helpers;

namespace QueryDump.Tests;

[Trait("Category", "Integration")]
[Collection("Docker Integration Tests")]
public class PostgreSqlIntegrationTests : IAsyncLifetime
{
    private PostgreSqlContainer? _postgres;

    public async ValueTask InitializeAsync()
    {
        if (!DockerHelper.IsAvailable()) return;

        try
        {
            _postgres = new PostgreSqlBuilder("postgres:15-alpine")
                .Build();
            await _postgres.StartAsync();

            await using var connection = new NpgsqlConnection(_postgres.GetConnectionString());
            await connection.OpenAsync();
            
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = TestDataSeeder.GenerateTableDDL(connection, "test_data");
            await cmd.ExecuteNonQueryAsync();

            await TestDataSeeder.SeedAsync(connection, "test_data");
        }
        catch (Exception)
        {
            _postgres = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_postgres is not null)
        {
            await _postgres.DisposeAsync();
        }
    }

    [Fact]
    public async Task PostgreSqlReader_ReadsAllRows()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        
        await using var reader = new PostgreSqlReader(
            connectionString, 
            "SELECT * FROM test_data ORDER BY Id", // Unquoted identifier for case-insensitive match (Postgres defaults to lowercase)
            timeout: 30);
            
        await reader.OpenAsync();
        
        var rows = new List<object?[]>();
        await foreach (var batch in reader.ReadBatchesAsync(10))
        {
            for(int i = 0; i < batch.Length; i++)
            {
                rows.Add(batch.Span[i]);
            }
        }
        
        Assert.Equal(4, rows.Count);
        Assert.Equal(7, reader.Columns!.Count);
        // Postgres returns lower case column names usually unless quoted in creation?
        // TestDataSeeder uses unquoted CREATE TABLE names, so Postgres lowercases them.
        Assert.Equal("id", reader.Columns[0].Name.ToLower());
        
        var alice = rows.First(r => r[0]?.ToString() == "1");
        // Check boolean
        Assert.Equal(true, alice[3]);
        // Check numeric
        Assert.Equal(95.50m, alice[4]);
    }
    
    [Fact]
    public async Task PostgreSqlWriter_WritesRows()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        var connectionString = _postgres.GetConnectionString();
        var targetTable = "test_export";
        
        // Options
        var options = new PostgreSqlWriterOptions 
        { 
            Table = targetTable, 
            Strategy = PostgreSqlWriteStrategy.Truncate 
        };
        
        // Prepare Data
        var columns = new List<QueryDump.Core.Models.ColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Name", typeof(string), true),
            new("Created", typeof(DateTime), true)
        };
        
        var data = new List<object?[]>
        {
            new object?[] { 1, "Test 1", DateTime.UtcNow },
            new object?[] { 2, "Test 2", DateTime.UtcNow.AddDays(-1) }
        };
        
        // Act
        await using var writer = new PostgreSqlDataWriter(connectionString, options);
        await writer.InitializeAsync(columns);
        await writer.WriteBatchAsync(data);
        await writer.CompleteAsync();
        
        // Assert
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand($"SELECT COUNT(*) FROM \"{targetTable}\"", connection);
        var count = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        
        Assert.Equal(2, count);
    }
    [Fact]
    public async Task PostgreSqlDataWriter_MixedOrder_MapsCorrectly()
    {
        if (!DockerHelper.IsAvailable() || _postgres is null) return;

        // Arrange
        var connectionString = _postgres.GetConnectionString();
        var tableName = "MixedOrderTest";

        // 1. Manually create table with mixed order: Score (NUMERIC), Name (TEXT), Id (INT)
        // Use quoted identifiers to match exact casing which PostgreSqlDataWriter uses.
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"CREATE TABLE \"{tableName}\" (\"Score\" NUMERIC, \"Name\" TEXT, \"Id\" INT)";
            await cmd.ExecuteNonQueryAsync();
        }

        // 2. Setup Source Data
        var columns = new List<QueryDump.Core.Models.ColumnInfo>
        {
            new("Id", typeof(int), false),
            new("Name", typeof(string), true),
            new("Score", typeof(decimal), false)
        };

        var row1 = new object?[] { 1, "Alice", 95.5m };
        var row2 = new object?[] { 2, "Bob", 80.0m };
        var batch = new List<object?[]> { row1, row2 };

        var writerOptions = new PostgreSqlWriterOptions
        {
            Table = tableName, // Writer will quote this -> "MixedOrderTest"
            Strategy = PostgreSqlWriteStrategy.Append
        };

        // Act
        await using var writer = new PostgreSqlDataWriter(connectionString, writerOptions);
        await writer.InitializeAsync(columns); // Will find table and append
        await writer.WriteBatchAsync(batch);
        await writer.CompleteAsync();

        // Assert
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync();
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT \"Id\", \"Name\", \"Score\" FROM \"{tableName}\" ORDER BY \"Id\"";
            await using var reader = await cmd.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.Equal(1, reader.GetInt32(0)); // Id
            Assert.Equal("Alice", reader.GetString(1)); // Name
            Assert.Equal(95.5m, reader.GetDecimal(2)); // Score
            
            Assert.True(await reader.ReadAsync());
            Assert.Equal(2, reader.GetInt32(0));
            Assert.Equal("Bob", reader.GetString(1));
            Assert.Equal(80.0m, reader.GetDecimal(2));
        }
    }
}
