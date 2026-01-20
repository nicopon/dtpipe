using Testcontainers.PostgreSql;
using Xunit;
using Npgsql;
using QueryDump.Providers.PostgreSQL;
using QueryDump.Tests.Helpers;

namespace QueryDump.Tests;

[Trait("Category", "Integration")]
public class PostgreSqlIntegrationTests : IAsyncLifetime
{
    private PostgreSqlContainer? _postgres;

    public async ValueTask InitializeAsync()
    {
        if (!DockerHelper.IsAvailable()) return;

        try
        {
            _postgres = new PostgreSqlBuilder()
                .WithImage("postgres:15-alpine")
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
        var options = new PostgreSqlOptions 
        { 
            Table = targetTable, 
            Strategy = PostgreSqlWriteStrategy.Recreate 
        };
        
        // Prepare Data
        var columns = new List<QueryDump.Core.ColumnInfo>
        {
            new("Id", typeof(int), false, true),
            new("Name", typeof(string), true, false),
            new("Created", typeof(DateTime), true, false)
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
}
